<script lang="ts">
	import { onMount } from 'svelte';
	import {
		Background,
		BackgroundVariant,
		SvelteFlow,
		type Connection,
		type Edge,
		type IsValidConnection
	} from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import AlertNode from '$lib/alerts/AlertNode.svelte';
	import { bindGraphActions } from '$lib/alerts/actions';
	import CreateDialog, {
		type AlertDraft,
		type CreateDraft,
		type MatcherDraft,
		type SourceDraft
	} from '$lib/alerts/CreateDialog.svelte';
	import {
		buildEdges,
		buildNodes,
		FALLBACK_SIZE,
		isAllowedWire,
		loadPositions,
		parseNodeId,
		savePositions,
		type AlertGraphNode,
		type GraphKind
	} from '$lib/alerts/graph';
	import MatcherNode from '$lib/alerts/MatcherNode.svelte';
	import SourceNode from '$lib/alerts/SourceNode.svelte';
	import {
		api,
		formatTs,
		type Alert,
		type AlertDelivery,
		type AlertMatcher,
		type AlertSource,
		type ApiCredential,
		type Venue
	} from '$lib/api';

	const nodeTypes = {
		source: SourceNode,
		matcher: MatcherNode,
		alert: AlertNode
	};

	let sources = $state<AlertSource[]>([]);
	let matchers = $state<AlertMatcher[]>([]);
	let alerts = $state<Alert[]>([]);
	let deliveries = $state<AlertDelivery[]>([]);
	let stsTypes = $state<string[]>([]);
	let liveTypeCounts = $state<Record<string, number>>({});
	let accounts = $state<ApiCredential[]>([]);
	let venues = $state<Venue[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let selectedAlert = $state<number | null>(null);
	let nodes = $state.raw<AlertGraphNode[]>([]);
	let edges = $state.raw<Edge[]>([]);
	let canvasEl = $state<HTMLDivElement | null>(null);
	let canvasWidth = $state(FALLBACK_SIZE.w);
	let canvasHeight = $state(FALLBACK_SIZE.h);
	const canvasSize = $derived({ w: canvasWidth, h: canvasHeight });

	const stsOptions = $derived(
		[...new Set([...stsTypes, ...Object.keys(liveTypeCounts)])].sort()
	);
	const empty = $derived(
		!loading && sources.length === 0 && matchers.length === 0 && alerts.length === 0
	);

	function rememberedPositions() {
		const stored = loadPositions();
		for (const node of nodes) {
			if (!stored.has(node.id)) stored.set(node.id, node.position);
		}
		return stored;
	}

	/**
	 * Only ever called from `onnodedragstop` — where a node's position is
	 * something a person chose. Never after `buildNodes`, which clamps to the
	 * current canvas: saving there would let a narrow window overwrite the
	 * stored layout with its own collapsed version, and widening again would
	 * not bring it back. A node this has never seen keeps its computed spot
	 * until someone drags it.
	 */
	function persistPositions() {
		if (nodes.length === 0) return;
		savePositions(new Map(nodes.map((node) => [node.id, node.position])));
	}

	function syncGraph(nextSources: AlertSource[], nextMatchers: AlertMatcher[], nextAlerts: Alert[]) {
		nodes = buildNodes(
			nextSources,
			nextMatchers,
			nextAlerts,
			rememberedPositions(),
			canvasSize
		);
		edges = buildEdges(nextSources, nextMatchers);
	}

	let refreshGen = 0;

	async function refresh(opts: { silent?: boolean } = {}) {
		const gen = ++refreshGen;
		if (!opts.silent) loading = true;
		error = null;
		try {
			const [src, mat, al, types, live, apis, venueList] = await Promise.all([
				api.alertSources(),
				api.alertMatchers(),
				api.alerts(),
				api.strategyTypes(),
				api.strategies({ status: 'live', limit: 200 }),
				api.apis(),
				api.venues()
			]);
			if (gen !== refreshGen) return;
			sources = src.sources;
			matchers = mat.matchers;
			alerts = al.alerts;
			stsTypes = types.types;
			accounts = apis.apis;
			venues = venueList.venues;
			const counts: Record<string, number> = {};
			for (const row of live.strategies) {
				if (!row.type) continue;
				counts[row.type] = (counts[row.type] ?? 0) + 1;
			}
			liveTypeCounts = counts;
			syncGraph(src.sources, mat.matchers, al.alerts);
			if (selectedAlert != null) {
				const listed = await api.alertDeliveries(selectedAlert);
				if (gen !== refreshGen) return;
				deliveries = listed.deliveries;
			}
		} catch (e) {
			if (gen !== refreshGen) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (gen === refreshGen) loading = false;
		}
	}

	function hideCreate(kind: GraphKind) {
		document.getElementById(`create-${kind}`)?.hidePopover();
	}

	async function createNode(kind: GraphKind, draft: CreateDraft) {
		error = null;
		try {
			if (kind === 'source') {
				const created = await api.createAlertSource(draft as SourceDraft);
				sources = [...sources, created];
			} else if (kind === 'matcher') {
				const created = await api.createAlertMatcher(draft as MatcherDraft);
				matchers = [...matchers, created];
			} else {
				const created = await api.createAlert(draft as AlertDraft);
				if ('webhook_url' in created) {
					error = 'GET must not return webhook_url';
				}
				alerts = [...alerts, created];
			}
			hideCreate(kind);
			syncGraph(sources, matchers, alerts);
			void refresh({ silent: true });
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function fireTest(id: number) {
		error = null;
		try {
			await api.testAlert(id);
			await selectAlert(id);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function selectAlert(id: number) {
		selectedAlert = id;
		const listed = await api.alertDeliveries(id);
		deliveries = listed.deliveries;
	}

	async function remove(kind: GraphKind, id: number) {
		error = null;
		try {
			if (kind === 'source') {
				await api.deleteAlertSource(id);
				sources = sources.filter((row) => row.id !== id);
			} else if (kind === 'matcher') {
				await api.deleteAlertMatcher(id);
				matchers = matchers.filter((row) => row.id !== id);
			} else {
				await api.deleteAlert(id);
				alerts = alerts.filter((row) => row.id !== id);
			}
			if (kind === 'alert' && selectedAlert === id) {
				selectedAlert = null;
				deliveries = [];
			}
			syncGraph(sources, matchers, alerts);
			void refresh({ silent: true });
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	const isValidConnection: IsValidConnection = (connection) => {
		if (!isAllowedWire(connection.source, connection.target)) return false;
		return !edges.some(
			(edge) => edge.source === connection.source && edge.target === connection.target
		);
	};

	async function onconnect(connection: Connection) {
		const from = parseNodeId(connection.source);
		const to = parseNodeId(connection.target);
		if (!from || !to) return;
		error = null;
		try {
			if (from.kind === 'source' && to.kind === 'matcher') {
				await api.wireSourceMatcher(from.id, to.id);
				sources = sources.map((row) =>
					row.id === from.id && !row.matcher_ids.includes(to.id)
						? { ...row, matcher_ids: [...row.matcher_ids, to.id] }
						: row
				);
				matchers = matchers.map((row) =>
					row.id === to.id && !row.source_ids.includes(from.id)
						? { ...row, source_ids: [...row.source_ids, from.id] }
						: row
				);
			} else if (from.kind === 'matcher' && to.kind === 'alert') {
				await api.wireMatcherAlert(from.id, to.id);
				matchers = matchers.map((row) =>
					row.id === from.id && !row.alert_ids.includes(to.id)
						? { ...row, alert_ids: [...row.alert_ids, to.id] }
						: row
				);
				alerts = alerts.map((row) =>
					row.id === to.id && !row.matcher_ids.includes(from.id)
						? { ...row, matcher_ids: [...row.matcher_ids, from.id] }
						: row
				);
			}
			syncGraph(sources, matchers, alerts);
			void refresh({ silent: true });
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
			void refresh({ silent: true });
		}
	}

	async function ondelete({
		nodes: gone,
		edges: goneEdges
	}: {
		nodes: AlertGraphNode[];
		edges: Edge[];
	}) {
		error = null;
		try {
			for (const edge of goneEdges) {
				const from = parseNodeId(edge.source);
				const to = parseNodeId(edge.target);
				if (from?.kind === 'source' && to?.kind === 'matcher') {
					await api.unwireSourceMatcher(from.id, to.id);
				} else if (from?.kind === 'matcher' && to?.kind === 'alert') {
					await api.unwireMatcherAlert(from.id, to.id);
				}
			}
			for (const node of gone) {
				const parsed = parseNodeId(node.id);
				if (!parsed) continue;
				if (parsed.kind === 'source') {
					await api.deleteAlertSource(parsed.id);
					sources = sources.filter((row) => row.id !== parsed.id);
				} else if (parsed.kind === 'matcher') {
					await api.deleteAlertMatcher(parsed.id);
					matchers = matchers.filter((row) => row.id !== parsed.id);
				} else {
					await api.deleteAlert(parsed.id);
					alerts = alerts.filter((row) => row.id !== parsed.id);
				}
			}
			syncGraph(sources, matchers, alerts);
			void refresh({ silent: true });
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
			void refresh({ silent: true });
		}
	}

	bindGraphActions({
		selectAlert: (id) => void selectAlert(id),
		testAlert: (id) => void fireTest(id),
		remove: (kind, id) => void remove(kind, id)
	});

	onMount(() => {
		void refresh();
		const el = canvasEl;
		if (!el) return;
		const ro = new ResizeObserver((entries) => {
			const box = entries[0]?.contentRect;
			if (!box || box.width < 80 || box.height < 80) return;
			const w = Math.round(box.width);
			const h = Math.round(box.height);
			if (Math.abs(w - canvasWidth) < 2 && Math.abs(h - canvasHeight) < 2) return;
			canvasWidth = w;
			canvasHeight = h;
			nodes = buildNodes(sources, matchers, alerts, rememberedPositions(), { w, h });
		});
		ro.observe(el);
		return () => ro.disconnect();
	});
</script>

<div class="page">
	<div class="page-head">
		<div>
			<h1>Alerts</h1>
			<p>Live logs to a Discord webhook. Wire Source → Matcher → Alert.</p>
		</div>
		<button type="button" class="secondary" onclick={() => void refresh()} disabled={loading}
			>Refresh</button
		>
	</div>

	{#if error}
		<div class="error-banner">{error}</div>
	{/if}

	{#snippet createForm(kind: GraphKind)}
		<CreateDialog
			{kind}
			{stsOptions}
			{liveTypeCounts}
			{accounts}
			{venues}
			oncancel={() => hideCreate(kind)}
			oncreate={(draft) => createNode(kind, draft)}
		/>
	{/snippet}

	<div class="lanes">
		<div class="lane">
			<div>
				<h2>Sources</h2>
				<p>STS type, TD account, or MD venue</p>
			</div>
			<button type="button" class="add" aria-label="Add source node" popovertarget="create-source">
				+
			</button>
			<div id="create-source" class="create-pop" popover="auto">
				{@render createForm('source')}
			</div>
		</div>
		<div class="lane">
			<div>
				<h2>Matchers</h2>
				<p>level, regex, or extract</p>
			</div>
			<button type="button" class="add" aria-label="Add matcher node" popovertarget="create-matcher">
				+
			</button>
			<div id="create-matcher" class="create-pop" popover="auto">
				{@render createForm('matcher')}
			</div>
		</div>
		<div class="lane">
			<div>
				<h2>Alerts</h2>
				<p>Discord webhook</p>
			</div>
			<button type="button" class="add" aria-label="Add alert node" popovertarget="create-alert">
				+
			</button>
			<div id="create-alert" class="create-pop" popover="auto">
				{@render createForm('alert')}
			</div>
		</div>
	</div>

	{#if empty}
		<p class="empty-state">No graph yet. Add a Source, a Matcher, and an Alert, then wire them.</p>
	{/if}

	<div class="canvas" bind:this={canvasEl}>
		<SvelteFlow
			bind:nodes
			bind:edges
			{nodeTypes}
			{isValidConnection}
			{onconnect}
			{ondelete}
			onnodedragstop={persistPositions}
			colorMode="dark"
			clickConnect
			connectionRadius={28}
			minZoom={1}
			maxZoom={1}
			zoomOnScroll={false}
			zoomOnPinch={false}
			zoomOnDoubleClick={false}
			panOnScroll={false}
			panOnDrag={false}
			autoPanOnNodeDrag={false}
			preventScrolling
			proOptions={{ hideAttribution: true }}
			defaultEdgeOptions={{ type: 'default', animated: true }}
			nodeExtent={[
				[0, 0],
				[canvasWidth, canvasHeight]
			]}
			onpaneclick={() => {
				selectedAlert = null;
			}}
			onnodeclick={({ node }) => {
				const parsed = parseNodeId(node.id);
				if (parsed?.kind === 'alert') void selectAlert(parsed.id);
			}}
		>
			<Background variant={BackgroundVariant.Dots} gap={22} size={1} />
		</SvelteFlow>
	</div>

	{#if selectedAlert != null}
		<section class="panel deliveries">
			<h3>Deliveries</h3>
			{#if deliveries.length === 0}
				<p class="empty-state compact">No fires yet.</p>
			{:else}
				<ul>
					{#each deliveries as row (row.id)}
						<li class="muted">
							{formatTs(row.ts)} · events={row.event_count} dropped={row.dropped_count}
							{row.http_status ?? '—'}
							{row.error ?? ''}
						</li>
					{/each}
				</ul>
			{/if}
		</section>
	{/if}
</div>

<style>
	.page {
		display: flex;
		flex-direction: column;
		height: calc(100vh - 4rem);
		min-height: 36rem;
		gap: 0.85rem;
	}
	.lanes {
		position: relative;
		z-index: 5;
		overflow: visible;
		pointer-events: auto;
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 0.75rem;
	}
	.lane {
		position: relative;
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
		padding: 0.7rem 0.9rem;
		border: 1px solid var(--border);
		border-radius: 10px;
		background: linear-gradient(180deg, rgba(24, 32, 43, 0.88), rgba(18, 24, 32, 0.8));
	}
	.lane h2 {
		margin: 0;
		font-size: 0.95rem;
		letter-spacing: 0.04em;
	}
	.lane p {
		margin: 0.15rem 0 0;
		color: var(--muted);
		font-size: 0.75rem;
	}
	.add {
		width: 2.1rem;
		height: 2.1rem;
		padding: 0;
		border-radius: 999px;
		font-size: 1.35rem;
		line-height: 1;
		font-weight: 500;
	}
	.create-pop {
		inset: unset;
		top: 50%;
		left: 50%;
		transform: translate(-50%, -50%);
		margin: 0;
		width: min(28rem, calc(100vw - 2rem));
		border: 0;
		padding: 0;
		background: transparent;
		overflow: visible;
	}
	.canvas {
		position: relative;
		flex: 1 1 auto;
		min-height: 22rem;
		isolation: isolate;
		overflow: hidden;
		border: 1px solid var(--border);
		border-radius: 10px;
		background: rgba(8, 12, 18, 0.55);
	}
	.empty-state.compact {
		padding: 0.6rem 0;
	}
	.deliveries {
		padding-top: 0.85rem;
	}
	.deliveries h3 {
		margin: 0 0 0.5rem;
		font-size: 0.9rem;
	}
	.deliveries ul {
		list-style: none;
		margin: 0;
		padding: 0;
	}
	.deliveries li {
		margin-bottom: 0.3rem;
		font-size: 0.82rem;
	}
	:global(.canvas .svelte-flow) {
		position: absolute;
		inset: 0;
		width: 100%;
		height: 100%;
		z-index: 2;
		background: transparent;
	}
	:global(.canvas .svelte-flow__attribution) {
		display: none;
	}
	:global(.canvas .svelte-flow__handle) {
		width: 12px;
		height: 12px;
		border: 2px solid #0c1016;
	}
	:global(.canvas .svelte-flow__handle-right) {
		top: 50%;
		right: 0;
		left: auto;
		transform: translate(50%, -50%);
	}
	:global(.canvas .svelte-flow__handle-left) {
		top: 50%;
		left: 0;
		right: auto;
		transform: translate(-50%, -50%);
	}
	:global(.canvas .svelte-flow__node) {
		overflow: visible;
		background: transparent;
		border: 0;
		padding: 0;
		border-radius: 0;
		box-shadow: none;
	}
	:global(.canvas .svelte-flow__edge-path) {
		stroke: #3d9cf0;
	}
	:global(.canvas .svelte-flow__controls) {
		display: none;
	}
	@media (max-width: 900px) {
		.lanes {
			grid-template-columns: 1fr;
		}
	}
</style>
