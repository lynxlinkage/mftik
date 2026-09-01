<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		api,
		defaultStrategyYml,
		formatTs,
		shortId,
		type StrategyRow,
		type StrategyTemplate,
		type StrategyYaml
	} from '$lib/api';
	import LogDownloadModal from '$lib/components/LogDownloadModal.svelte';
	import Pager from '$lib/components/Pager.svelte';
	import StrategyPicker from '$lib/components/StrategyPicker.svelte';
	import {
		connectStsStatus,
		type StatusConnection,
		type StsSessionStatusEvent
	} from '$lib/logging/status';
	import { pageCountOf } from '$lib/paging';

	type Tab = 'live' | 'attention' | 'history';

	const TAB_STATUS: Record<Tab, string> = {
		live: 'live',
		attention: 'failed,interrupted',
		history: 'done,ack'
	};

	const PAGE_SIZE = 50;

	let tab = $state<Tab>('live');
	let strategies = $state<StrategyRow[]>([]);
	let page = $state(1);
	let total = $state(0);
	let yamlText = $state(defaultStrategyYml());
	let templates = $state<StrategyTemplate[]>([]);
	let selectedType = $state('');
	// Tracks whether the editor still holds the selected type's template
	// untouched, so switching type can only discard what nobody typed.
	let pristineYaml = $state(defaultStrategyYml());
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);

	// The strategy.yml of a past deploy: the submitted document, or a rebuild
	// from the stored spec for deploys made before the text was kept.
	let viewing = $state<StrategyYaml | null>(null);
	let viewingId = $state<string | null>(null);
	let copied = $state(false);

	// Matches mftik.protocol.STS_REASON_OPERATOR_STOP. A stopped session is
	// `done` like any other, so this string is the only thing telling the two
	// apart — keep it in step with the backend constant.
	const OPERATOR_STOP = 'operator_stop';

	let connection = $state<StatusConnection>('connecting');
	// Envelope ts of the newest event applied per session, so a replayed event
	// cannot overwrite a newer one we already have.
	let lastEventTs = new Map<string, number>();
	let downloadId = $state<string | null>(null);

	// One queue writes the table. refresh / page / the socket all go
	// through it so a tab switch cannot leave live rows under History, and
	// a rebuilt session that lands after a fetch has left is not discarded.
	let listEpoch = 0;
	let listTail: Promise<void> = Promise.resolve();
	let pendingReload = false;
	let pendingSessions = new Set<string>();

	const pageCount = $derived(pageCountOf(total, PAGE_SIZE));

	function statusesOf(which: Tab): Set<string> {
		return new Set(TAB_STATUS[which].split(','));
	}

	function bumpListEpoch() {
		listEpoch += 1;
	}

	function enqueueList(job: () => Promise<void>): Promise<void> {
		const run = listTail.then(job, job);
		listTail = run.then(
			() => undefined,
			() => undefined
		);
		return run;
	}

	const lineCount = $derived(Math.max(12, yamlText.split('\n').length + 2));
	const selected = $derived(
		templates.find((t) => t.type === selectedType) ?? null
	);
	const dirty = $derived(yamlText !== pristineYaml);

	/** Newest deploy whose type is still on the picker; else the catalogue default. */
	function pickType(
		rows: StrategyRow[],
		available: StrategyTemplate[],
		fallback: string
	): string {
		const known = new Set(available.map((t) => t.type));
		for (const row of rows) {
			if (row.type && known.has(row.type)) return row.type;
		}
		if (fallback && known.has(fallback)) return fallback;
		return available[0]?.type ?? '';
	}

	async function applyPage(epoch: number, withTypes: boolean) {
		const myTab = tab;
		let myPage = page;
		loading = true;
		error = null;
		try {
			const typesP = withTypes
				? api.strategyTypes().catch(() => ({
						types: [],
						templates: [],
						default: 'NoopStrategy'
					}))
				: null;
			let offset = Math.max(0, (myPage - 1) * PAGE_SIZE);
			let list = await api.strategies({
				status: TAB_STATUS[myTab],
				limit: PAGE_SIZE,
				offset
			});
			if (epoch !== listEpoch || tab !== myTab) return;
			if (offset > 0 && offset >= list.total) {
				myPage = pageCountOf(list.total, PAGE_SIZE);
				offset = (myPage - 1) * PAGE_SIZE;
				page = myPage;
				list = await api.strategies({
					status: TAB_STATUS[myTab],
					limit: PAGE_SIZE,
					offset
				});
				if (epoch !== listEpoch || tab !== myTab) return;
			}
			strategies = list.strategies;
			total = list.total ?? 0;
			if (typesP) {
				const t = await typesP;
				if (epoch !== listEpoch) return;
				templates = t.templates;
				if (!templates.some((x) => x.type === selectedType)) {
					selectedType = pickType(list.strategies, templates, t.default);
				}
				// Only seed the editor while it is untouched — a refresh must not
				// throw away a document someone is in the middle of writing.
				if (!dirty || !yamlText.trim()) applyTemplate(selectedType);
			}
		} catch (e) {
			if (epoch !== listEpoch) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (epoch === listEpoch) loading = false;
		}
	}

	async function refresh() {
		bumpListEpoch();
		const epoch = listEpoch;
		return enqueueList(() => applyPage(epoch, true));
	}

	function setTab(next: Tab) {
		if (next === tab) return;
		tab = next;
		page = 1;
		total = 0;
		bumpListEpoch();
		// Drop the outgoing tab's rows before the fetch. A failed History
		// load must not leave live rows — and their Stop buttons — under
		// the History heading.
		strategies = [];
		viewing = null;
		viewingId = null;
		error = null;
		loading = true;
		const epoch = listEpoch;
		void enqueueList(() => applyPage(epoch, true));
	}

	function setPage(next: number) {
		if (next === page || next < 1) return;
		page = next;
		bumpListEpoch();
		strategies = [];
		error = null;
		loading = true;
		const epoch = listEpoch;
		void enqueueList(() => applyPage(epoch, false));
	}

	function dropRow(sessionId: string) {
		strategies = strategies.filter((s) => s.session_id !== sessionId);
		if (viewingId === sessionId) {
			viewing = null;
			viewingId = null;
		}
	}

	/** Load a type's template into the editor, replacing what is there. */
	function applyTemplate(type: string) {
		const tpl = templates.find((t) => t.type === type);
		const next = tpl?.yaml || defaultStrategyYml();
		yamlText = next;
		pristineYaml = next;
	}

	function changeType(next: string) {
		// The config schema belongs to the type, so a document written for the
		// old one cannot be carried over — swap rather than try to merge.
		if (dirty && !confirm(`Replace the editor with the ${next} template?`)) {
			return;
		}
		selectedType = next;
		applyTemplate(next);
	}

	async function deploy() {
		busy = true;
		error = null;
		try {
			const created = await api.deploySts({ type: selectedType, yaml: yamlText });
			// The new row is live. Someone who deployed from History would
			// otherwise refresh a tab that cannot show it.
			tab = 'live';
			page = 1;
			total = 0;
			strategies = [];
			await refresh();
			await goto(`/sts/${created.session_id}`);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function resetTemplate() {
		applyTemplate(selectedType);
	}

	async function showYaml(s: StrategyRow) {
		// Second click on the same row closes the panel.
		if (viewingId === s.session_id) {
			viewing = null;
			viewingId = null;
			return;
		}
		viewingId = s.session_id;
		viewing = null;
		error = null;
		try {
			viewing = await api.strategyYaml(s.session_id);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
			viewingId = null;
		}
	}

	async function copyYaml() {
		if (viewing === null) return;
		try {
			await navigator.clipboard.writeText(viewing.yaml);
			copied = true;
			setTimeout(() => (copied = false), 1500);
		} catch {
			error = 'Clipboard unavailable — select the text and copy manually.';
		}
	}

	function loadIntoEditor() {
		if (viewing === null) return;
		// Restore the type too, or Deploy would re-run it as something else.
		if (viewing.type) selectedType = viewing.type;
		yamlText = viewing.yaml;
		pristineYaml = viewing.yaml;
		viewing = null;
		viewingId = null;
		window.scrollTo({ top: 0, behavior: 'smooth' });
	}

	async function stop(s: StrategyRow) {
		busy = true;
		error = null;
		try {
			await api.stopSts(s.session_id);
			dropRow(s.session_id);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function ack(s: StrategyRow) {
		busy = true;
		error = null;
		try {
			await api.ackSts(s.session_id);
			dropRow(s.session_id);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	/**
	 * Fetch the list because `sessionId` is not in it yet.
	 *
	 * Concurrent unknowns share one page-one fetch. An id that arrives
	 * after that request has gone out stays in `pendingSessions` and sets
	 * `pendingReload`, so the drain runs again — joining a fetch that
	 * already left is how the second of two rebuilt sessions vanished.
	 */
	async function drainUnknownReloads() {
		while (pendingReload) {
			pendingReload = false;
			bumpListEpoch();
			const epoch = listEpoch;
			const wanted = [...pendingSessions];
			await applyPage(epoch, false);
			for (const id of wanted) pendingSessions.delete(id);
		}
	}

	function fetchUnknownSession(sessionId: string) {
		pendingSessions.add(sessionId);
		pendingReload = true;
		return enqueueList(drainUnknownReloads);
	}

	// One shared tooltip node, positioned in viewport coordinates. It lives
	// outside the table on purpose: `.table-wrap` scrolls horizontally, which
	// makes it a clipping container, and anything absolutely positioned inside
	// it gets cut off at its edges.
	let tip = $state<{ text: string; x: number; y: number; below: boolean } | null>(
		null
	);
	let tipEl = $state<HTMLDivElement | null>(null);

	function showTip(event: MouseEvent | FocusEvent, reason: string | null) {
		const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
		// Flip below the icon when there is no room above it.
		const below = rect.top < 90;
		tip = {
			text: reason ?? 'no reason recorded',
			x: rect.left + rect.width / 2,
			y: below ? rect.bottom + 8 : rect.top - 8,
			below
		};
	}

	function hideTip() {
		tip = null;
	}

	// Nudge back into view when the icon sits near a viewport edge. Done after
	// render because it needs the tooltip's real width, which depends on how
	// long the reason is.
	$effect(() => {
		if (tip === null || tipEl === null) return;
		const rect = tipEl.getBoundingClientRect();
		const margin = 8;
		const overflowLeft = margin - rect.left;
		const overflowRight = rect.right - (window.innerWidth - margin);
		if (overflowLeft > 0) tipEl.style.marginLeft = `${overflowLeft}px`;
		else if (overflowRight > 0) tipEl.style.marginLeft = `${-overflowRight}px`;
	});

	/** Apply one live status event to the table. */
	function applyStatus(ev: StsSessionStatusEvent) {
		const previous = lastEventTs.get(ev.session_id);
		if (previous !== undefined && ev.ts < previous) return;
		lastEventTs.set(ev.session_id, ev.ts);

		const inTab = statusesOf(tab).has(ev.status);
		const row = strategies.find((s) => s.session_id === ev.session_id);
		if (row === undefined) {
			// History does not insert from the socket — a new done row
			// belongs on page one, which this view is not showing.
			if (inTab && tab !== 'history' && page === 1) fetchUnknownSession(ev.session_id);
			return;
		}
		if (!inTab) {
			dropRow(ev.session_id);
			return;
		}
		strategies = strategies.map((s) =>
			s.session_id === ev.session_id
				? { ...s, status: ev.status, reason: ev.reason }
				: s
		);
	}

	onMount(() => {
		refresh();
		return connectStsStatus(applyStatus, (state) => (connection = state));
	});
</script>

<div class="page-head">
	<div>
		<h1>STS</h1>
		<p>
			Pick a strategy, then edit its <code>strategy.yml</code> to deploy TD + MD infra.
			<code>td</code> uses account names (not api ids); <code>sts</code> holds that
			strategy's own parameters.
		</p>
	</div>
	<div class="head-actions">
		<!-- A silently dead socket is the failure this whole stream exists to
		     avoid, so say when the table has stopped updating itself. -->
		{#if connection !== 'open'}
			<span class="live-state" title="Session states are not updating live">
				{connection === 'connecting' ? 'connecting…' : 'not live'}
			</span>
		{/if}
		<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
	</div>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<section class="panel editor">
	<div class="editor-head">
		<div>
			<strong>strategy.yml</strong>
			<span class="muted">Live editor — td / md / sts</span>
		</div>
		<label class="type-pick">
			Strategy
			<StrategyPicker
				{templates}
				value={selectedType}
				disabled={busy || templates.length === 0}
				onchange={changeType}
			/>
		</label>
		<div class="editor-actions">
			<button type="button" class="secondary" onclick={resetTemplate} disabled={busy}>
				Reset template
			</button>
			<button type="button" onclick={deploy} disabled={busy || !yamlText.trim() || !selectedType}>
				Deploy
			</button>
		</div>
	</div>
	{#if selected}
		<p class="type-note">
			<code>{selected.type}</code> · {selected.description}
			{#if dirty}<span class="edited">edited</span>{/if}
		</p>
		{#if selected.env_ok === false && selected.requires?.length}
			<p class="type-note env-gap">
				Needs {selected.requires.join(', ')} — this node does not have them yet. Deploy
				will refuse.
			</p>
		{/if}
	{/if}
	<textarea
		class="yml"
		bind:value={yamlText}
		rows={lineCount}
		spellcheck="false"
		disabled={busy}
		aria-label="strategy.yml editor"
	></textarea>
</section>

<div class="tabs" role="tablist">
	<button type="button" class:active={tab === 'live'} onclick={() => setTab('live')}>Live</button>
	<button type="button" class:active={tab === 'attention'} onclick={() => setTab('attention')}>
		Attention
	</button>
	<button type="button" class:active={tab === 'history'} onclick={() => setTab('history')}>
		History
	</button>
</div>

<!-- The reason is detail, not headline: a row is scanned for its status, and
     256 characters of it inline would bury that. No `title` attribute — the
     tooltip replaces it, and having both would show two of them. -->
{#snippet why(reason: string | null)}
	<button
		type="button"
		class="why"
		aria-label={`Why it ended: ${reason ?? 'no reason recorded'}`}
		onmouseenter={(e) => showTip(e, reason)}
		onmouseleave={hideTip}
		onfocus={(e) => showTip(e, reason)}
		onblur={hideTip}
	>i</button>
{/snippet}

<section class="panel table-wrap" onscroll={hideTip}>
	{#if strategies.length === 0}
		<p class="empty-state">
			{loading
				? 'Loading…'
				: pageCount > 1
					? 'Nothing on this page.'
					: tab === 'live'
						? 'No live sessions.'
						: tab === 'attention'
							? 'Nothing needs attention.'
							: 'No STS history yet.'}
		</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Type</th>
					<th>Session</th>
					<th>Status</th>
					<th>Created</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each strategies as s (s.session_id)}
					<tr>
						<td><code>{s.type ?? '—'}</code></td>
						<td>
							<a href={`/sts/${s.session_id}`} title={s.session_id}>
								{shortId(s.session_id)}
							</a>
						</td>
						<td>
							<!-- The terminal statuses come first: they are final.
							     `status` is the only thing consulted. A null `type`
							     used to be read as failed here, to cover rollbacks that an
							     older path recorded as `done` — those rows are fixed and
							     that path now sends `sts.session.fail`, so inferring from
							     `type` only served to outrank the real status and left an
							     acked session showing failed forever. -->
							{#if s.status === 'failed' || s.status === 'interrupted'}
								<div class="status-cell">
									<span class="badge {s.status}">{s.status}</span>
									{@render why(s.reason)}
								</div>
							{:else if s.status === 'ack'}
								<div class="status-cell">
									<span class="badge ack">ack</span>
									{#if s.reason}{@render why(s.reason)}{/if}
								</div>
							{:else if s.status === 'done' && s.reason === OPERATOR_STOP}
								<!-- Someone pulled this one. Same status as a strategy that
								     finished, but a different thing to have happened, and the
								     badge is where that should be readable. -->
								<span class="badge stopped">stopped</span>
							{:else if s.status === 'done'}
								<div class="status-cell">
									<span class="badge done">done</span>
									<!-- Sessions that ended before reasons were recorded have
									     none; there is nothing to offer for those. -->
									{#if s.reason}{@render why(s.reason)}{/if}
								</div>
							{:else if s.status === 'live'}
								<span class="badge live">running</span>
							{:else}
								<span class="badge">{s.status ?? '—'}</span>
							{/if}
						</td>
						<td class="muted">{formatTs(s.created_at)}</td>
						<td>
							<div class="actions">
								{#if s.status === 'live'}
									<button type="button" class="danger" disabled={busy} onclick={() => stop(s)}>
										Stop
									</button>
								{/if}
								{#if s.status === 'failed' || s.status === 'interrupted'}
									<button type="button" class="secondary" disabled={busy} onclick={() => ack(s)}>
										Ack
									</button>
								{/if}
								<button
									type="button"
									class="secondary"
									class:active={viewingId === s.session_id}
									onclick={() => showYaml(s)}
								>
									YAML
								</button>
								<a class="link-btn" href={`/sts/${s.session_id}`}>Logs</a>
								<button
									type="button"
									class="secondary"
									onclick={() => (downloadId = s.session_id)}
								>
									Download
								</button>
							</div>
						</td>
					</tr>
					{#if viewingId === s.session_id}
						<tr class="yaml-row">
							<td colspan="5">
								{#if viewing === null}
									<p class="muted small">Loading…</p>
								{:else}
									<div class="yaml-head">
										<span class="muted small">The document as submitted.</span>
										<div class="actions">
											<button type="button" class="secondary" onclick={copyYaml}>
												{copied ? 'Copied' : 'Copy'}
											</button>
											<button type="button" class="secondary" onclick={loadIntoEditor}>
												Load into editor
											</button>
										</div>
									</div>
									<pre class="yml-view">{viewing.yaml}</pre>
								{/if}
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	{/if}
	<Pager {page} {pageCount} disabled={loading} onchange={setPage} />
</section>

<!-- Rendered outside the scrolling table, in viewport coordinates, so the
     container that clips its own overflow cannot clip this too. Hidden from
     assistive tech: the trigger's aria-label already carries the reason, and
     announcing it twice helps nobody. -->
<svelte:window onscroll={hideTip} onresize={hideTip} />
{#if tip}
	<div
		class="tip"
		class:below={tip.below}
		style={`left: ${tip.x}px; top: ${tip.y}px;`}
		bind:this={tipEl}
		aria-hidden="true"
	>
		{tip.text}
	</div>
{/if}

{#if downloadId}
	<LogDownloadModal
		domain="sts"
		streamId={downloadId}
		open={true}
		onclose={() => (downloadId = null)}
	/>
{/if}

<style>
	.editor {
		display: grid;
		gap: 0.75rem;
		margin-bottom: 1rem;
	}

	.editor-head {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
	}

	.editor-head strong {
		display: block;
		font-size: 0.95rem;
	}

	.editor-head .muted {
		font-size: 0.8rem;
	}

	.type-pick {
		display: grid;
		gap: 0.3rem;
		font-size: 0.75rem;
		color: var(--muted);
	}

	.type-note {
		margin: 0;
		font-size: 0.78rem;
		color: var(--muted);
	}

	.env-gap {
		color: var(--warn);
	}

	.edited {
		margin-left: 0.4rem;
		color: var(--warn);
	}

	.editor-actions {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem;
	}

	.yml {
		width: 100%;
		min-height: 16rem;
		resize: vertical;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
		font-size: 0.85rem;
		line-height: 1.45;
		tab-size: 2;
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		border-radius: var(--radius);
		padding: 0.85rem 1rem;
	}

	.yml:focus {
		outline: 2px solid color-mix(in srgb, var(--accent) 55%, transparent);
		outline-offset: 1px;
	}

	.table-wrap {
		overflow-x: auto;
	}

	.actions button.active {
		border-color: var(--accent);
		color: var(--text);
	}

	.yaml-row td {
		background: var(--bg);
	}

	.yaml-head {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
		margin-bottom: 0.6rem;
	}

	.small {
		font-size: 0.78rem;
	}

	.warn {
		margin: 0 0 0.6rem;
		color: var(--warn);
	}

	.yml-view {
		margin: 0;
		padding: 0.85rem 1rem;
		background: var(--bg-elevated);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
		font-size: 0.82rem;
		line-height: 1.45;
		overflow-x: auto;
		white-space: pre;
	}

	.badge.done,
	.badge.ack {
		opacity: 0.75;
	}

	.head-actions {
		display: flex;
		align-items: center;
		gap: 0.6rem;
	}

	.live-state {
		font-size: 0.78rem;
		color: var(--warn);
	}

	.status-cell {
		display: flex;
		align-items: center;
		gap: 0.45rem;
		min-width: 0;
	}

	/* A button so it is focusable and announced; it has no click behaviour of
	   its own — hovering it is the whole interaction. */
	.why {
		padding: 0;
		background: none;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 1.05rem;
		height: 1.05rem;
		border: 1px solid var(--border);
		border-radius: 50%;
		color: var(--muted);
		font-size: 0.68rem;
		font-style: italic;
		line-height: 1;
		cursor: help;
	}

	.why:hover,
	.why:focus-visible {
		color: var(--err);
		border-color: rgba(240, 113, 120, 0.5);
		outline: none;
	}

	.tip {
		position: fixed;
		z-index: 50;
		/* Sits above the icon; `below` flips it under when there is no room. */
		transform: translate(-50%, -100%);
		max-width: min(30rem, calc(100vw - 2rem));
		padding: 0.45rem 0.6rem;
		background: var(--bg-elevated);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		font-size: 0.78rem;
		line-height: 1.45;
		/* Reasons can be one long unbroken token — break rather than overflow. */
		overflow-wrap: anywhere;
		box-shadow: 0 6px 20px rgba(0, 0, 0, 0.45);
		/* Never let the tooltip take the pointer: hovering it must not count as
		   leaving the icon, or it would flicker. */
		pointer-events: none;
	}

	.tip.below {
		transform: translate(-50%, 0);
	}

	.link-btn {
		display: inline-flex;
		align-items: center;
		padding: 0.5rem 0.75rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		text-decoration: none;
		font-weight: 600;
		font-size: 0.9rem;
	}

	.link-btn:hover {
		border-color: var(--accent);
		text-decoration: none;
	}
</style>
