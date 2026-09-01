<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		api,
		apiLabel,
		defaultStrategyYml,
		formatTs,
		shortId,
		venuesFromMdFeeds,
		type ApiCredential,
		type StrategyRow,
		type StrategyTemplate
	} from '$lib/api';
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

	const OPERATOR_STOP = 'operator_stop';
	const PAGE_SIZE = 50;

	let tab = $state<Tab>('live');
	let strategies = $state<StrategyRow[]>([]);
	let page = $state(1);
	let total = $state(0);
	let yamlText = $state(defaultStrategyYml());
	let templates = $state<StrategyTemplate[]>([]);
	let selectedType = $state('');
	let pristineYaml = $state(defaultStrategyYml());
	let accounts = $state<ApiCredential[]>([]);
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);
	let connection = $state<StatusConnection>('connecting');
	let lastEventTs = new Map<string, number>();

	let listEpoch = 0;
	let listTail: Promise<void> = Promise.resolve();
	let pendingReload = false;
	let pendingSessions = new Set<string>();

	const pageCount = $derived(pageCountOf(total, PAGE_SIZE));

	const lineCount = $derived(Math.max(12, yamlText.split('\n').length + 2));
	const selected = $derived(templates.find((t) => t.type === selectedType) ?? null);
	const dirty = $derived(yamlText !== pristineYaml);

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

	function pickType(rows: StrategyRow[], available: StrategyTemplate[], fallback: string): string {
		const known = new Set(available.map((t) => t.type));
		for (const row of rows) {
			if (row.type && known.has(row.type)) return row.type;
		}
		if (fallback && known.has(fallback)) return fallback;
		return available[0]?.type ?? '';
	}

	function accountLabel(apiId: number): string {
		const row = accounts.find((a) => a.id === apiId);
		return row
			? apiLabel({ api_id: row.id, venue: row.venue, name: row.name })
			: String(apiId);
	}

	function tdIds(s: StrategyRow): number[] {
		return s.td_api_ids ?? [];
	}

	function mdFeeds(s: StrategyRow): string[] {
		return s.md_ids ?? [];
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
			const accountsP = withTypes ? api.apis().catch(() => ({ apis: [] })) : null;
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
			if (accountsP) {
				const a = await accountsP;
				if (epoch !== listEpoch) return;
				accounts = a.apis;
			}
			if (typesP) {
				const t = await typesP;
				if (epoch !== listEpoch) return;
				templates = t.templates;
				if (!templates.some((x) => x.type === selectedType)) {
					selectedType = pickType(list.strategies, templates, t.default);
				}
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
		strategies = [];
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
	}

	function applyTemplate(type: string) {
		const tpl = templates.find((t) => t.type === type);
		const next = tpl?.yaml || defaultStrategyYml();
		yamlText = next;
		pristineYaml = next;
	}

	function changeType(next: string) {
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
			tab = 'live';
			page = 1;
			total = 0;
			strategies = [];
			await refresh();
			await goto(`/strategy/${created.session_id}`);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function resetTemplate() {
		applyTemplate(selectedType);
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

	function applyStatus(ev: StsSessionStatusEvent) {
		const previous = lastEventTs.get(ev.session_id);
		if (previous !== undefined && ev.ts < previous) return;
		lastEventTs.set(ev.session_id, ev.ts);

		const inTab = statusesOf(tab).has(ev.status);
		const row = strategies.find((s) => s.session_id === ev.session_id);
		if (row === undefined) {
			if (inTab && tab !== 'history' && page === 1) fetchUnknownSession(ev.session_id);
			return;
		}
		if (!inTab) {
			dropRow(ev.session_id);
			return;
		}
		strategies = strategies.map((s) =>
			s.session_id === ev.session_id ? { ...s, status: ev.status, reason: ev.reason } : s
		);
	}

	onMount(() => {
		refresh();
		return connectStsStatus(applyStatus, (state) => (connection = state));
	});
</script>

<div class="page-head">
	<div>
		<h1>Strategy</h1>
		<p>
			One deploy, and the TD accounts and MD venues it attached. Session logs live
			on the run; account and venue logs stay on their own streams.
		</p>
	</div>
	<div class="head-actions">
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

<section class="panel table-wrap">
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
							: 'No strategy history yet.'}
		</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Type</th>
					<th>Session</th>
					<th>TD</th>
					<th>MD</th>
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
							<a href={`/strategy/${s.session_id}`} title={s.session_id}>
								{shortId(s.session_id)}
							</a>
						</td>
						<td>
							{#if tdIds(s).length === 0}
								<span class="muted">—</span>
							{:else}
								<div class="chips">
									{#each tdIds(s) as id (id)}
										<a href={`/td/${id}`}>{accountLabel(id)}</a>
									{/each}
								</div>
							{/if}
						</td>
						<td>
							{#if mdFeeds(s).length === 0}
								<span class="muted">—</span>
							{:else}
								<div class="chips">
									{#each venuesFromMdFeeds(mdFeeds(s)) as venue (venue)}
										<a href={`/md/${venue}`}>{venue}</a>
									{/each}
								</div>
							{/if}
						</td>
						<td>
							{#if s.status === 'failed' || s.status === 'interrupted'}
								<span class="badge {s.status}">{s.status}</span>
							{:else if s.status === 'ack'}
								<span class="badge ack">ack</span>
							{:else if s.status === 'done' && s.reason === OPERATOR_STOP}
								<span class="badge stopped">stopped</span>
							{:else if s.status === 'done'}
								<span class="badge done">done</span>
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
								<a class="link-btn" href={`/strategy/${s.session_id}`}>Open</a>
							</div>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
	<Pager {page} {pageCount} disabled={loading} onchange={setPage} />
</section>

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

	.head-actions {
		display: flex;
		align-items: center;
		gap: 0.6rem;
	}

	.live-state {
		font-size: 0.78rem;
		color: var(--warn);
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 0.35rem 0.6rem;
		font-size: 0.85rem;
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
