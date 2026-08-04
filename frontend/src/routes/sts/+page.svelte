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
	import {
		connectStsStatus,
		type StatusConnection,
		type StsSessionStatusEvent
	} from '$lib/logging/status';

	let strategies = $state<StrategyRow[]>([]);
	let yamlText = $state(defaultStrategyYml());
	let templates = $state<StrategyTemplate[]>([]);
	let selectedType = $state('NoopStrategy');
	// Tracks whether the editor still holds the selected type's template
	// untouched, so switching type can only discard what nobody typed.
	let pristineYaml = $state(defaultStrategyYml());
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);

	// The strategy.yml of a past deploy, rebuilt on demand from the stored spec.
	let viewing = $state<StrategyYaml | null>(null);
	let viewingId = $state<number | null>(null);
	let copied = $state(false);

	let connection = $state<StatusConnection>('connecting');
	// Envelope ts of the newest event applied per session, so a replayed event
	// cannot overwrite a newer one we already have.
	let lastEventTs = new Map<string, number>();
	// Sessions we are already fetching a row for, so a burst of events for the
	// same new session does not fan out into a burst of list fetches.
	let pendingSessions = new Set<string>();

	const lineCount = $derived(Math.max(12, yamlText.split('\n').length + 2));
	const selected = $derived(
		templates.find((t) => t.type === selectedType) ?? null
	);
	const dirty = $derived(yamlText !== pristineYaml);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [list, t] = await Promise.all([
				api.strategies(),
				api.strategyTypes().catch(() => ({
					types: [],
					templates: [],
					default: 'NoopStrategy'
				}))
			]);
			strategies = list.strategies;
			templates = t.templates;
			if (!templates.some((x) => x.type === selectedType)) {
				selectedType = t.default || templates[0]?.type || selectedType;
			}
			// Only seed the editor while it is untouched — a refresh must not
			// throw away a document someone is in the middle of writing.
			if (!dirty || !yamlText.trim()) applyTemplate(selectedType);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
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
		if (viewingId === s.id) {
			viewing = null;
			viewingId = null;
			return;
		}
		viewingId = s.id;
		viewing = null;
		error = null;
		try {
			viewing = await api.strategyYaml(s.id);
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

	async function togglePause(s: StrategyRow) {
		busy = true;
		error = null;
		try {
			if (s.paused) await api.resumeSts(s.sts_session);
			else await api.pauseSts(s.sts_session);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function stop(s: StrategyRow) {
		busy = true;
		error = null;
		try {
			await api.stopSts(s.sts_session);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	/**
	 * Fetch the list because `sessionId` is not in it yet, and try once more if
	 * it still is not.
	 *
	 * Deploy creates the STS session — which announces itself as live straight
	 * away — before the API writes the `strategies` row this table is built
	 * from. So the first fetch legitimately races the row into existence and
	 * can come back without it. One retry closes that window; beyond that the
	 * session simply has no strategy row (nothing deployed it) and asking
	 * again forever would just be a poll.
	 */
	async function fetchUnknownSession(sessionId: string) {
		if (pendingSessions.has(sessionId)) return;
		pendingSessions.add(sessionId);
		try {
			await refresh();
			if (strategies.some((s) => s.sts_session === sessionId)) return;
			await new Promise((r) => setTimeout(r, 1500));
			await refresh();
		} finally {
			pendingSessions.delete(sessionId);
		}
	}

	/** Apply one live status event to the table. */
	function applyStatus(ev: StsSessionStatusEvent) {
		const previous = lastEventTs.get(ev.session_id);
		if (previous !== undefined && ev.ts < previous) return;
		lastEventTs.set(ev.session_id, ev.ts);

		const row = strategies.find((s) => s.sts_session === ev.session_id);
		if (row === undefined) {
			// A session this page has never seen — a deploy from another tab, or
			// this one. The event alone cannot build a row (it carries no strategy
			// id, config or deploy time), so go and fetch one.
			fetchUnknownSession(ev.session_id);
			return;
		}
		strategies = strategies.map((s) =>
			s.sts_session === ev.session_id
				? { ...s, status: ev.status, paused: ev.paused, reason: ev.reason }
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
			<select
				value={selectedType}
				disabled={busy || templates.length === 0}
				onchange={(e) => changeType((e.currentTarget as HTMLSelectElement).value)}
			>
				{#each templates as t (t.type)}
					<option value={t.type}>{t.label}</option>
				{/each}
			</select>
		</label>
		<div class="editor-actions">
			<button type="button" class="secondary" onclick={resetTemplate} disabled={busy}>
				Reset template
			</button>
			<button type="button" onclick={deploy} disabled={busy || !yamlText.trim()}>
				Deploy
			</button>
		</div>
	</div>
	{#if selected}
		<p class="type-note">
			<code>{selected.type}</code> · {selected.description}
			{#if dirty}<span class="edited">edited</span>{/if}
		</p>
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

<section class="panel table-wrap">
	{#if strategies.length === 0}
		<p class="empty-state">{loading ? 'Loading…' : 'No strategies deployed yet.'}</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>ID</th>
					<th>Type</th>
					<th>Session</th>
					<th>Status</th>
					<th>Created</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each strategies as s (s.id)}
					<tr>
						<td>{s.id}</td>
						<td><code>{s.type}</code></td>
						<td>
							<a href={`/sts/${s.sts_session}`} title={s.sts_session}>
								{shortId(s.sts_session)}
							</a>
						</td>
						<td>
							<!-- failed is checked first: it is terminal, and a stale `paused`
							     from the live-session probe must never mask it. -->
							{#if s.status === 'failed'}
								<div class="status-cell">
									<span class="badge failed">failed</span>
									<span class="reason" title={s.reason ?? ''}>
										{s.reason ?? 'no reason recorded'}
									</span>
								</div>
							{:else if s.status === 'done'}
								<span class="badge done">done</span>
							{:else if s.paused}
								<span class="badge paused">paused</span>
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
									<button
										type="button"
										class="secondary"
										disabled={busy}
										onclick={() => togglePause(s)}
									>
										{s.paused ? 'Resume' : 'Pause'}
									</button>
									<button type="button" class="danger" disabled={busy} onclick={() => stop(s)}>
										Stop
									</button>
								{/if}
								<button
									type="button"
									class="secondary"
									class:active={viewingId === s.id}
									onclick={() => showYaml(s)}
								>
									YAML
								</button>
								<a class="link-btn" href={`/sts/${s.sts_session}`}>Logs</a>
							</div>
						</td>
					</tr>
					{#if viewingId === s.id}
						<tr class="yaml-row">
							<td colspan="6">
								{#if viewing === null}
									<p class="muted small">Rebuilding…</p>
								{:else}
									<div class="yaml-head">
										<span class="muted small">
											Rebuilt from the stored spec — the submitted document is not kept, so
											comments and formatting are gone.
										</span>
										<div class="actions">
											<button type="button" class="secondary" onclick={copyYaml}>
												{copied ? 'Copied' : 'Copy'}
											</button>
											<button type="button" class="secondary" onclick={loadIntoEditor}>
												Load into editor
											</button>
										</div>
									</div>
									{#if viewing.unresolved_td.length > 0}
										<p class="warn small">
											api {viewing.unresolved_td.join(', ')} no longer exists — the account name
											could not be recovered, so those <code>td</code> entries are placeholders and
											will not redeploy as-is.
										</p>
									{/if}
									<pre class="yml-view">{viewing.yaml}</pre>
								{/if}
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	{/if}
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

	.type-pick select {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.45rem 0.6rem;
		border-radius: var(--radius);
		min-width: 11rem;
	}

	.type-note {
		margin: 0;
		font-size: 0.78rem;
		color: var(--muted);
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

	.badge.done {
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

	/* The reason can run to 256 chars; clamp it and keep the full text in the
	   title so a long one cannot push the actions column off screen. */
	.reason {
		font-size: 0.78rem;
		color: var(--muted);
		max-width: 22rem;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
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
