<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import {
		api,
		defaultStrategyYml,
		formatTs,
		shortId,
		type StrategyRow,
		type StrategyYaml
	} from '$lib/api';

	let strategies = $state<StrategyRow[]>([]);
	let yamlText = $state(defaultStrategyYml());
	let types = $state<string[]>(['NoopStrategy']);
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);

	// The strategy.yml of a past deploy, rebuilt on demand from the stored spec.
	let viewing = $state<StrategyYaml | null>(null);
	let viewingId = $state<number | null>(null);
	let copied = $state(false);

	const lineCount = $derived(Math.max(12, yamlText.split('\n').length + 2));

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [list, tpl, t] = await Promise.all([
				api.strategies(),
				api.strategyTemplate().catch(() => ({ yaml: defaultStrategyYml() })),
				api.strategyTypes().catch(() => ({ types: ['NoopStrategy'] }))
			]);
			strategies = list.strategies;
			types = t.types.length ? t.types : ['NoopStrategy'];
			if (!yamlText.trim()) yamlText = tpl.yaml || defaultStrategyYml();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function deploy() {
		busy = true;
		error = null;
		try {
			const created = await api.deploySts({ yaml: yamlText });
			await refresh();
			await goto(`/sts/${created.session_id}`);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function resetTemplate() {
		yamlText = defaultStrategyYml();
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
		yamlText = viewing.yaml;
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

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>STS</h1>
		<p>
			Edit <code>strategy.yml</code> to deploy TD + MD infra with a customized STS
			strategy. <code>td</code> uses account names (not api ids). Types: {types.join(', ')}.
		</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<section class="panel editor">
	<div class="editor-head">
		<div>
			<strong>strategy.yml</strong>
			<span class="muted">Live editor — td / md / sts.type + config</span>
		</div>
		<div class="editor-actions">
			<button type="button" class="secondary" onclick={resetTemplate} disabled={busy}>
				Reset template
			</button>
			<button type="button" onclick={deploy} disabled={busy || !yamlText.trim()}>
				Deploy
			</button>
		</div>
	</div>
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
							{#if s.status === 'done'}
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
