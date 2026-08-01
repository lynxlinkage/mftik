<script lang="ts">
	import { onMount } from 'svelte';
	import { api, apiLabel, formatTs, shortId, type Session } from '$lib/api';

	let tab = $state<'live' | 'history'>('live');
	let sessions = $state<Session[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const res = await api.tdSessions(tab === 'live' ? 'live' : 'done');
			sessions = res.sessions;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	function setTab(next: 'live' | 'history') {
		tab = next;
		void refresh();
	}

	function rowKey(s: Session): string {
		return `${s.api_id ?? 'x'}:${s.session_id}`;
	}

	function label(s: Session): string {
		return apiLabel({
			api_id: s.api_id,
			venue: s.venue,
			api_name: s.api_name
		});
	}

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>TD</h1>
		<p>Trading accounts (venue/name). Paths stay keyed by api_id; labels resolve from accounts.</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<div class="tabs" role="tablist">
	<button type="button" class:active={tab === 'live'} onclick={() => setTab('live')}>Live</button>
	<button type="button" class:active={tab === 'history'} onclick={() => setTab('history')}>
		History
	</button>
</div>

<section class="panel">
	{#if sessions.length === 0}
		<p class="empty-state">
			{loading ? 'Loading…' : tab === 'live' ? 'No live TD sessions.' : 'No TD history yet.'}
		</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Account</th>
					<th>STS session</th>
					<th>Status</th>
					<th>Created</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each sessions as s (rowKey(s))}
					<tr>
						<td>
							{#if s.api_id != null}
								<a href={`/td/${s.api_id}`} title={`api_id=${s.api_id}`}>
									{label(s)}
								</a>
							{:else}
								—
							{/if}
						</td>
						<td class="muted">
							<a href={`/sts/${s.session_id}`} title={s.session_id}>
								{shortId(s.session_id)}
							</a>
						</td>
						<td>
							<span class="badge" class:live={s.status === 'live'} class:done={s.status !== 'live'}>
								{s.status}
							</span>
						</td>
						<td class="muted">{formatTs(s.created_at)}</td>
						<td>
							{#if s.api_id != null}
								<a class="link-btn" href={`/td/${s.api_id}`}>Logs</a>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

<style>
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
