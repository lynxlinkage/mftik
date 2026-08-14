<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatTs, shortId, type Session } from '$lib/api';
	import LogDownloadModal from '$lib/components/LogDownloadModal.svelte';

	let tab = $state<'live' | 'history'>('live');
	let sessions = $state<Session[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let downloadVenue = $state<string | null>(null);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const res = await api.mdSessions(tab === 'live' ? 'live' : 'done');
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
		return `${s.venue ?? '—'}:${s.session_id}`;
	}

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>MD</h1>
		<p>
			Market-data attaches by venue. Logs are venue-scoped (<code>/ws/md/&#123;venue&#125;</code>).
		</p>
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
			{loading ? 'Loading…' : tab === 'live' ? 'No live MD sessions.' : 'No MD history yet.'}
		</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Venue</th>
					<th>STS</th>
					<th>Status</th>
					<th>Created</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each sessions as s (rowKey(s))}
					<tr>
						<td>
							{#if s.venue}
								<a href={`/md/${s.venue}`}>{s.venue}</a>
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
							{#if s.venue}
								<div class="row-actions">
									<a class="link-btn" href={`/md/${s.venue}`}>Logs</a>
									<button
										type="button"
										class="secondary"
										onclick={() => (downloadVenue = s.venue)}
									>
										Download
									</button>
								</div>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

{#if downloadVenue}
	<LogDownloadModal
		domain="md"
		streamId={downloadVenue}
		open={true}
		onclose={() => (downloadVenue = null)}
	/>
{/if}

<style>
	.row-actions {
		display: flex;
		align-items: center;
		gap: 0.4rem;
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
