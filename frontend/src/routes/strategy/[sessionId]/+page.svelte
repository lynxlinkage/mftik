<script lang="ts">
	import { page } from '$app/state';
	import {
		api,
		apiLabel,
		formatTs,
		venueFromMdFeed,
		venuesFromMdFeeds,
		type ApiCredential,
		type StrategyRow,
		type StrategyYaml
	} from '$lib/api';
	import EventLogDownload from '$lib/components/EventLogDownload.svelte';
	import LogDownloadModal from '$lib/components/LogDownloadModal.svelte';
	import LogViewer from '$lib/components/LogViewer.svelte';

	const sessionId = $derived(page.params.sessionId ?? '');

	const OPERATOR_STOP = 'operator_stop';

	let session = $state<StrategyRow | null>(null);
	let yaml = $state<StrategyYaml | null>(null);
	let accounts = $state<ApiCredential[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);
	let showYaml = $state(false);
	let copied = $state(false);
	let download = $state<{ domain: 'td' | 'md'; streamId: string } | null>(null);

	const tdIds = $derived(session?.td_api_ids ?? []);
	const mdFeeds = $derived(session?.md_ids ?? []);
	const mdVenues = $derived(venuesFromMdFeeds(mdFeeds));

	function accountLabel(apiId: number): string {
		const row = accounts.find((a) => a.id === apiId);
		return row
			? apiLabel({ api_id: row.id, venue: row.venue, name: row.name })
			: String(apiId);
	}

	function feedsForVenue(venue: string): string[] {
		return mdFeeds.filter((feed) => venueFromMdFeed(feed) === venue);
	}

	function statusLabel(row: StrategyRow): string {
		if (row.status === 'done' && row.reason === OPERATOR_STOP) return 'stopped';
		if (row.status === 'live') return 'running';
		return row.status ?? '—';
	}

	async function refresh() {
		const id = sessionId;
		loading = true;
		error = null;
		try {
			const [row, creds] = await Promise.all([
				api.strategySession(id),
				api.apis().catch(() => ({ apis: [] as ApiCredential[] }))
			]);
			if (id !== sessionId) return;
			session = row;
			accounts = creds.apis;
		} catch (e) {
			if (id !== sessionId) return;
			session = null;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (id === sessionId) loading = false;
		}
	}

	async function toggleYaml() {
		if (showYaml) {
			showYaml = false;
			return;
		}
		if (yaml === null) {
			try {
				yaml = await api.strategyYaml(sessionId);
			} catch (e) {
				error = e instanceof Error ? e.message : String(e);
				return;
			}
		}
		showYaml = true;
	}

	async function copyYaml() {
		if (yaml === null) return;
		try {
			await navigator.clipboard.writeText(yaml.yaml);
			copied = true;
			setTimeout(() => (copied = false), 1500);
		} catch {
			error = 'Clipboard unavailable — select the text and copy manually.';
		}
	}

	async function stop() {
		if (session === null) return;
		busy = true;
		error = null;
		try {
			await api.stopSts(session.session_id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function ack() {
		if (session === null) return;
		busy = true;
		error = null;
		try {
			await api.ackSts(session.session_id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	$effect(() => {
		const id = sessionId;
		loading = true;
		error = null;
		let cancelled = false;
		void (async () => {
			try {
				const [row, creds] = await Promise.all([
					api.strategySession(id),
					api.apis().catch(() => ({ apis: [] as ApiCredential[] }))
				]);
				if (cancelled || id !== sessionId) return;
				session = row;
				accounts = creds.apis;
			} catch (e) {
				if (cancelled || id !== sessionId) return;
				session = null;
				error = e instanceof Error ? e.message : String(e);
			} finally {
				if (!cancelled && id === sessionId) loading = false;
			}
		})();
		return () => {
			cancelled = true;
		};
	});
</script>

<p class="back"><a href="/strategy">← Strategy</a></p>

<div class="page-head">
	<div>
		<h1>{session?.type ?? (loading ? 'Strategy' : 'Session')}</h1>
		<p class="mono muted">{sessionId}</p>
	</div>
	<div class="head-actions">
		{#if session}
			<span
				class="badge"
				class:live={session.status === 'live'}
				class:done={session.status === 'done' || session.status === 'ack'}
				class:failed={session.status === 'failed'}
				class:interrupted={session.status === 'interrupted'}
				class:stopped={session.status === 'done' && session.reason === OPERATOR_STOP}
				title={session.reason ?? ''}
			>
				{statusLabel(session)}
			</span>
		{/if}
		<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
	</div>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

{#if session}
	<section class="panel summary">
		<dl>
			<div>
				<dt>Created</dt>
				<dd>{formatTs(session.created_at)}</dd>
			</div>
			<div>
				<dt>Reason</dt>
				<dd>{session.reason ?? '—'}</dd>
			</div>
		</dl>
		<div class="actions">
			{#if session.status === 'live'}
				<button type="button" class="danger" disabled={busy} onclick={stop}>Stop</button>
			{/if}
			{#if session.status === 'failed' || session.status === 'interrupted'}
				<button type="button" class="secondary" disabled={busy} onclick={ack}>Ack</button>
			{/if}
			<button type="button" class="secondary" class:active={showYaml} onclick={toggleYaml}>
				YAML
			</button>
		</div>
	</section>

	{#if showYaml}
		<section class="panel">
			<div class="yaml-head">
				<span class="muted small">
					{#if yaml === null}
						Loading…
					{:else}
						The document as submitted.
					{/if}
				</span>
				{#if yaml}
					<button type="button" class="secondary" onclick={copyYaml}>
						{copied ? 'Copied' : 'Copy'}
					</button>
				{/if}
			</div>
			{#if yaml}
				<pre class="yml-view">{yaml.yaml}</pre>
			{/if}
		</section>
	{/if}

	<section class="panel">
		<h2>Trading accounts</h2>
		<p class="muted note">
			Account streams — not this session's private log. Two deploys on the same
			account share <code>/td/&#123;api_id&#125;</code>.
		</p>
		{#if tdIds.length === 0}
			<p class="empty-state">No TD attach recorded on this deploy.</p>
		{:else}
			<table class="data">
				<thead>
					<tr>
						<th>Account</th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					{#each tdIds as id (id)}
						<tr>
							<td>
								<a href={`/td/${id}`} title={`api_id=${id}`}>{accountLabel(id)}</a>
							</td>
							<td>
								<div class="actions">
									<a class="link-btn" href={`/td/${id}`}>Logs</a>
									<button
										type="button"
										class="secondary"
										onclick={() => (download = { domain: 'td', streamId: String(id) })}
									>
										Download
									</button>
								</div>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	</section>

	<section class="panel">
		<h2>Market data</h2>
		<p class="muted note">
			Venue streams — refcount and fan-out for everyone attached to that venue.
		</p>
		{#if mdVenues.length === 0}
			<p class="empty-state">No MD attach recorded on this deploy.</p>
		{:else}
			<table class="data">
				<thead>
					<tr>
						<th>Venue</th>
						<th>Feeds</th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					{#each mdVenues as venue (venue)}
						<tr>
							<td><a href={`/md/${venue}`}>{venue}</a></td>
							<td>
								<div class="feeds">
									{#each feedsForVenue(venue) as feed (feed)}
										<code>{feed}</code>
									{/each}
								</div>
							</td>
							<td>
								<div class="actions">
									<a class="link-btn" href={`/md/${venue}`}>Logs</a>
									<button
										type="button"
										class="secondary"
										onclick={() => (download = { domain: 'md', streamId: venue })}
									>
										Download
									</button>
								</div>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	</section>

	<EventLogDownload {sessionId} />
	<LogViewer
		domain="sts"
		streamId={sessionId}
		title="STS log"
		subtitle="This session's stream (indexed by session_id)"
	/>
{/if}

{#if download}
	<LogDownloadModal
		domain={download.domain}
		streamId={download.streamId}
		open={true}
		onclose={() => (download = null)}
	/>
{/if}

<style>
	.back {
		margin: 0 0 1rem;
		font-size: 0.9rem;
	}

	.mono {
		font-family: var(--font);
		word-break: break-all;
	}

	.head-actions {
		display: flex;
		align-items: center;
		gap: 0.6rem;
	}

	.summary {
		display: grid;
		gap: 1rem;
		margin-bottom: 1rem;
	}

	dl {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr));
		gap: 0.75rem 1.25rem;
		margin: 0;
	}

	dt {
		color: var(--muted);
		font-size: 0.75rem;
		text-transform: uppercase;
		letter-spacing: 0.06em;
	}

	dd {
		margin: 0.25rem 0 0;
	}

	.panel {
		margin-bottom: 1rem;
	}

	.panel h2 {
		margin: 0 0 0.35rem;
		font-size: 1rem;
	}

	.note {
		margin: 0 0 0.85rem;
		font-size: 0.82rem;
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

	.feeds {
		display: flex;
		flex-wrap: wrap;
		gap: 0.35rem;
	}

	.feeds code {
		font-size: 0.78rem;
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

	.actions button.active {
		border-color: var(--accent);
		color: var(--text);
	}
</style>
