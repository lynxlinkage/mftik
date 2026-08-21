<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { page } from '$app/state';
	import {
		api,
		formatTs,
		shortId,
		type BoardFill,
		type BoardSession
	} from '$lib/api';
	import { connectFills, type FillConnection } from '$lib/logging/fills';
	import { formatTickerTag } from '$lib/ticker';

	/**
	 * Every execution this run recorded, newest first.
	 *
	 * Prices and sizes are strings from the API and stay strings here. They are
	 * NUMERIC(38,18) in the database precisely so nothing rounds them, and
	 * parsing to a JS number on the way to a screen would undo that at the last
	 * possible moment — where nothing downstream could notice.
	 *
	 * Rows arriving live are prepended, but only ones this session placed. The
	 * live event carries less than a stored row does (no fee, no venue ids yet,
	 * since those come from the writer's flush), so it is marked as pending and
	 * a refresh replaces it with the recorded row.
	 */

	const sessionId = $derived(page.params.sessionId ?? '');

	let summary = $state<BoardSession | null>(null);
	let fills = $state<BoardFill[]>([]);
	let pending = $state<BoardFill[]>([]);
	let hasMore = $state(false);
	let connection = $state<FillConnection>('connecting');
	let error = $state<string | null>(null);
	let loading = $state(true);
	let loadingMore = $state(false);
	let disconnect: (() => void) | null = null;
	let exporting = $state(false);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [head, page1] = await Promise.all([
				api.boardSession(sessionId),
				api.boardFills(sessionId, { limit: 100 })
			]);
			summary = head;
			fills = page1.fills;
			hasMore = page1.has_more;
			pending = [];
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore() {
		const oldest = fills.at(-1);
		if (!oldest || loadingMore) return;
		loadingMore = true;
		try {
			const next = await api.boardFills(sessionId, {
				beforeTs: oldest.ts,
				beforeId: oldest.id,
				limit: 100
			});
			fills = [...fills, ...next.fills];
			hasMore = next.has_more;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loadingMore = false;
		}
	}

	async function exportCsv() {
		exporting = true;
		error = null;
		try {
			await api.downloadBoardFillsCsv(sessionId);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			exporting = false;
		}
	}

	function duration(seconds: number): string {
		if (!seconds || seconds < 0) return '—';
		const s = Math.floor(seconds % 60);
		const m = Math.floor((seconds / 60) % 60);
		const h = Math.floor(seconds / 3600);
		if (h > 0) return `${h}h ${m}m`;
		if (m > 0) return `${m}m ${s}s`;
		return `${s}s`;
	}

	onMount(() => {
		void refresh();
		disconnect = connectFills(
			(event) => {
				if (event.session_id !== sessionId) return;
				pending = [
					{
						id: -pending.length - 1,
						fill_id: '',
						universal_ticker: event.universal_ticker ?? '',
						side: event.side ?? '',
						price: event.price ?? '',
						qty: event.qty ?? '',
						fee: '',
						fee_asset: '',
						client_order_id: null,
						venue_order_id: null,
						api_id: event.api_id,
						ts: event.ts,
						source: 'stream',
						settled: false
					},
					...pending
				];
			},
			(state) => (connection = state)
		);
	});

	onDestroy(() => disconnect?.());
</script>

<div class="page-head">
	<div>
		<h1>{summary?.strategy ?? 'Run'}</h1>
		<p class="mono muted">{sessionId}</p>
	</div>
	<div class="head-actions">
		<span class="conn" class:open={connection === 'open'} title={`live stream ${connection}`}>
			{connection === 'open' ? 'live' : connection}
		</span>
		<a class="link-btn" href="/board">← Board</a>
		<button type="button" class="secondary" onclick={exportCsv} disabled={exporting || loading}>
			{exporting ? 'Exporting…' : 'Export CSV'}
		</button>
		<button type="button" class="secondary" onclick={refresh} disabled={loading}>
			Refresh
		</button>
	</div>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

{#if summary}
	<section class="panel summary">
		<dl>
			<div>
				<dt>Status</dt>
				<dd>
					<span
						class="badge"
						class:live={summary.status === 'live'}
						class:done={summary.status === 'done' || summary.status === 'ack'}
						class:failed={summary.status === 'failed'}
						class:interrupted={summary.status === 'interrupted'}
						title={summary.reason ?? ''}
					>
						{summary.status}
					</span>
				</dd>
			</div>
			<div><dt>Fills</dt><dd>{summary.fills + pending.length}</dd></div>
			<div><dt>Started</dt><dd>{formatTs(summary.created_at)}</dd></div>
			<div><dt>Ran for</dt><dd>{duration(summary.duration_s)}</dd></div>
			<div>
				<dt>Record</dt>
				<dd>
					{#if summary.settled}
						<span class="badge done" title="the venue has been re-read across this whole run">
							settled
						</span>
					{:else}
						<span
							class="badge pending"
							title={summary.confirmed_through_ts
								? `confirmed against the venue through ${formatTs(summary.confirmed_through_ts)}`
								: 'not yet re-read against the venue'}
						>
							provisional
						</span>
					{/if}
				</dd>
			</div>
			<div>
				<dt>Accounts</dt>
				<dd>
					{#each summary.td_api_ids as id, i (id)}
						<a href={`/td/${id}`}>{id}</a>{i < summary.td_api_ids.length - 1 ? ', ' : ''}
					{:else}
						—
					{/each}
				</dd>
			</div>
			{#if summary.tickers.length}
				<div class="instruments">
					<dt>Instruments</dt>
					<dd class="tags">
						{#each summary.tickers as ticker (ticker)}
							<span class="badge" title={ticker}>{formatTickerTag(ticker)}</span>
						{/each}
					</dd>
				</div>
			{/if}
		</dl>
	</section>
{/if}

<section class="panel">
	{#if fills.length === 0 && pending.length === 0}
		<p class="empty-state">
			{loading ? 'Loading…' : 'No executions recorded for this run.'}
		</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Time</th>
					<th>Instrument</th>
					<th>Side</th>
					<th class="num">Price</th>
					<th class="num">Qty</th>
					<th class="num">Fee</th>
					<th>Order</th>
					<th>Source</th>
				</tr>
			</thead>
			<tbody>
				{#each pending as f (f.id)}
					<tr class="arriving">
						<td class="muted">{formatTs(f.ts)}</td>
						<td>{f.universal_ticker || '—'}</td>
						<td class:sell={f.side === 'sell'}>{f.side || '—'}</td>
						<td class="num mono">{f.price || '—'}</td>
						<td class="num mono">{f.qty || '—'}</td>
						<td class="num muted">—</td>
						<td class="muted">—</td>
						<td>
							<span class="badge pending" title="just arrived on the live stream; not yet stored">
								arriving
							</span>
						</td>
					</tr>
				{/each}
				{#each fills as f (f.id)}
					<tr>
						<td class="muted">{formatTs(f.ts)}</td>
						<td>{f.universal_ticker}</td>
						<td class:sell={f.side === 'sell'}>{f.side}</td>
						<td class="num mono">{f.price}</td>
						<td class="num mono">{f.qty}</td>
						<td class="num mono muted">
							{f.fee === '0' || f.fee === '' ? '—' : `${f.fee} ${f.fee_asset}`}
						</td>
						<td class="muted mono" title={f.client_order_id ?? f.venue_order_id ?? ''}>
							{f.client_order_id ? shortId(f.client_order_id) : '—'}
						</td>
						<td>
							<span
								class="badge"
								class:done={f.settled}
								class:pending={!f.settled}
								title={f.settled
									? 're-read from the venue and agreed'
									: 'recorded live; not yet confirmed against the venue'}
							>
								{f.source}
							</span>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>

		{#if hasMore}
			<div class="more">
				<button type="button" class="secondary" onclick={loadMore} disabled={loadingMore}>
					{loadingMore ? 'Loading…' : 'Load older'}
				</button>
			</div>
		{/if}
	{/if}
</section>

<style>
	.head-actions {
		display: flex;
		align-items: center;
		gap: 0.75rem;
	}

	.conn {
		font-size: 0.8rem;
		font-weight: 600;
		color: var(--muted);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.25rem 0.5rem;
	}

	.conn.open {
		color: var(--accent);
		border-color: var(--accent);
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

	.summary dl {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(9rem, 1fr));
		gap: 1rem;
		margin: 0;
	}

	.summary dt {
		color: var(--muted);
		font-size: 0.8rem;
		margin-bottom: 0.25rem;
	}

	.summary dd {
		margin: 0;
		font-weight: 600;
	}

	.summary .instruments {
		grid-column: 1 / -1;
	}

	.tags {
		display: flex;
		flex-wrap: wrap;
		gap: 0.3rem;
		font-weight: 500;
	}

	.num {
		text-align: right;
		font-variant-numeric: tabular-nums;
	}

	.mono {
		font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
	}

	.sell {
		color: var(--danger, #c0392b);
	}

	.arriving {
		opacity: 0.75;
	}

	.badge.pending {
		color: var(--muted);
	}

	.more {
		display: flex;
		justify-content: center;
		padding-top: 1rem;
	}
</style>
