<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { api, formatTs, shortId, type BoardFill, type BoardSession } from '$lib/api';
	import { connectFills, type FillConnection } from '$lib/logging/fills';

	/**
	 * One card per strategy run: how much it traded and how long it ran.
	 *
	 * No PnL. Deriving a result means matching executions into positions and
	 * valuing whatever is left open when a run ends, and a number shown here
	 * before that machinery exists would be read as one that had been computed.
	 *
	 * The fill count comes from the database and is authoritative. `live` counts
	 * what has arrived on the socket since this page loaded, and exists so a
	 * running session moves before the writer's next flush — it is a hint, never
	 * a total, and a refresh replaces it with the real figure.
	 *
	 * The last tab shows rows instead of cards, because what it lists has no run
	 * to be a card of: executions belonging to no session. A card would need a
	 * heading naming whose they are, which is the one thing nobody knows — so
	 * they are listed one execution per line, the way the drill-in lists a run's,
	 * and read rather than counted.
	 */

	type Tab = 'all' | 'live' | 'done' | 'external';

	let sessions = $state<BoardSession[]>([]);
	let external = $state<BoardFill[]>([]);
	let externalMore = $state(false);
	let live = $state<Record<string, number>>({});
	let connection = $state<FillConnection>('connecting');
	let filter = $state<Tab>('all');
	let error = $state<string | null>(null);
	let loading = $state(true);
	let loadingMore = $state(false);
	let disconnect: (() => void) | null = null;

	async function refresh() {
		loading = true;
		error = null;
		try {
			if (filter === 'external') {
				const res = await api.boardExternalFills({ limit: 100 });
				external = res.fills;
				externalMore = res.has_more;
			} else {
				const res = await api.boardSessions(
					filter === 'all'
						? {}
						: { status: filter === 'live' ? 'live' : 'done,ack' }
				);
				sessions = res.sessions;
				// The server just told us the truth; anything counted locally is
				// already inside it.
				live = {};
			}
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore() {
		const oldest = external.at(-1);
		if (!oldest || loadingMore) return;
		loadingMore = true;
		try {
			const next = await api.boardExternalFills({
				beforeTs: oldest.ts,
				beforeId: oldest.id,
				limit: 100
			});
			external = [...external, ...next.fills];
			externalMore = next.has_more;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loadingMore = false;
		}
	}

	function setFilter(next: Tab) {
		filter = next;
		void refresh();
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

	function fillsOf(s: BoardSession): number {
		return s.fills + (live[s.session_id] ?? 0);
	}

	onMount(() => {
		void refresh();
		disconnect = connectFills(
			(event) => {
				live = {
					...live,
					[event.session_id]: (live[event.session_id] ?? 0) + 1
				};
			},
			(state) => (connection = state)
		);
	});

	onDestroy(() => disconnect?.());
</script>

<div class="page-head">
	<div>
		<h1>Board</h1>
		<p>
			How much each run traded and how long it ran. Executions only: every figure here
			is cumulative, so losing a row makes a run look quiet rather than wrong. No PnL
			yet — matching executions into positions is its own piece of work, and a number
			here before that exists would be believed.
		</p>
	</div>
	<div class="head-actions">
		<span class="conn" class:open={connection === 'open'} title={`live stream ${connection}`}>
			{connection === 'open' ? 'live' : connection}
		</span>
		<button type="button" class="secondary" onclick={refresh} disabled={loading}>
			Refresh
		</button>
	</div>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<div class="tabs" role="tablist">
	<button type="button" class:active={filter === 'all'} onclick={() => setFilter('all')}>
		All
	</button>
	<button type="button" class:active={filter === 'live'} onclick={() => setFilter('live')}>
		Live
	</button>
	<button type="button" class:active={filter === 'done'} onclick={() => setFilter('done')}>
		Finished
	</button>
	<button
		type="button"
		class:active={filter === 'external'}
		onclick={() => setFilter('external')}
		title="executions on these accounts that no run of ours placed"
	>
		External
	</button>
</div>

{#if filter === 'external'}
	<section class="panel">
		<p class="note">
			Executions recorded on these accounts that no session placed. Trading done by hand
			or by another tool belongs here and is nothing to fix. Something you recognise as a
			strategy's does not: it means the fill reached the record and its order did not, and
			this is the only listing it appears in.
		</p>

		{#if external.length === 0}
			<p class="empty-state">
				{loading ? 'Loading…' : 'Every execution on file belongs to a run.'}
			</p>
		{:else}
			<table class="data">
				<thead>
					<tr>
						<th>Time</th>
						<th>Account</th>
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
					{#each external as f (f.id)}
						<tr>
							<td class="muted">{formatTs(f.ts)}</td>
							<td><a href={`/td/${f.api_id}`}>{f.api_id}</a></td>
							<td>{f.universal_ticker}</td>
							<td class:sell={f.side === 'sell'}>{f.side}</td>
							<td class="num mono">{f.price}</td>
							<td class="num mono">{f.qty}</td>
							<td class="num mono muted">
								{f.fee === '0' || f.fee === '' ? '—' : `${f.fee} ${f.fee_asset}`}
							</td>
							<td
								class="muted mono"
								title={f.client_order_id ?? f.venue_order_id ?? 'no order id on the fill'}
							>
								{#if f.client_order_id}
									{shortId(f.client_order_id)}
								{:else if f.venue_order_id}
									{shortId(f.venue_order_id)}
								{:else}
									—
								{/if}
							</td>
							<td>
								<span
									class="badge"
									class:done={f.settled}
									class:pending={!f.settled}
									title={f.settled
										? 'the venue has been re-read past this row, orders included — no run of ours claims it'
										: 'not yet re-read against the venue; an order claiming this fill may still arrive'}
								>
									{f.source}
								</span>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>

			{#if externalMore}
				<div class="more">
					<button type="button" class="secondary" onclick={loadMore} disabled={loadingMore}>
						{loadingMore ? 'Loading…' : 'Load older'}
					</button>
				</div>
			{/if}
		{/if}
	</section>
{:else if sessions.length === 0}
	<section class="panel">
		<p class="empty-state">{loading ? 'Loading…' : 'No sessions yet.'}</p>
	</section>
{:else}
	<div class="cards">
		{#each sessions as s (s.session_id)}
			<a class="card" href={`/board/${s.session_id}`} title={s.session_id}>
				<header>
					<span class="strategy">{s.strategy ?? 'unknown'}</span>
					<span
						class="badge"
						class:live={s.status === 'live'}
						class:done={s.status === 'done' || s.status === 'ack'}
						class:failed={s.status === 'failed'}
						class:interrupted={s.status === 'interrupted'}
						title={s.reason ?? ''}
					>
						{s.status}
					</span>
				</header>

				<div class="figure">
					<span class="count">{fillsOf(s)}</span>
					<span class="unit">
						fills
						{#if live[s.session_id]}
							<span class="delta" title="arrived on the live stream since this page loaded">
								+{live[s.session_id]}
							</span>
						{/if}
					</span>
				</div>

				<dl class="facts">
					<div><dt>Started</dt><dd>{formatTs(s.created_at)}</dd></div>
					<div><dt>Ran for</dt><dd>{duration(s.duration_s)}</dd></div>
					<div><dt>Session</dt><dd class="mono">{shortId(s.session_id)}</dd></div>
				</dl>

				<footer>
					{#if s.settled}
						<span class="badge done" title="the venue has been re-read across this whole run">
							settled
						</span>
					{:else}
						<span
							class="badge pending"
							title={s.confirmed_through_ts
								? `confirmed against the venue through ${formatTs(s.confirmed_through_ts)}`
								: 'not yet re-read against the venue'}
						>
							provisional
						</span>
					{/if}
					<span class="go">View trades →</span>
				</footer>
			</a>
		{/each}
	</div>
{/if}

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

	.cards {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(17rem, 1fr));
		gap: 1rem;
	}

	.card {
		display: flex;
		flex-direction: column;
		gap: 0.85rem;
		padding: 1rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		background: var(--panel, transparent);
		color: inherit;
		text-decoration: none;
	}

	.card:hover {
		border-color: var(--accent);
		text-decoration: none;
	}

	.card header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.5rem;
	}

	.strategy {
		font-weight: 700;
	}

	.figure {
		display: flex;
		align-items: baseline;
		gap: 0.4rem;
	}

	.count {
		font-size: 2rem;
		font-weight: 700;
		font-variant-numeric: tabular-nums;
		line-height: 1;
	}

	.unit {
		color: var(--muted);
		font-size: 0.85rem;
	}

	.delta {
		color: var(--accent);
		font-weight: 600;
	}

	.facts {
		display: flex;
		flex-direction: column;
		gap: 0.3rem;
		margin: 0;
		font-size: 0.85rem;
	}

	.facts > div {
		display: flex;
		justify-content: space-between;
		gap: 0.5rem;
	}

	.facts dt {
		color: var(--muted);
	}

	.facts dd {
		margin: 0;
	}

	.mono {
		font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
	}

	.note {
		margin: 0 0 1rem;
		color: var(--muted);
		font-size: 0.85rem;
		max-width: 60rem;
	}

	.num {
		text-align: right;
		font-variant-numeric: tabular-nums;
	}

	.sell {
		color: var(--danger, #c0392b);
	}

	.more {
		display: flex;
		justify-content: center;
		padding-top: 1rem;
	}

	.card footer {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.5rem;
		padding-top: 0.6rem;
		border-top: 1px solid var(--border);
	}

	.go {
		color: var(--muted);
		font-size: 0.85rem;
		font-weight: 600;
	}

	.card:hover .go {
		color: var(--accent);
	}

	.badge.pending {
		color: var(--muted);
	}
</style>
