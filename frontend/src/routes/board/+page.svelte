<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { api, formatTs, shortId, type BoardFill, type BoardSession } from '$lib/api';
	import { handleUnauthorized } from '$lib/auth';
	import BindingChips from '$lib/components/BindingChips.svelte';
	import Pager from '$lib/components/Pager.svelte';
	import { connectFills, type FillConnection } from '$lib/logging/fills';
	import { symbolsFromTickers, venuesFromTickers } from '$lib/ticker';
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
	 * Sessions can be cards or a table — same facts, the switch only changes
	 * layout. The last tab is always rows: what it lists has no run to be a
	 * card of. A card would need a heading naming whose they are, which is the
	 * one thing nobody knows.
	 */

	type Tab = 'all' | 'live' | 'done' | 'external';
	type Layout = 'cards' | 'table';

	const LAYOUT_KEY = 'mftik.board.layout';
	const PAGE_SIZE = 50;

	let sessions = $state<BoardSession[]>([]);
	let page = $state(1);
	let total = $state(0);
	let external = $state<BoardFill[]>([]);
	let externalMore = $state(false);
	let live = $state<Record<string, number>>({});
	let connection = $state<FillConnection>('connecting');
	let filter = $state<Tab>('all');
	let layout = $state<Layout>('cards');
	let error = $state<string | null>(null);
	let loading = $state(true);
	let loadingMore = $state(false);
	let disconnect: (() => void) | null = null;

	let listEpoch = 0;

	const pageCount = $derived(Math.max(1, Math.ceil(total / PAGE_SIZE)));

	function sessionStatus(which: Tab): string | undefined {
		if (which === 'all' || which === 'external') return undefined;
		return which === 'live' ? 'live' : 'done,ack';
	}

	async function refresh() {
		const epoch = ++listEpoch;
		const myFilter = filter;
		loading = true;
		error = null;
		try {
			if (myFilter === 'external') {
				const res = await api.boardExternalFills({ limit: 100 });
				if (epoch !== listEpoch || filter !== myFilter) return;
				external = res.fills;
				externalMore = res.has_more;
			} else {
				let myPage = page;
				let offset = Math.max(0, (myPage - 1) * PAGE_SIZE);
				let res = await api.boardSessions({
					status: sessionStatus(myFilter),
					limit: PAGE_SIZE,
					offset
				});
				if (epoch !== listEpoch || filter !== myFilter) return;
				if (offset > 0 && offset >= res.total) {
					myPage = Math.max(1, Math.ceil(Math.max(res.total, 0) / PAGE_SIZE));
					offset = (myPage - 1) * PAGE_SIZE;
					page = myPage;
					res = await api.boardSessions({
						status: sessionStatus(myFilter),
						limit: PAGE_SIZE,
						offset
					});
					if (epoch !== listEpoch || filter !== myFilter) return;
				}
				sessions = res.sessions;
				total = res.total ?? 0;
				// The server just told us the truth; anything counted locally is
				// already inside it.
				live = {};
			}
		} catch (e) {
			if (epoch !== listEpoch) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (epoch === listEpoch) loading = false;
		}
	}

	async function loadMore() {
		if (loadingMore) return;
		const epoch = listEpoch;
		const myFilter = filter;
		loadingMore = true;
		error = null;
		try {
			const oldest = external.at(-1);
			if (myFilter !== 'external' || !oldest) return;
			const next = await api.boardExternalFills({
				beforeTs: oldest.ts,
				beforeId: oldest.id,
				limit: 100
			});
			if (epoch !== listEpoch || filter !== myFilter) return;
			external = [...external, ...next.fills];
			externalMore = next.has_more;
		} catch (e) {
			if (epoch !== listEpoch) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loadingMore = false;
		}
	}

	function setFilter(next: Tab) {
		if (next === filter) return;
		filter = next;
		page = 1;
		total = 0;
		listEpoch += 1;
		sessions = [];
		external = [];
		externalMore = false;
		error = null;
		loading = true;
		void refresh();
	}

	function setPage(next: number) {
		if (next === page || next < 1) return;
		page = next;
		sessions = [];
		error = null;
		loading = true;
		void refresh();
	}

	function setLayout(next: Layout) {
		layout = next;
		try {
			localStorage.setItem(LAYOUT_KEY, next);
		} catch {
			// private mode — the switch still works for this visit
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

	function fillsOf(s: BoardSession): number {
		return s.fills + (live[s.session_id] ?? 0);
	}

	async function start() {
		try {
			const status = await api.authStatus();
			if (status.enabled && !status.authenticated) {
				handleUnauthorized();
				loading = false;
				return;
			}
		} catch {
			/* status is public; a network error is not a verdict */
		}
		void refresh();
		try {
			disconnect = connectFills(
				(event) => {
					live = {
						...live,
						[event.session_id]: (live[event.session_id] ?? 0) + 1
					};
				},
				(state) => (connection = state)
			);
		} catch {
			connection = 'error';
		}
	}

	onMount(() => {
		try {
			const stored = localStorage.getItem(LAYOUT_KEY);
			if (stored === 'cards' || stored === 'table') layout = stored;
		} catch {
			// ignore
		}
		void start();
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

<div class="toolbar">
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
	{#if filter !== 'external'}
		<div class="view-switch" role="group" aria-label="Session layout">
			<button
				type="button"
				class:active={layout === 'cards'}
				onclick={() => setLayout('cards')}
			>
				Cards
			</button>
			<button
				type="button"
				class:active={layout === 'table'}
				onclick={() => setLayout('table')}
			>
				Table
			</button>
		</div>
	{/if}
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
		<Pager {page} {pageCount} disabled={loading} onchange={setPage} />
	</section>
{:else if layout === 'table'}
	<section class="panel">
		<table class="data">
			<thead>
				<tr>
					<th>Strategy</th>
					<th>Status</th>
					<th class="num">Fills</th>
					<th>Symbols</th>
					<th>Venues</th>
					<th>Started</th>
					<th>Ran for</th>
					<th>Session</th>
					<th>Record</th>
				</tr>
			</thead>
			<tbody>
				{#each sessions as s (s.session_id)}
					<tr>
						<td>
							<a href={`/board/${s.session_id}`}>{s.strategy ?? 'unknown'}</a>
						</td>
						<td>
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
						</td>
						<td class="num">
							{fillsOf(s)}
							{#if live[s.session_id]}
								<span class="delta" title="arrived on the live stream since this page loaded">
									+{live[s.session_id]}
								</span>
							{/if}
						</td>
						<td class="bindings-cell">
							<BindingChips items={symbolsFromTickers(s.tickers)} maxChips={2} />
						</td>
						<td class="bindings-cell">
							<BindingChips
								kind="venue"
								items={venuesFromTickers(s.tickers)}
								maxChips={2}
							/>
						</td>
						<td class="muted">{formatTs(s.created_at)}</td>
						<td>{duration(s.duration_s)}</td>
						<td class="mono muted" title={s.session_id}>{shortId(s.session_id)}</td>
						<td>
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
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
		<Pager {page} {pageCount} disabled={loading} onchange={setPage} />
	</section>
{:else}
	<div class="cards">
		{#each sessions as s (s.session_id)}
			<a class="card" href={`/board/${s.session_id}`} title={s.session_id}>
				<header>
					<h3 class="name">{s.strategy ?? 'unknown'}</h3>
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

				<div class="figures">
					<div class="figure">
						<div class="value">{fillsOf(s)}</div>
						<div class="label">
							fills
							{#if live[s.session_id]}
								<span class="delta" title="arrived on the live stream since this page loaded">
									+{live[s.session_id]}
								</span>
							{/if}
						</div>
					</div>
				</div>

				<dl class="run">
					<dt>started</dt>
					<dd>{formatTs(s.created_at)}</dd>
					<dt>ran for</dt>
					<dd>{duration(s.duration_s)}</dd>
				</dl>

				<div class="bindings">
					<BindingChips label="symbols" items={symbolsFromTickers(s.tickers)} />
					<BindingChips
						label="venues"
						kind="venue"
						items={venuesFromTickers(s.tickers)}
					/>
				</div>

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
	<Pager {page} {pageCount} disabled={loading} onchange={setPage} />
{/if}

<style>
	.head-actions {
		display: flex;
		align-items: center;
		gap: 0.75rem;
	}

	.toolbar {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		flex-wrap: wrap;
		margin-bottom: 1rem;
	}

	.toolbar .tabs {
		margin-bottom: 0;
	}

	.view-switch {
		display: flex;
		gap: 0.35rem;
	}

	.view-switch button {
		background: transparent;
		color: var(--muted);
		border: 1px solid transparent;
		font-weight: 500;
		padding: 0.4rem 0.7rem;
	}

	.view-switch button.active {
		color: var(--text);
		border-color: var(--border);
		background: var(--accent-dim);
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
		grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
		gap: 1rem;
	}

	.card {
		display: flex;
		flex-direction: column;
		padding: 1rem 1.125rem;
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

	.name {
		margin: 0;
		font-family: var(--font);
		font-size: 0.9375rem;
		font-weight: 600;
		letter-spacing: -0.01em;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.figures {
		display: flex;
		gap: 1.75rem;
		margin-top: 0.875rem;
	}

	.value {
		font-family: var(--font);
		font-size: 1.375rem;
		font-weight: 500;
		line-height: 1.1;
		font-variant-numeric: tabular-nums;
	}

	.label {
		margin-top: 0.25rem;
		font-family: var(--font);
		font-size: 0.625rem;
		letter-spacing: 0.09em;
		text-transform: uppercase;
		color: var(--muted);
	}

	.delta {
		color: var(--accent);
		font-weight: 600;
		letter-spacing: 0;
		text-transform: none;
	}

	.run {
		display: grid;
		grid-template-columns: max-content 1fr;
		gap: 0.25rem 0.875rem;
		margin: 0.875rem 0 0;
		padding-top: 0.75rem;
		border-top: 1px solid var(--border);
		font-family: var(--font);
		font-size: 0.75rem;
	}

	.run dt {
		color: var(--muted);
		letter-spacing: 0.04em;
	}

	.run dd {
		margin: 0;
		font-variant-numeric: tabular-nums;
	}

	.bindings {
		margin-top: 0.75rem;
		display: grid;
		gap: 0.375rem;
	}

	.bindings-cell {
		max-width: 12rem;
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
		margin-top: 0.75rem;
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
