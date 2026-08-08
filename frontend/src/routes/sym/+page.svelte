<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatDecimal, formatTs, type SymbolInfo } from '$lib/api';

	// The plane holds thousands of instruments per venue. Rendering all of them
	// stalls the page for no benefit, so cap the table and tell the user to
	// narrow instead.
	const MAX_ROWS = 300;

	let symbols = $state<SymbolInfo[]>([]);
	let venues = $state<string[]>([]);
	let counts = $state<Record<string, number>>({});
	let error = $state<string | null>(null);
	let loading = $state(true);

	let venue = $state('');
	let query = $state('');
	let includeInactive = $state(false);

	/** `Gate_Spot_BTCUSDT` → `{ venue, category, symbol }`.
	 *
	 * The wire carries the one identity string; the table shows its parts. `_`
	 * separates and cannot appear inside a part, so a plain split is exact. */
	function parts(s: SymbolInfo): { venue: string; category: string; symbol: string } {
		const [venue = '', category = '', symbol = ''] = s.universal_ticker.split('_');
		return { venue, category, symbol };
	}

	const filtered = $derived.by(() => {
		const q = query.trim().toUpperCase();
		if (!q) return symbols;
		return symbols.filter(
			(s) =>
				s.universal_ticker.toUpperCase().includes(q) ||
				s.exch_ticker.toUpperCase().includes(q) ||
				s.base.toUpperCase().includes(q) ||
				s.quote.toUpperCase().includes(q)
		);
	});
	const shown = $derived(filtered.slice(0, MAX_ROWS));
	const truncated = $derived(filtered.length - shown.length);

	/** The universal ticker is the plane's identity, so it is also the row key. */
	function rowKey(s: SymbolInfo): string {
		return s.universal_ticker;
	}

	/** ``—`` the venue does not publish this restriction at all; ``none`` it
	 * publishes it with no bound. The plane keeps the key precisely so those
	 * two stay distinguishable. */
	function filterValue(s: SymbolInfo, name: string): string {
		const row = s.filters.find((f) => f.name === name);
		if (row === undefined) return '—';
		return formatDecimal(row.value) ?? 'none';
	}

	async function loadVenues() {
		try {
			const res = await api.symVenues();
			venues = res.venues;
			counts = res.counts;
		} catch {
			venues = [];
			counts = {};
		}
	}

	async function refresh() {
		loading = true;
		error = null;
		try {
			const res = await api.symbols({
				venue: venue || undefined,
				activeOnly: !includeInactive
			});
			symbols = res.symbols;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
			symbols = [];
		} finally {
			loading = false;
		}
	}

	function setVenue(next: string) {
		venue = next;
		void refresh();
	}

	function toggleInactive() {
		includeInactive = !includeInactive;
		void refresh();
	}

	onMount(async () => {
		await loadVenues();
		await refresh();
	});
</script>

<div class="page-head">
	<div>
		<h1>Sym</h1>
		<p>
			Instruments the symbol plane tracks — the golden record for tick sizes and lot steps. The
			<strong>Ticker</strong> column is the platform's identity for an instrument
			(<code>Venue_Category_SYMBOL</code>): that is what strategy.yml <code>md</code> feeds
			(<code>topic.Ticker</code>, e.g. <code>bestquote.Gate_Spot_ETHUSDT</code>) take, not the
			venue's own spelling.
		</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<div class="tabs" role="tablist">
	<button type="button" class:active={venue === ''} onclick={() => setVenue('')}>All</button>
	{#each venues as v (v)}
		<button type="button" class:active={venue === v} onclick={() => setVenue(v)}>
			{v}
			{#if counts[v] != null}<span class="count">{counts[v]}</span>{/if}
		</button>
	{/each}
</div>

<section class="panel controls">
	<label>
		Search
		<input
			bind:value={query}
			disabled={loading}
			placeholder="BTC, USDT, Gate_Spot_BTCUSDT…"
			autocomplete="off"
		/>
	</label>
	<label class="check">
		<input type="checkbox" checked={includeInactive} disabled={loading} onchange={toggleInactive} />
		Include delisted
	</label>
	<p class="summary">
		{#if loading}
			Loading…
		{:else}
			{filtered.length} of {symbols.length} instrument{symbols.length === 1 ? '' : 's'}
			{#if truncated > 0}
				· showing first {MAX_ROWS}, narrow the search to see the other {truncated}
			{/if}
		{/if}
	</p>
</section>

<section class="panel table-wrap">
	{#if shown.length === 0}
		<p class="empty-state">
			{#if loading}
				Loading…
			{:else if symbols.length === 0}
				No instruments. The plane refreshes hourly — check that <code>sym</code> is running.
			{:else}
				Nothing matches “{query}”.
			{/if}
		</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Ticker</th>
					<th>Venue</th>
					<th>Category</th>
					<th>Pair</th>
					<th>Venue ticker</th>
					<th>Price tick</th>
					<th>Qty step</th>
					<th>Min qty</th>
					<th>Min notional</th>
					<th>Status</th>
					<th>Updated</th>
				</tr>
			</thead>
			<tbody>
				{#each shown as s (rowKey(s))}
					<tr>
						<td><code class="sym">{s.universal_ticker}</code></td>
						<td><code>{parts(s).venue}</code></td>
						<td class="muted">{parts(s).category}</td>
						<td class="muted">{s.base}/{s.quote}</td>
						<td><code>{s.exch_ticker}</code></td>
						<td class="num">{filterValue(s, 'price_tick')}</td>
						<td class="num">{filterValue(s, 'qty_step')}</td>
						<td class="num">{filterValue(s, 'min_qty')}</td>
						<td class="num">{filterValue(s, 'min_notional')}</td>
						<td>
							<span class="badge" class:live={s.is_active} class:done={!s.is_active}>
								{s.is_active ? 'active' : 'delisted'}
							</span>
						</td>
						<td class="muted">{formatTs(s.updated_at)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

<style>
	.controls {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 0.85rem;
		margin-bottom: 1rem;
	}

	label {
		display: grid;
		gap: 0.35rem;
		font-size: 0.8rem;
		color: var(--muted);
	}

	label.check {
		display: flex;
		align-items: center;
		gap: 0.4rem;
	}

	.controls input:not([type]) {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.55rem 0.65rem;
		border-radius: var(--radius);
		min-width: 14rem;
	}

	.summary {
		margin: 0 0 0 auto;
		font-size: 0.78rem;
		color: var(--muted);
	}

	.count {
		margin-left: 0.35rem;
		font-size: 0.72rem;
		color: var(--muted);
	}

	.table-wrap {
		overflow-x: auto;
	}

	code {
		font-family: var(--font);
		font-size: 0.82rem;
	}

	.sym {
		color: var(--text);
		font-weight: 600;
	}

	.num {
		font-family: var(--font);
		font-size: 0.82rem;
		text-align: right;
		white-space: nowrap;
	}
</style>
