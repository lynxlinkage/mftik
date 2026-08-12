<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { api, formatDecimal, formatTs, type SymbolInfo } from '$lib/api';
	import {
		cacheMatches,
		loadPageCache,
		loadPreferredVenue,
		savePageCache,
		savePreferredVenue,
		type SymPageCache
	} from '$lib/symCache';

	const PAGE_SIZE = 100;
	const SEARCH_DEBOUNCE_MS = 250;

	let symbols = $state<SymbolInfo[]>([]);
	let total = $state(0);
	let venues = $state<string[]>([]);
	let counts = $state<Record<string, number>>({});
	let error = $state<string | null>(null);
	let loading = $state(true);
	let refreshing = $state(false);

	/** Empty string means All — never the initial load unless remembered. */
	let venue = $state('');
	let query = $state('');
	let debouncedQuery = $state('');
	let includeInactive = $state(false);
	let offset = $state(0);
	let ready = $state(false);

	/** Multiple rows can stay open so filters can be compared side by side. */
	let expanded = $state<Record<string, true>>({});
	let details = $state<Record<string, SymbolInfo>>({});
	let detailLoading = $state<Record<string, true>>({});
	let detailErrors = $state<Record<string, string>>({});

	let searchTimer: ReturnType<typeof setTimeout> | null = null;
	/** Drop stale list responses when the user clicks faster than the plane. */
	let refreshSeq = 0;

	function clearDetails() {
		expanded = {};
		details = {};
		detailLoading = {};
		detailErrors = {};
	}

	/** `Gate_Spot_BTCUSDT` → `{ venue, category, symbol }`. */
	function parts(s: SymbolInfo): { venue: string; category: string; symbol: string } {
		const [v = '', category = '', symbol = ''] = s.universal_ticker.split('_');
		return { venue: v, category, symbol };
	}

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

	const pageEnd = $derived(Math.min(offset + symbols.length, total));
	const pageLabel = $derived(
		total === 0 ? '0 instruments' : `${offset + 1}–${pageEnd} of ${total}`
	);
	const canPrev = $derived(offset > 0);
	const canNext = $derived(offset + PAGE_SIZE < total);

	function applyCache(cache: SymPageCache) {
		venues = cache.venues;
		counts = cache.counts;
		symbols = cache.symbols;
		total = cache.total;
		venue = cache.venue;
		includeInactive = cache.includeInactive;
		query = cache.query;
		debouncedQuery = cache.query;
		offset = cache.offset;
	}

	function persistCache(page: {
		venue: string;
		includeInactive: boolean;
		query: string;
		offset: number;
		symbols: SymbolInfo[];
		total: number;
	}) {
		savePageCache({
			...page,
			limit: PAGE_SIZE,
			venues,
			counts
		});
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

	async function refresh(opts: { background?: boolean } = {}) {
		const background = opts.background ?? false;
		const seq = ++refreshSeq;
		const req = {
			venue,
			includeInactive,
			query: debouncedQuery.trim(),
			offset
		};
		if (background) refreshing = true;
		else loading = true;
		error = null;
		try {
			let res = await api.symbols({
				venue: req.venue || undefined,
				activeOnly: !req.includeInactive,
				q: req.query || undefined,
				limit: PAGE_SIZE,
				offset: req.offset,
				slim: true
			});
			if (seq !== refreshSeq) return;

			let usedOffset = req.offset;
			if (usedOffset > 0 && usedOffset >= res.total) {
				usedOffset = Math.max(0, Math.floor(Math.max(res.total - 1, 0) / PAGE_SIZE) * PAGE_SIZE);
				if (usedOffset !== req.offset) {
					res = await api.symbols({
						venue: req.venue || undefined,
						activeOnly: !req.includeInactive,
						q: req.query || undefined,
						limit: PAGE_SIZE,
						offset: usedOffset,
						slim: true
					});
					if (seq !== refreshSeq) return;
					offset = usedOffset;
				}
			}

			symbols = res.symbols;
			total = res.total;
			persistCache({
				venue: req.venue,
				includeInactive: req.includeInactive,
				query: req.query,
				offset: usedOffset,
				symbols: res.symbols,
				total: res.total
			});
		} catch (e) {
			if (seq !== refreshSeq) return;
			error = e instanceof Error ? e.message : String(e);
			if (!background) {
				symbols = [];
				total = 0;
			}
		} finally {
			if (seq === refreshSeq) {
				loading = false;
				refreshing = false;
			}
		}
	}

	function setVenue(next: string) {
		venue = next;
		offset = 0;
		clearDetails();
		savePreferredVenue(next);
		void refresh();
	}

	function toggleInactive() {
		includeInactive = !includeInactive;
		offset = 0;
		clearDetails();
		void refresh();
	}

	function onQueryInput(event: Event) {
		const value = (event.target as HTMLInputElement).value;
		query = value;
		if (searchTimer) clearTimeout(searchTimer);
		searchTimer = setTimeout(() => {
			debouncedQuery = value;
			offset = 0;
			clearDetails();
			void refresh();
		}, SEARCH_DEBOUNCE_MS);
	}

	function prevPage() {
		offset = Math.max(0, offset - PAGE_SIZE);
		clearDetails();
		void refresh();
	}

	function nextPage() {
		offset = offset + PAGE_SIZE;
		clearDetails();
		void refresh();
	}

	async function toggleDetail(s: SymbolInfo) {
		const key = s.universal_ticker;
		if (expanded[key]) {
			const { [key]: _e, ...restExpanded } = expanded;
			expanded = restExpanded;
			const { [key]: _d, ...restDetails } = details;
			details = restDetails;
			const { [key]: _l, ...restLoading } = detailLoading;
			detailLoading = restLoading;
			const { [key]: _err, ...restErrors } = detailErrors;
			detailErrors = restErrors;
			return;
		}
		expanded = { ...expanded, [key]: true };
		const { [key]: _err, ...restErrors } = detailErrors;
		detailErrors = restErrors;
		if (details[key]) return;
		detailLoading = { ...detailLoading, [key]: true };
		try {
			// Exact ticker match on the plane — do not widen to a venue list.
			const res = await api.symbols({
				universalTicker: key,
				activeOnly: false,
				slim: false
			});
			const row = res.symbols[0];
			if (!row) {
				detailErrors = { ...detailErrors, [key]: 'Instrument not found' };
			} else {
				details = { ...details, [key]: row };
			}
		} catch (e) {
			detailErrors = {
				...detailErrors,
				[key]: e instanceof Error ? e.message : String(e)
			};
		} finally {
			const { [key]: _l, ...restLoading } = detailLoading;
			detailLoading = restLoading;
		}
	}

	onMount(async () => {
		const cached = loadPageCache();
		const preferred = loadPreferredVenue();

		await loadVenues();

		if (cached && venues.length > 0) {
			// Prefer the remembered venue when the cache is for a different tab.
			const want =
				preferred != null && (preferred === '' || venues.includes(preferred))
					? preferred
					: cached.venue;
			venue = want;
			includeInactive = cached.includeInactive;
			query = cached.query;
			debouncedQuery = cached.query;
			offset = want === cached.venue ? cached.offset : 0;
			if (
				cacheMatches(cached, {
					venue: want,
					includeInactive: cached.includeInactive,
					query: cached.query,
					offset,
					limit: PAGE_SIZE
				})
			) {
				applyCache({ ...cached, venue: want, offset, venues, counts });
				ready = true;
				void refresh({ background: true });
				return;
			}
		} else if (preferred != null && (preferred === '' || venues.includes(preferred))) {
			venue = preferred;
		} else {
			venue = venues[0] ?? '';
		}

		savePreferredVenue(venue);
		ready = true;
		await refresh();
	});

	onDestroy(() => {
		if (searchTimer) clearTimeout(searchTimer);
		refreshSeq += 1;
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
	<button type="button" class="secondary" onclick={() => refresh()} disabled={loading || refreshing}>
		{refreshing ? 'Refreshing…' : 'Refresh'}
	</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

{#if ready}
	<div class="tabs" role="tablist">
		<button type="button" class:active={venue === ''} onclick={() => setVenue('')}>
			All
		</button>
		{#each venues as v (v)}
			<button type="button" class:active={venue === v} onclick={() => setVenue(v)}>
				{v}
				{#if counts[v] != null}<span class="count">{counts[v]}</span>{/if}
			</button>
		{/each}
	</div>
{/if}

<section class="panel controls">
	<label>
		Search
		<input
			value={query}
			oninput={onQueryInput}
			disabled={loading && symbols.length === 0}
			placeholder="BTC, USDT, Gate_Spot_BTCUSDT…"
			autocomplete="off"
		/>
	</label>
	<label class="check">
		<input
			type="checkbox"
			checked={includeInactive}
			disabled={loading && symbols.length === 0}
			onchange={toggleInactive}
		/>
		Include delisted
	</label>
	<p class="summary">
		{#if loading && symbols.length === 0}
			Loading…
		{:else}
			{pageLabel}
			{#if refreshing}
				· refreshing…
			{/if}
		{/if}
	</p>
</section>

<section class="panel table-wrap">
	{#if !ready || (loading && symbols.length === 0)}
		<p class="empty-state">Loading…</p>
	{:else if symbols.length === 0}
		<p class="empty-state">
			{#if debouncedQuery.trim()}
				Nothing matches “{debouncedQuery.trim()}”.
			{:else}
				No instruments. The plane refreshes hourly — check that <code>sym</code> is running.
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
				{#each symbols as s (rowKey(s))}
					<tr
						class="row"
						class:open={!!expanded[s.universal_ticker]}
						onclick={() => toggleDetail(s)}
					>
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
					{#if expanded[s.universal_ticker]}
						<tr class="detail">
							<td colspan="11">
								{#if detailLoading[s.universal_ticker]}
									<span class="muted">Loading filters…</span>
								{:else if detailErrors[s.universal_ticker]}
									<span class="err">{detailErrors[s.universal_ticker]}</span>
								{:else if details[s.universal_ticker]}
									<div class="filters">
										{#each details[s.universal_ticker].filters as f (f.name)}
											<span>
												<code>{f.name}</code>
												{formatDecimal(f.value) ?? 'none'}
											</span>
										{:else}
											<span class="muted">No filters published.</span>
										{/each}
									</div>
								{/if}
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	{/if}
</section>

{#if ready && total > PAGE_SIZE}
	<section class="pager">
		<button type="button" class="secondary" onclick={prevPage} disabled={!canPrev || loading}>
			Previous
		</button>
		<span class="muted">{pageLabel}</span>
		<button type="button" class="secondary" onclick={nextPage} disabled={!canNext || loading}>
			Next
		</button>
	</section>
{/if}

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

	.row {
		cursor: pointer;
	}

	.row.open {
		background: color-mix(in srgb, var(--text) 6%, transparent);
	}

	.detail td {
		padding: 0.65rem 0.75rem 0.85rem;
		background: color-mix(in srgb, var(--text) 3%, transparent);
	}

	.filters {
		display: flex;
		flex-wrap: wrap;
		gap: 0.65rem 1.1rem;
		font-size: 0.82rem;
	}

	.filters code {
		margin-right: 0.35rem;
		color: var(--muted);
	}

	.err {
		color: var(--danger, #c44);
		font-size: 0.82rem;
	}

	.pager {
		display: flex;
		align-items: center;
		justify-content: center;
		gap: 1rem;
		margin-top: 1rem;
	}
</style>
