/**
 * Fold a classic venue pair onto one brand, keep the category.
 *
 * `Gate_Spot_BTCUSDT` and `GateFutures_Perp_BTCUSDT` must not collapse to the
 * same label — category is what tells the legs of a cash-and-carry apart.
 * Underscore cannot appear inside a part, so a three-way split is the parse.
 */

const BRAND: Record<string, string> = {
	GateFutures: 'Gate',
	BinanceFuture: 'Binance'
};

export type TickerParts = {
	venue: string;
	category: string;
	symbol: string;
};

export function parseTicker(ticker: string): TickerParts {
	const [venue = '', category = '', symbol = ''] = ticker.split('_');
	return { venue, category, symbol };
}

export function formatTickerTag(ticker: string): string {
	const { venue, category, symbol } = parseTicker(ticker);
	if (!venue || !category || !symbol) return ticker;
	return `${BRAND[venue] ?? venue} ${category} ${symbol}`;
}

function unique(labels: Iterable<string>): string[] {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const label of labels) {
		if (!label || seen.has(label)) continue;
		seen.add(label);
		out.push(label);
	}
	return out;
}

/** Instrument names a run traded, first-seen order. */
export function symbolsFromTickers(tickers: string[]): string[] {
	return unique((tickers ?? []).map((t) => parseTicker(t).symbol || t));
}

/**
 * Venues those instruments sit on. The registry name is kept — folding
 * GateFutures onto Gate would hide which book the order actually hit.
 */
export function venuesFromTickers(tickers: string[]): string[] {
	return unique((tickers ?? []).map((t) => parseTicker(t).venue));
}
