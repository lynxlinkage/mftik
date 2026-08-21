/**
 * Board tags: fold a classic venue pair onto one brand, keep the category.
 *
 * `Gate_Spot_BTCUSDT` and `GateFutures_Perp_BTCUSDT` must not collapse to the
 * same label — category is what tells the legs of a cash-and-carry apart.
 * Underscore cannot appear inside a part, so a three-way split is the parse.
 */

const BRAND: Record<string, string> = {
	GateFutures: 'Gate',
	BinanceFuture: 'Binance'
};

export const CARD_TAG_LIMIT = 3;

export function formatTickerTag(ticker: string): string {
	const [venue = '', category = '', symbol = ''] = ticker.split('_');
	if (!venue || !category || !symbol) return ticker;
	return `${BRAND[venue] ?? venue} ${category} ${symbol}`;
}

export function visibleTags(tickers: string[]): { shown: string[]; extra: number } {
	if (tickers.length <= CARD_TAG_LIMIT) {
		return { shown: tickers, extra: 0 };
	}
	return {
		shown: tickers.slice(0, CARD_TAG_LIMIT),
		extra: tickers.length - CARD_TAG_LIMIT
	};
}
