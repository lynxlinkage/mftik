/**
 * What a table can show of a venue API key without dumping the whole thing.
 *
 * Venue keys are long (Binance ~64). Paper keys are short (`paper-key-3`).
 * Three head and three tail is enough to tell two keys apart and short
 * enough not to stretch the column. Shorter strings keep less, or just
 * `***`, so a 6-character key is not printed in full under a mask.
 */
export function maskApiKey(value: string): string {
	const key = value.trim();
	if (key.length <= 4) return '***';
	if (key.length <= 8) return `${key.slice(0, 2)}***${key.slice(-1)}`;
	return `${key.slice(0, 3)}***${key.slice(-3)}`;
}
