/**
 * Numbered pages over a limit/offset list.
 *
 * One formula, because the four session lists each need it twice — once
 * for the pager and once to clamp a page number that has run past the end
 * of a list that shrank — and two spellings of it drift.
 */

/** How many pages `total` rows fill. Always at least one: page one exists. */
export function pageCountOf(total: number, pageSize: number): number {
	if (!Number.isFinite(total) || total <= 0) return 1;
	return Math.max(1, Math.ceil(total / pageSize));
}
