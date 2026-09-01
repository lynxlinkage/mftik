/**
 * Numbered pages over a limit/offset list.
 *
 * One formula, because the four session lists each need it twice — once
 * for the pager and once to clamp a page number that has run past the end
 * of a list that shrank — and two spellings of it drift.
 */

/**
 * How many pages of `total` rows a caller can reach.
 *
 * Always at least one: page one exists even for an empty list.
 *
 * `maxOffset` is the `max_offset` the list served — how far the API will
 * page before answering a 422, because an offset page makes the database
 * walk every index entry it skips. Passing it keeps the pager from
 * offering a number the API would refuse; a list longer than that keeps
 * its oldest rows, they just stop being reachable by page number. Omit it
 * before the first response has said, when there is nothing to page yet.
 */
export function pageCountOf(total: number, pageSize: number, maxOffset?: number): number {
	if (!Number.isFinite(total) || total <= 0) return 1;
	const filled = Math.ceil(total / pageSize);
	const reachable =
		maxOffset != null && Number.isFinite(maxOffset)
			? Math.floor(maxOffset / pageSize) + 1
			: filled;
	return Math.max(1, Math.min(filled, reachable));
}
