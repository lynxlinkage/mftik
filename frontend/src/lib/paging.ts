/**
 * Numbered pages over a limit/offset list.
 *
 * One formula, because the four session lists each need it twice — once
 * for the pager and once to clamp a page number that has run past the end
 * of a list that shrank — and two spellings of it drift.
 */

/**
 * How far the API will page. Mirrors `MAX_LIST_OFFSET` in
 * `apps/api/src/mftik_api/deps.py`, where an `offset` past it is a 422:
 * an offset page makes the database walk every index entry it skips, so
 * the far side of a browse is a refusal rather than a slow scan.
 *
 * It is a copy, not an import — there is no codegen between the contract
 * and this app. If the API's cap moves, this one has to move with it.
 */
export const MAX_LIST_OFFSET = 100_000;

/**
 * How many pages of `total` rows a caller can reach.
 *
 * Always at least one: page one exists even for an empty list. Never more
 * than {@link MAX_LIST_OFFSET} allows, so the pager cannot offer a number
 * the API would refuse — a list longer than that keeps its oldest rows,
 * they just stop being reachable by page number.
 */
export function pageCountOf(total: number, pageSize: number): number {
	if (!Number.isFinite(total) || total <= 0) return 1;
	const filled = Math.ceil(total / pageSize);
	const reachable = Math.floor(MAX_LIST_OFFSET / pageSize) + 1;
	return Math.max(1, Math.min(filled, reachable));
}
