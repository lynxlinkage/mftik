import type { SymbolInfo } from '$lib/api';

const VENUE_KEY = 'mftik.sym.venue';
const PAGE_KEY = 'mftik.sym.page';

export type SymPageCache = {
	venue: string;
	includeInactive: boolean;
	query: string;
	offset: number;
	limit: number;
	symbols: SymbolInfo[];
	total: number;
	venues: string[];
	counts: Record<string, number>;
};

export function loadPreferredVenue(): string | null {
	try {
		return sessionStorage.getItem(VENUE_KEY);
	} catch {
		return null;
	}
}

export function savePreferredVenue(venue: string): void {
	try {
		sessionStorage.setItem(VENUE_KEY, venue);
	} catch {
		/* private mode / quota */
	}
}

export function loadPageCache(): SymPageCache | null {
	try {
		const raw = sessionStorage.getItem(PAGE_KEY);
		if (!raw) return null;
		return JSON.parse(raw) as SymPageCache;
	} catch {
		return null;
	}
}

export function savePageCache(page: SymPageCache): void {
	try {
		sessionStorage.setItem(PAGE_KEY, JSON.stringify(page));
	} catch {
		/* private mode / quota */
	}
}

export function cacheMatches(
	cache: SymPageCache,
	opts: {
		venue: string;
		includeInactive: boolean;
		query: string;
		offset: number;
		limit: number;
	}
): boolean {
	return (
		cache.venue === opts.venue &&
		cache.includeInactive === opts.includeInactive &&
		cache.query === opts.query &&
		cache.offset === opts.offset &&
		cache.limit === opts.limit
	);
}
