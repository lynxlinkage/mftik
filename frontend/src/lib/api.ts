export type DomainStats = {
	domain: string;
	live: number;
	done: number;
	healthy: boolean | null;
};

export type Session = {
	session_id: string;
	domain: string;
	created_by: number;
	created_at: number;
	finished_at: number | null;
	status: string;
	api_id: number | null;
	api_name: string | null;
	sts_session_id: string | null;
	strategy: string | null;
	paused: boolean | null;
	venue: string | null;
};

export type StsControl = {
	session_id: string;
	status: string;
	paused: boolean;
	strategy: string | null;
};

export type StrategyDeployBody = {
	yaml: string;
	timeout?: number;
};

export type DeployResponse = {
	id: number;
	session_id: string;
	type: string;
	config: Record<string, unknown>;
	td: { api_id: number; refcount: number }[];
	md: string[];
	status: string;
};

export type StrategyRow = {
	id: number;
	type: string;
	config: Record<string, unknown>;
	created_by: number;
	created_at: number;
	sts_session: string;
	status: string | null;
	paused: boolean | null;
};

export type Audit = {
	id: number;
	user_id: number;
	operation: string;
	result: string;
	created_at: number | null;
};

export type ApiCredential = {
	id: number;
	account_id: number;
	name: string;
	venue: string;
	api_key: string;
	type: string;
	created_at: number;
	created_by: number;
};

export type ApiCreateBody = {
	name: string;
	venue: string;
	api_key: string;
	api_secret: string;
	type?: string;
	passphrase?: string;
};

const DEFAULT_STRATEGY_YML = `td:
  - paper trader
md:
  - paper.orderbook.BTCUSDT
sts:
  type: NoopStrategy
  config:
    # BUY mid-gap/mid/mid+gap (place→cancel each), flip to SELL, then exit.
    # 100 of the quote currency (USDT here) per order; mid from the book.
    exec_interval_ms: 1000
    gap_bps: 10
    qty_quote: 100
`;

function apiBase(): string {
	// Always go through the Vite `/api` proxy so Docker (frontend → api service)
	// and local (`localhost:8000`) both work. See API_PROXY_TARGET in vite.config.ts.
	return '/api';
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
	const res = await fetch(`${apiBase()}${path}`, {
		...init,
		headers: {
			'Content-Type': 'application/json',
			...(init?.headers ?? {})
		}
	});
	if (!res.ok) {
		let detail = res.statusText;
		try {
			const body = (await res.json()) as { detail?: string };
			if (body.detail) detail = body.detail;
		} catch {
			/* ignore */
		}
		throw new Error(detail || `HTTP ${res.status}`);
	}
	return (await res.json()) as T;
}

export const api = {
	stats: () => request<{ domains: DomainStats[] }>('/stats'),
	apis: () => request<{ apis: ApiCredential[] }>('/apis'),
	createApi: (body: ApiCreateBody) =>
		request<ApiCredential>('/apis', {
			method: 'POST',
			body: JSON.stringify({
				name: body.name,
				venue: body.venue,
				api_key: body.api_key,
				api_secret: body.api_secret,
				type: body.type ?? 'HMAC',
				passphrase: body.passphrase
			})
		}),
	deleteApi: (id: number) =>
		request<{ id: number; account_id: number; deleted: boolean }>(
			`/apis/${encodeURIComponent(String(id))}`,
			{ method: 'DELETE' }
		),
	strategyTemplate: () => request<{ yaml: string }>('/sts/template'),
	strategyTypes: () => request<{ types: string[] }>('/sts/types'),
	strategies: () => request<{ strategies: StrategyRow[] }>('/sts/strategies'),
	stsSessions: (status: string | null = 'live') =>
		request<{ sessions: Session[] }>(
			`/sts/sessions${status ? `?status=${encodeURIComponent(status)}` : ''}`
		),
	deploySts: (body: StrategyDeployBody) =>
		request<DeployResponse>('/sts', {
			method: 'POST',
			body: JSON.stringify({
				yaml: body.yaml,
				timeout: body.timeout ?? 30
			})
		}),
	pauseSts: (id: string) =>
		request<StsControl>(`/sts/sessions/${encodeURIComponent(id)}/pause`, {
			method: 'POST'
		}),
	resumeSts: (id: string) =>
		request<StsControl>(`/sts/sessions/${encodeURIComponent(id)}/resume`, {
			method: 'POST'
		}),
	stopSts: (id: string) =>
		request<StsControl>(`/sts/sessions/${encodeURIComponent(id)}/stop`, {
			method: 'POST'
		}),
	tdSessions: (status: string | null = 'live') =>
		request<{ sessions: Session[] }>(
			`/td/sessions${status ? `?status=${encodeURIComponent(status)}` : ''}`
		),
	mdSessions: (status: string | null = 'live') =>
		request<{ sessions: Session[] }>(
			`/md/sessions${status ? `?status=${encodeURIComponent(status)}` : ''}`
		),
	audits: (limit = 100) =>
		request<{ audits: Audit[] }>(`/audits?limit=${limit}`)
};

export function defaultStrategyYml(): string {
	return DEFAULT_STRATEGY_YML;
}

export function shortId(id: string): string {
	return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

export function formatTs(ts: number | null | undefined): string {
	if (ts == null || ts === 0) return '—';
	return new Date(ts * 1000).toLocaleString();
}

/** Display label for a TD api: ``venue/name`` (falls back to api_id). */
export function apiLabel(opts: {
	api_id?: number | null;
	venue?: string | null;
	api_name?: string | null;
	name?: string | null;
}): string {
	const venue = opts.venue?.trim();
	const name = (opts.api_name ?? opts.name)?.trim();
	if (venue && name) return `${venue}/${name}`;
	if (name) return name;
	if (opts.api_id != null) return String(opts.api_id);
	return '—';
}
