export type DomainStats = {
	domain: string;
	live: number;
	done: number;
	/** Sessions that ended badly. Always 0 outside `sts`. */
	failed: number;
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
	/** Why a `failed` session ended. Null for live sessions and clean exits. */
	reason: string | null;
};

export type StsControl = {
	session_id: string;
	status: string;
	paused: boolean;
	strategy: string | null;
	reason: string | null;
};

export type StrategyDeployBody = {
	/** Strategy class to run. The document no longer carries it. */
	type: string;
	yaml: string;
	timeout?: number;
};

/** A deployable strategy and the document it starts from. */
export type StrategyTemplate = {
	type: string;
	label: string;
	description: string;
	yaml: string;
};

export type StrategyTypes = {
	types: string[];
	templates: StrategyTemplate[];
	default: string;
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
	/** Why a `failed` session ended. Null for live sessions and clean exits. */
	reason: string | null;
};

/** A past deploy rebuilt as strategy.yml (`GET /sts/strategies/{id}/yaml`). */
export type StrategyYaml = {
	id: number;
	/** Strategy class this was deployed as. */
	type: string;
	sts_session: string;
	yaml: string;
	/** api ids whose account name could not be recovered — their `td` entries
	 * are placeholders that will not redeploy. */
	unresolved_td: number[];
};

export type Audit = {
	id: number;
	user_id: number;
	operation: string;
	result: string;
	created_at: number | null;
};

export type SessionLog = {
	id: string;
	db_id: number;
	ts: number;
	source: string;
	level: string;
	message: string;
};

export type SessionLogList = {
	logs: SessionLog[];
	has_more: boolean;
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

/** A venue a credential can be registered against (`GET /venues`). */
export type Venue = {
	name: string;
	label: string;
	api_types: string[];
	requires_passphrase: boolean;
	simulated: boolean;
	symbol_example: string;
};

/** What the symbol plane actually tracks (`GET /sym/venues`). */
export type SymVenues = {
	venues: string[];
	counts: Record<string, number>;
};

export type SymbolFilter = {
	name: string;
	/** Decimal on the wire: a string, so exact scale survives the round trip. */
	value: string | null;
};

export type SymbolInfo = {
	venue: string;
	symbol: string;
	category: string;
	base: string;
	quote: string;
	exch_ticker: string;
	is_active: boolean;
	filters: SymbolFilter[];
	updated_at: number | null;
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
	venues: () => request<{ venues: Venue[] }>('/venues'),
	symVenues: () => request<SymVenues>('/sym/venues'),
	symbols: (opts: { venue?: string; activeOnly?: boolean } = {}) => {
		const q = new URLSearchParams();
		if (opts.venue) q.set('venue', opts.venue);
		if (opts.activeOnly === false) q.set('active_only', 'false');
		const qs = q.toString();
		return request<{ symbols: SymbolInfo[] }>(`/sym/symbols${qs ? `?${qs}` : ''}`);
	},
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
	strategyTypes: () => request<StrategyTypes>('/sts/types'),
	strategyTypeTemplate: (type: string) =>
		request<StrategyTemplate>(`/sts/types/${encodeURIComponent(type)}/template`),
	strategies: () => request<{ strategies: StrategyRow[] }>('/sts/strategies'),
	strategyYaml: (id: number) =>
		request<StrategyYaml>(`/sts/strategies/${encodeURIComponent(String(id))}/yaml`),
	stsSessions: (status: string | null = 'live') =>
		request<{ sessions: Session[] }>(
			`/sts/sessions${status ? `?status=${encodeURIComponent(status)}` : ''}`
		),
	deploySts: (body: StrategyDeployBody) =>
		request<DeployResponse>(`/sts/deploy/${encodeURIComponent(body.type)}`, {
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
		request<{ audits: Audit[] }>(`/audits?limit=${limit}`),
	logs: (
		domain: 'sts' | 'td' | 'md',
		streamId: string,
		opts: { beforeTs?: number; beforeId?: number; limit?: number } = {}
	) => {
		const q = new URLSearchParams();
		if (opts.beforeTs != null) q.set('before_ts', String(opts.beforeTs));
		if (opts.beforeId != null) q.set('before_id', String(opts.beforeId));
		if (opts.limit != null) q.set('limit', String(opts.limit));
		const qs = q.toString();
		return request<SessionLogList>(
			`/logs/${encodeURIComponent(domain)}/${encodeURIComponent(streamId)}${qs ? `?${qs}` : ''}`
		);
	}
};

export function defaultStrategyYml(): string {
	return DEFAULT_STRATEGY_YML;
}

export function shortId(id: string): string {
	return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

/** Trim a wire decimal for display: ``0.000100000000000000`` → ``0.0001``.
 *
 * Done on the string rather than via ``Number`` so an 8-decimal tick size does
 * not come back as ``1e-8``, and so nothing is rounded on the way through.
 */
export function formatDecimal(value: string | number | null | undefined): string | null {
	if (value == null) return null;
	const s = String(value).trim();
	// Anything not plain fixed-point (exponent form, junk) is shown as-is.
	if (!/^-?\d+\.\d+$/.test(s)) return s || null;
	const trimmed = s.replace(/0+$/, '').replace(/\.$/, '');
	return trimmed === '' || trimmed === '-' ? '0' : trimmed;
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
