import { handleUnauthorized } from '$lib/auth';

/**
 * One strategy run, as the board shows it.
 *
 * Counts and times, and deliberately no PnL: deriving a result means matching
 * executions into positions and valuing whatever is left open, and a number
 * shown before that exists would be believed.
 *
 * `fills` is what has been recorded. `settled` says whether the venue has been
 * re-read across the whole run and agreed — a live run never is, and neither
 * is a finished one whose last minutes are still inside the safety lag.
 */
export type BoardSession = {
	session_id: string;
	strategy: string | null;
	status: string;
	reason: string | null;
	created_at: number;
	finished_at: number | null;
	duration_s: number;
	running: boolean;
	/** Executions recorded. The only count here — see the board route. */
	fills: number;
	td_api_ids: number[];
	confirmed_through_ts: number | null;
	settled: boolean;
};

/**
 * One execution, as the record holds it.
 *
 * Decimals arrive as strings and stay that way. Every one is money or size,
 * and parsing to a JS number is exactly the silent rounding the NUMERIC(38,18)
 * columns exist to avoid — display them, do not compute with them.
 */
export type BoardFill = {
	id: number;
	fill_id: string;
	universal_ticker: string;
	side: string;
	price: string;
	qty: string;
	fee: string;
	fee_asset: string;
	client_order_id: string | null;
	venue_order_id: string | null;
	api_id: number;
	ts: number;
	/** `stream` — caught live. `backfill` — re-read from the venue. */
	source: string;
	settled: boolean;
};

export type BoardFillList = {
	fills: BoardFill[];
	has_more: boolean;
};

export type DomainStats = {
	domain: string;
	live: number;
	done: number;
	/** Sessions that ended badly. Always 0 outside `sts`. */
	failed: number;
	/** Sessions STS cut short when it went down. Always 0 outside `sts`. */
	interrupted: number;
	/** Failed/interrupted sessions an operator has acknowledged. Always 0 outside `sts`. */
	ack: number;
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

/** One strategy tree in this node's registry (public, private, or pulled). */
export type RegistryStrategy = {
	name: string;
	type: string;
	digest: string;
	requires_mft: string;
	origin: string;
	files: string[];
};

export type RegistryRemote = {
	name: string;
	url: string;
	count: number;
	/** Whether we hold a registry key for this peer. Never the key itself. */
	authenticated: boolean;
};

export type RegistryRemoteDetail = RegistryRemote & {
	strategies: RegistryStrategy[];
};

export type RegistrySyncStatus =
	| 'synced'
	| 'diverged'
	| 'remote_only'
	| 'local_only'
	| 'unknown';

export type RegistrySyncRow = {
	name: string;
	type: string;
	local_digest: string | null;
	remote_digest: string | null;
	status: RegistrySyncStatus;
};

export type RegistryDiff = RegistryRemote & {
	reachable: boolean;
	error: string | null;
	strategies: RegistrySyncRow[];
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

/** The strategy.yml behind a past deploy (`GET /sts/strategies/{id}/yaml`). */
export type StrategyYaml = {
	id: number;
	/** Strategy class this was deployed as. */
	type: string;
	sts_session: string;
	yaml: string;
	/** api ids whose account name could not be recovered — their `td` entries
	 * are placeholders that will not redeploy. Reconstructed documents only. */
	unresolved_td: number[];
	/** False when `yaml` is the submitted document; true when it was rebuilt
	 * from the stored spec because that deploy predates the text being kept. */
	reconstructed: boolean;
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

/** What STS holds for one session's event log (`GET /sts/sessions/{id}/eventlog/info`). */
export type EventLogInfo = {
	session_id: string;
	/** Whether there is anything to download. */
	available: boolean;
	/** Whether this deployment keeps event logs at all. */
	enabled: boolean;
	parts: number;
	total_bytes: number;
	/** Session still running, so a download is a prefix of the log. */
	live: boolean;
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
	/** Markets this venue trades. One entry is a classic account; several is a
	 * unified one, where the category is part of every instrument's identity. */
	categories: string[];
	requires_passphrase: boolean;
	simulated: boolean;
	ticker_example: string;
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
	/** `<Venue>_<Category>_<SYMBOL>` — the platform's instrument identity. */
	universal_ticker: string;
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
  - orderbook.Paper_Spot_BTCUSDT
sts:
  # BUY mid-gap/mid/mid+gap (place→cancel each), flip to SELL, then exit.
  # 100 of the quote currency (USDT here) per order; mid from the book.
  exec_interval_ms: 1000
  gap_bps: 10
  qty_quote: 100
`;

/**
 * What the login page needs before anyone has proved anything.
 *
 * `setup_required` is true while the Owner has no password — either no user
 * row at all, or the passwordless one `seed` creates so foreign keys resolve.
 * Both mean the same thing to whoever is looking at the form: claim this
 * instance rather than sign in to it.
 */
export type AuthStatus = {
	/**
	 * Whether the gate is on at all. Off, every request is already the Owner
	 * and there is nothing to sign in or out of — the UI hides both rather
	 * than offering a no-op.
	 */
	enabled: boolean;
	setup_required: boolean;
	providers: string[];
	authenticated: boolean;
	username: string | null;
	/** The server's rule, not a copy of it. Hard-coding it here is how the
	 *  form and the endpoint that enforces it drift apart. */
	min_password_length: number;
};

/** The Owner, and which proof this request arrived with. */
export type Me = {
	user_id: number;
	username: string | null;
	display_name: string;
	email: string | null;
	via: string;
};

/**
 * A machine credential, as the settings page can know it.
 *
 * `prefix` is `mft_ak_abc12345…` — everything of the token that survives
 * minting. The secret exists once, in the response that creates it, and is a
 * hash in the database from then on, so no list can ever show it again.
 */
export type AuthKey = {
	id: number;
	name: string;
	kind: string;
	prefix: string;
	scopes: string[];
	created_at: number;
	last_used_at: number | null;
	revoked_at: number | null;
};

/** The mint response. The only shape that carries `token`. */
export type AuthKeyCreated = AuthKey & { token: string };

/**
 * One way of proving you are the Owner.
 *
 * The password has no `id` and is not removable: it is a column on the user,
 * not a row, which is precisely what makes it impossible to unlink yourself
 * out of your own instance. It is listed anyway so the UI shows all the ways
 * in as one set.
 */
export type Identity = {
	id: number | null;
	provider: string;
	label: string | null;
	email: string | null;
	linked_at: number | null;
	removable: boolean;
};

function apiBase(): string {
	// Always go through the Vite `/api` proxy so Docker (frontend → api service)
	// and local (`localhost:8000`) both work. See API_PROXY_TARGET in vite.config.ts.
	return '/api';
}

async function detailOf(res: Response): Promise<string> {
	let detail = res.statusText;
	try {
		const body = (await res.json()) as { detail?: string };
		if (body.detail) detail = body.detail;
	} catch {
		/* ignore */
	}
	return detail || `HTTP ${res.status}`;
}

/**
 * `signIn: true` marks the endpoints that *are* the login. A 401 from those is
 * the answer — wrong password — not a sign the session lapsed, and routing it
 * to the login page would send someone standing on that page back to it.
 */
async function request<T>(
	path: string,
	init?: RequestInit,
	opts?: { signIn?: boolean }
): Promise<T> {
	const res = await fetch(`${apiBase()}${path}`, {
		...init,
		headers: {
			'Content-Type': 'application/json',
			...(init?.headers ?? {})
		}
	});
	// An expired login is answered by whichever gate is in front — see
	// $lib/auth for how they differ. The throw still happens so no caller
	// treats this as data: the page is on its way out, but nothing may proceed
	// in the meantime.
	if (res.status === 401 && !opts?.signIn && handleUnauthorized(res)) {
		throw new Error('Login session expired — signing in again…');
	}
	if (!res.ok) {
		throw new Error(await detailOf(res));
	}
	return (await res.json()) as T;
}

function filenameFromDisposition(header: string | null, fallback: string): string {
	if (!header) return fallback;
	const star = /filename\*=(?:UTF-8''|)([^;]+)/i.exec(header);
	if (star?.[1]) {
		try {
			return decodeURIComponent(star[1].trim().replace(/^"|"$/g, ''));
		} catch {
			/* fall through */
		}
	}
	const quoted = /filename="([^"]+)"/i.exec(header);
	if (quoted?.[1]) return quoted[1];
	const plain = /filename=([^;]+)/i.exec(header);
	if (plain?.[1]) return plain[1].trim();
	return fallback;
}

async function downloadFile(path: string, fallbackName: string): Promise<void> {
	const res = await fetch(`${apiBase()}${path}`);
	if (res.status === 401 && handleUnauthorized(res)) {
		throw new Error('Login session expired — signing in again…');
	}
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
	const blob = await res.blob();
	const name = filenameFromDisposition(res.headers.get('Content-Disposition'), fallbackName);
	const url = URL.createObjectURL(blob);
	try {
		const a = document.createElement('a');
		a.href = url;
		a.download = name;
		a.rel = 'noopener';
		document.body.appendChild(a);
		a.click();
		a.remove();
	} finally {
		URL.revokeObjectURL(url);
	}
}

export const api = {
	/* -- auth ------------------------------------------------------------- */
	authStatus: () => request<AuthStatus>('/auth/status', undefined, { signIn: true }),
	authSetup: (username: string, password: string) =>
		request<Me>(
			'/auth/setup',
			{ method: 'POST', body: JSON.stringify({ username, password }) },
			{ signIn: true }
		),
	authLogin: (username: string, password: string) =>
		request<Me>(
			'/auth/login/password',
			{ method: 'POST', body: JSON.stringify({ username, password }) },
			{ signIn: true }
		),
	authMe: () => request<Me>('/auth/me'),
	authLogout: () => request<{ status: string }>('/auth/logout', { method: 'POST' }),
	authIdentities: () => request<{ identities: Identity[] }>('/auth/identities'),
	authIdentityUnlink: (id: number) =>
		request<Identity>(`/auth/identities/${encodeURIComponent(String(id))}`, {
			method: 'DELETE'
		}),
	authKeys: () => request<{ keys: AuthKey[] }>('/auth/keys'),
	authKeyCreate: (name: string, kind: 'api' | 'registry' = 'api') =>
		request<AuthKeyCreated>('/auth/keys', {
			method: 'POST',
			body: JSON.stringify({ name, kind })
		}),
	authKeyRevoke: (id: number) =>
		request<AuthKey>(`/auth/keys/${encodeURIComponent(String(id))}`, { method: 'DELETE' }),
	stats: () => request<{ domains: DomainStats[] }>('/stats'),
	boardSessions: (opts: { status?: string; limit?: number } = {}) => {
		const q = new URLSearchParams();
		if (opts.status) q.set('status', opts.status);
		if (opts.limit != null) q.set('limit', String(opts.limit));
		const qs = q.toString();
		return request<{ sessions: BoardSession[] }>(`/board/sessions${qs ? `?${qs}` : ''}`);
	},
	boardSession: (sessionId: string) =>
		request<BoardSession>(`/board/sessions/${encodeURIComponent(sessionId)}`),
	boardFills: (
		sessionId: string,
		opts: { beforeTs?: number; beforeId?: number; limit?: number } = {}
	) => {
		const q = new URLSearchParams();
		if (opts.beforeTs != null) q.set('before_ts', String(opts.beforeTs));
		if (opts.beforeId != null) q.set('before_id', String(opts.beforeId));
		if (opts.limit != null) q.set('limit', String(opts.limit));
		const qs = q.toString();
		return request<BoardFillList>(
			`/board/sessions/${encodeURIComponent(sessionId)}/fills${qs ? `?${qs}` : ''}`
		);
	},
	/**
	 * Executions no session of ours placed — trading done outside the platform,
	 * and our own fills whose order never reached the record. Not keyed by a
	 * session, because that is exactly what these rows are missing.
	 */
	boardExternalFills: (
		opts: { beforeTs?: number; beforeId?: number; limit?: number } = {}
	) => {
		const q = new URLSearchParams();
		if (opts.beforeTs != null) q.set('before_ts', String(opts.beforeTs));
		if (opts.beforeId != null) q.set('before_id', String(opts.beforeId));
		if (opts.limit != null) q.set('limit', String(opts.limit));
		const qs = q.toString();
		return request<BoardFillList>(`/board/fills/external${qs ? `?${qs}` : ''}`);
	},
	venues: () => request<{ venues: Venue[] }>('/venues'),
	symVenues: () => request<SymVenues>('/sym/venues'),
	symbols: (
		opts: {
			venue?: string;
			activeOnly?: boolean;
			universalTicker?: string;
			q?: string;
			limit?: number;
			offset?: number;
			slim?: boolean;
		} = {}
	) => {
		const q = new URLSearchParams();
		if (opts.venue) q.set('venue', opts.venue);
		if (opts.activeOnly === false) q.set('active_only', 'false');
		if (opts.universalTicker) q.set('universal_ticker', opts.universalTicker);
		if (opts.q) q.set('q', opts.q);
		if (opts.limit != null) q.set('limit', String(opts.limit));
		if (opts.offset != null && opts.offset > 0) q.set('offset', String(opts.offset));
		if (opts.slim) q.set('slim', 'true');
		const qs = q.toString();
		return request<{ symbols: SymbolInfo[]; total: number }>(
			`/sym/symbols${qs ? `?${qs}` : ''}`
		);
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
	renameApi: (id: number, name: string) =>
		request<ApiCredential>(`/apis/${encodeURIComponent(String(id))}`, {
			method: 'PATCH',
			body: JSON.stringify({ name })
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
	ackSts: (id: string) =>
		request<StsControl>(`/sts/sessions/${encodeURIComponent(id)}/ack`, {
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
	},
	downloadBoardFillsCsv: (sessionId: string) =>
		downloadFile(
			`/board/sessions/${encodeURIComponent(sessionId)}/fills.csv`,
			`${sessionId}_historical_fills.csv`
		),
	eventLogInfo: (sessionId: string) =>
		request<EventLogInfo>(`/sts/sessions/${encodeURIComponent(sessionId)}/eventlog/info`),
	downloadEventLog: (sessionId: string) =>
		downloadFile(
			`/sts/sessions/${encodeURIComponent(sessionId)}/eventlog`,
			`${sessionId}.jsonl.gz`
		),
	downloadLogs: (
		domain: 'sts' | 'td' | 'md',
		streamId: string,
		from: string,
		to: string
	) => {
		const q = new URLSearchParams({ from, to });
		const fallback =
			from === to
				? `${streamId}_${domain}_${from}.log`
				: `${streamId}_${domain}_${from}_${to}.tar.gz`;
		return downloadFile(
			`/logs/${encodeURIComponent(domain)}/${encodeURIComponent(streamId)}/download?${q}`,
			fallback
		);
	},
	registryStrategies: () =>
		request<{ strategies: RegistryStrategy[] }>('/registry/v1/strategies'),
	registryPrivate: () =>
		request<{ strategies: RegistryStrategy[] }>('/registry/v1/private'),
	registryRemotes: () => request<{ remotes: RegistryRemote[] }>('/registry/v1/remotes'),
	registryRemote: (name: string) =>
		request<RegistryRemoteDetail>(`/registry/v1/remotes/${encodeURIComponent(name)}`),
	registryDiff: (name: string) =>
		request<RegistryDiff>(`/registry/v1/remotes/${encodeURIComponent(name)}/diff`),
	connectRegistry: (body: { name: string; url: string; token?: string }) =>
		request<{ name: string; url: string; pulled: RegistryStrategy[] }>(
			'/registry/v1/remotes',
			{ method: 'POST', body: JSON.stringify(body) }
		),
	disconnectRegistry: (name: string) =>
		request<RegistryRemote>(`/registry/v1/remotes/${encodeURIComponent(name)}`, {
			method: 'DELETE'
		})
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

/* ---------------------------------------------------------------- auth --- */


/**
 * Start an OAuth flow.
 *
 * A navigation, not a fetch: the browser has to leave for the provider and
 * come back, and the callback answers with a redirect and a Set-Cookie. An
 * XHR would follow all of that invisibly and land the result nowhere.
 */
export function startOAuth(provider: string, mode: 'login' | 'connect'): void {
	window.location.href = `${apiBase()}/auth/${mode}/${encodeURIComponent(provider)}`;
}
