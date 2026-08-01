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
	sts_session_id: string | null;
	strategy: string | null;
	paused: boolean | null;
};

export type StsControl = {
	session_id: string;
	status: string;
	paused: boolean;
	strategy: string | null;
};

export type DeployBody = {
	td?: number[];
	md?: string[];
	st_paras?: Record<string, unknown>;
	timeout?: number;
};

export type DeployResponse = {
	session_id: string;
	strategy: string;
	td: { api_id: number; refcount: number }[];
	md: string[];
	status: string;
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
	venue: string;
	api_key: string;
	type: string;
};

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
	strategies: () => request<{ strategies: string[] }>('/sts/strategies'),
	stsSessions: (status: string | null = 'live') =>
		request<{ sessions: Session[] }>(
			`/sts/sessions${status ? `?status=${encodeURIComponent(status)}` : ''}`
		),
	deploySts: (strategy: string, body: DeployBody) =>
		request<DeployResponse>(`/sts/${encodeURIComponent(strategy)}`, {
			method: 'POST',
			body: JSON.stringify({
				td: body.td ?? [],
				md: body.md ?? [],
				st_paras: body.st_paras ?? {},
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

export function shortId(id: string): string {
	return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

export function formatTs(ts: number | null | undefined): string {
	if (ts == null || ts === 0) return '—';
	return new Date(ts * 1000).toLocaleString();
}
