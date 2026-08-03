export type LogEntry = {
	id: string;
	ts: number;
	source: string;
	level: string;
	message: string;
	raw: string;
	/** Postgres row id when loaded from REST history; absent for live WS lines. */
	dbId?: number;
};

export type SessionLogMessage = {
	id?: string;
	type?: string;
	source?: string;
	session_id?: string;
	ts?: number;
	payload?: {
		level?: string;
		message?: string;
		[key: string]: unknown;
	};
};

export type LogDomain = 'sts' | 'td' | 'md';

function wsBaseUrl(): string {
	const api = import.meta.env.PUBLIC_API_URL as string | undefined;
	if (api) {
		const u = new URL(api);
		u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:';
		return u.origin;
	}
	const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
	return `${proto}//${window.location.hostname}:8000`;
}

export function connectDomainLog(
	domain: LogDomain,
	id: string,
	onMessage: (entry: LogEntry) => void,
	onStatus: (status: 'connecting' | 'open' | 'closed' | 'error') => void
): () => void {
	const url = `${wsBaseUrl()}/ws/${domain}/${encodeURIComponent(id)}`;
	onStatus('connecting');
	const ws = new WebSocket(url);

	ws.onopen = () => onStatus('open');
	ws.onerror = () => onStatus('error');
	ws.onclose = () => onStatus('closed');
	ws.onmessage = (ev) => {
		const raw = String(ev.data);
		try {
			const msg = JSON.parse(raw) as SessionLogMessage;
			onMessage({
				id: msg.id ?? crypto.randomUUID(),
				ts: msg.ts ?? Date.now() / 1000,
				source: msg.source ?? 'unknown',
				level: msg.payload?.level ?? 'info',
				message: msg.payload?.message ?? raw,
				raw
			});
		} catch {
			onMessage({
				id: crypto.randomUUID(),
				ts: Date.now() / 1000,
				source: 'raw',
				level: 'info',
				message: raw,
				raw
			});
		}
	};

	return () => {
		ws.close();
	};
}

/** @deprecated Use connectDomainLog('sts', sessionId, ...) */
export function connectSessionLog(
	sessionId: string,
	onMessage: (entry: LogEntry) => void,
	onStatus: (status: 'connecting' | 'open' | 'closed' | 'error') => void
): () => void {
	return connectDomainLog('sts', sessionId, onMessage, onStatus);
}

export function newSessionId(): string {
	return crypto.randomUUID().replace(/-/g, '').slice(0, 12);
}
