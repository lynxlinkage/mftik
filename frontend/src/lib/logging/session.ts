import { reloadIfSessionExpired } from '$lib/auth';
import { wsBaseUrl } from '$lib/ws';

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

export function connectDomainLog(
	domain: LogDomain,
	id: string,
	onMessage: (entry: LogEntry) => void,
	onStatus: (status: 'connecting' | 'open' | 'closed' | 'error') => void
): () => void {
	const url = `${wsBaseUrl()}/ws/${domain}/${encodeURIComponent(id)}`;
	onStatus('connecting');
	const ws = new WebSocket(url);
	// Distinguishes our own teardown from the socket dropping under us; only
	// the latter is worth asking the auth chain about.
	let disposed = false;

	ws.onopen = () => onStatus('open');
	ws.onerror = () => onStatus('error');
	ws.onclose = () => {
		onStatus('closed');
		// An expired session closes the handshake with no status the browser
		// will show us, so a dead login is indistinguishable here from a
		// finished stream. $lib/auth asks the question over REST instead.
		if (!disposed) void reloadIfSessionExpired();
	};
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
		disposed = true;
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
