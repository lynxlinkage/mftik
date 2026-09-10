import { shouldReopen, wsBaseUrl } from '$lib/ws';

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

export type LogConnection = 'connecting' | 'open' | 'closed' | 'error';

export type LogConnectionDetail = {
	code?: number;
	reason?: string;
	/**
	 * Absent while a reconnect is still coming; false once the stream has given
	 * up, which only happens for a login no retry can repair.
	 */
	willRetry?: boolean;
};

/**
 * Subscribe to a domain log stream. Returns a disposer.
 *
 * Reconnects with backoff, same reason as the status and board sockets: a
 * first-paint close that never comes back looks like an empty log even when
 * REST already has rows. `shouldReopen` is what keeps that from becoming a
 * loop against a session that will never be accepted again.
 */
export function connectDomainLog(
	domain: LogDomain,
	id: string,
	onMessage: (entry: LogEntry) => void,
	onStatus: (status: LogConnection, detail?: LogConnectionDetail) => void
): () => void {
	const url = `${wsBaseUrl()}/ws/${domain}/${encodeURIComponent(id)}`;
	let ws: WebSocket | null = null;
	let retry: ReturnType<typeof setTimeout> | null = null;
	let attempt = 0;
	let disposed = false;

	function open() {
		if (disposed) return;
		onStatus('connecting');
		ws = new WebSocket(url);

		ws.onopen = () => {
			attempt = 0;
			onStatus('open');
		};
		ws.onerror = () => onStatus('error');
		ws.onclose = (ev) => {
			const detail = { code: ev.code, reason: ev.reason };
			onStatus('closed', detail);
			if (disposed) return;
			void (async () => {
				// An expired session closes the handshake with no status the
				// browser will show us, so a dead login is indistinguishable
				// here from a finished stream. $lib/ws asks over REST instead.
				if (!(await shouldReopen(ev.code))) {
					if (!disposed) onStatus('closed', { ...detail, willRetry: false });
					return;
				}
				if (disposed) return;
				// 1s, 2s, 4s … capped at 30s.
				const delay = Math.min(1000 * 2 ** attempt, 30_000);
				attempt += 1;
				retry = setTimeout(open, delay);
			})();
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
	}

	open();

	return () => {
		disposed = true;
		if (retry !== null) clearTimeout(retry);
		ws?.close();
	};
}

/** @deprecated Use connectDomainLog('sts', sessionId, ...) */
export function connectSessionLog(
	sessionId: string,
	onMessage: (entry: LogEntry) => void,
	onStatus: (status: LogConnection, detail?: LogConnectionDetail) => void
): () => void {
	return connectDomainLog('sts', sessionId, onMessage, onStatus);
}

export function newSessionId(): string {
	return crypto.randomUUID().replace(/-/g, '').slice(0, 12);
}
