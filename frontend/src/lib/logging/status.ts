import { reloadIfSessionExpired } from '$lib/auth';
import { wsBaseUrl } from '$lib/ws';

/**
 * Live STS session state over `/ws/status/sts`.
 *
 * Every message is a full snapshot of one session, so a consumer applies the
 * newest one per `session_id` and is correct regardless of what it missed.
 * The server replays a buffer on connect, which means replayed and live
 * events overlap — hence the `ts` comparison rather than blind assignment.
 */

export type StsSessionStatusEvent = {
	session_id: string;
	/** live | done | failed */
	status: string;
	paused: boolean;
	strategy: string | null;
	reason: string | null;
	created_by: number | null;
	finished_at: number | null;
	/** Envelope timestamp, used to drop events older than what we hold. */
	ts: number;
};

type StatusEnvelope = {
	type?: string;
	ts?: number;
	payload?: Partial<StsSessionStatusEvent>;
};

export type StatusConnection = 'connecting' | 'open' | 'closed' | 'error';

/**
 * Subscribe to STS session state. Returns a disposer.
 *
 * Reconnects with backoff: the page holding this open is a dashboard someone
 * leaves running, and a dropped socket that never comes back is worse than no
 * socket at all — it looks live while showing frozen state.
 */
export function connectStsStatus(
	onEvent: (event: StsSessionStatusEvent) => void,
	onConnection?: (state: StatusConnection) => void
): () => void {
	let ws: WebSocket | null = null;
	let retry: ReturnType<typeof setTimeout> | null = null;
	let attempt = 0;
	let closed = false;

	function open() {
		if (closed) return;
		onConnection?.('connecting');
		ws = new WebSocket(`${wsBaseUrl()}/ws/status/sts`);

		ws.onopen = () => {
			attempt = 0;
			onConnection?.('open');
		};
		ws.onerror = () => onConnection?.('error');
		ws.onclose = () => {
			onConnection?.('closed');
			if (closed) return;
			// This dashboard is the one people leave open for hours, so it is
			// the one that outlives its login session. A handshake the auth
			// chain rejected looks exactly like the API going away, and the
			// backoff below would retry a dead session forever behind a UI
			// that still claims to be connecting.
			void reloadIfSessionExpired();
			// 1s, 2s, 4s … capped at 30s.
			const delay = Math.min(1000 * 2 ** attempt, 30_000);
			attempt += 1;
			retry = setTimeout(open, delay);
		};
		ws.onmessage = (ev) => {
			let msg: StatusEnvelope;
			try {
				msg = JSON.parse(String(ev.data)) as StatusEnvelope;
			} catch {
				return;
			}
			const p = msg.payload;
			if (!p?.session_id || !p.status) return;
			onEvent({
				session_id: p.session_id,
				status: p.status,
				paused: p.paused ?? false,
				strategy: p.strategy ?? null,
				reason: p.reason ?? null,
				created_by: p.created_by ?? null,
				finished_at: p.finished_at ?? null,
				ts: msg.ts ?? 0
			});
		};
	}

	open();

	return () => {
		closed = true;
		if (retry !== null) clearTimeout(retry);
		ws?.close();
	};
}
