import { wsBaseUrl } from '$lib/ws';

/**
 * Live executions over `/ws/board`, already attributed to a session.
 *
 * The counts the board loads over REST come from the database, which is
 * minutes behind a fill by design — the writer batches, and the settlement
 * line sits further back still. This is what makes a *live* run readable
 * without either of those being made to hurry.
 *
 * Each event is one execution, not a total. A consumer increments; it must not
 * treat a missed event as a gap it can detect, because it cannot. Reloading
 * the REST snapshot is what re-establishes truth, and the server's own counts
 * are the ones to believe whenever the two disagree.
 */

export type FillEvent = {
	type: 'fill';
	session_id: string;
	api_id: number;
	universal_ticker: string | null;
	side: string | null;
	qty: string | null;
	price: string | null;
	ts: number;
};

export type FillConnection = 'connecting' | 'open' | 'closed' | 'error';

/**
 * Subscribe to live executions. Returns a disposer.
 *
 * Reconnects with backoff, for the same reason the status stream does: this is
 * a page someone leaves open, and a socket that dies quietly is worse than no
 * socket — it looks live while showing frozen numbers.
 */
export function connectFills(
	onFill: (event: FillEvent) => void,
	onConnection?: (state: FillConnection) => void
): () => void {
	let ws: WebSocket | null = null;
	let retry: ReturnType<typeof setTimeout> | null = null;
	let attempt = 0;
	let closed = false;

	function open() {
		if (closed) return;
		onConnection?.('connecting');
		ws = new WebSocket(`${wsBaseUrl()}/ws/board`);

		ws.onopen = () => {
			attempt = 0;
			onConnection?.('open');
		};
		ws.onerror = () => onConnection?.('error');
		ws.onclose = () => {
			ws = null;
			if (closed) {
				onConnection?.('closed');
				return;
			}
			onConnection?.('closed');
			const delay = Math.min(30_000, 500 * 2 ** attempt++);
			retry = setTimeout(open, delay);
		};
		ws.onmessage = (message) => {
			let event: FillEvent;
			try {
				event = JSON.parse(message.data as string) as FillEvent;
			} catch {
				return;
			}
			if (event?.type !== 'fill' || !event.session_id) return;
			onFill(event);
		};
	}

	open();

	return () => {
		closed = true;
		if (retry) clearTimeout(retry);
		ws?.close();
		ws = null;
	};
}
