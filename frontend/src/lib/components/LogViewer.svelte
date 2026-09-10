<script lang="ts">
	import { tick } from 'svelte';
	import { api, type SessionLog } from '$lib/api';
	import {
		connectDomainLog,
		type LogConnection,
		type LogDomain,
		type LogEntry
	} from '$lib/logging/session';
	import LogDownloadModal from '$lib/components/LogDownloadModal.svelte';

	interface Props {
		domain: LogDomain;
		streamId: string;
		title?: string;
		subtitle?: string;
	}

	let { domain, streamId, title = 'Session log', subtitle }: Props = $props();

	let status = $state<LogConnection>('connecting');
	let logs = $state<LogEntry[]>([]);
	let copied = $state(false);
	let preEl = $state<HTMLPreElement | null>(null);
	let stickToBottom = true;
	let loadingOlder = $state(false);
	let hasMoreHistory = $state(true);
	let historyLoaded = $state(false);
	let historyHint = $state('');
	let downloadOpen = $state(false);
	let closeCode = $state<number | undefined>(undefined);
	let closeReason = $state('');
	let sawDisconnect = $state(false);

	const HISTORY_PAGE = 100;
	const SCROLL_TOP_THRESHOLD = 40;
	const LIVE_CAP = 500;

	function fromRestRow(row: SessionLog): LogEntry {
		return {
			id: row.id,
			ts: row.ts,
			source: row.source,
			level: row.level,
			message: row.message,
			raw: '',
			dbId: row.db_id
		};
	}

	function logOrder(a: LogEntry, b: LogEntry): number {
		if (a.ts !== b.ts) return a.ts - b.ts;
		const aDb = a.dbId ?? 0;
		const bDb = b.dbId ?? 0;
		if (aDb !== bDb) return aDb - bDb;
		return a.id.localeCompare(b.id);
	}

	function mergeLogs(existing: LogEntry[], incoming: LogEntry[]): LogEntry[] {
		const seen = new Set(existing.map((e) => e.id));
		const extra = incoming.filter((e) => !seen.has(e.id));
		if (extra.length === 0) return existing;
		return [...existing, ...extra].sort(logOrder);
	}

	function formatTime(ts: number): string {
		return new Date(ts * 1000).toLocaleTimeString('en-GB', {
			hour12: false,
			hour: '2-digit',
			minute: '2-digit',
			second: '2-digit',
			fractionalSecondDigits: 3
		});
	}

	function formatLine(entry: LogEntry): string {
		const t = formatTime(entry.ts);
		const level = entry.level.toUpperCase().padEnd(5, ' ');
		const source = entry.source.padEnd(16, ' ');
		return `${t}  ${level}  ${source}  ${entry.message}`;
	}

	const text = $derived(logs.length === 0 ? '' : logs.map(formatLine).join('\n') + '\n');

	function connectionHint(): string {
		if (closeCode === 1008) return 'authentication required';
		if (closeReason) return closeReason;
		if (closeCode != null && closeCode !== 1005) return `closed (${closeCode})`;
		return 'disconnected';
	}

	const emptyMessage = $derived.by(() => {
		if (status === 'error') return 'Log stream error. Reconnecting…';
		if (status === 'closed') {
			return `Disconnected — ${connectionHint()}. Reconnecting…`;
		}
		if (sawDisconnect && status === 'connecting') return 'Reconnecting…';
		return 'Waiting for log lines…';
	});

	async function scrollIfNeeded() {
		await tick();
		if (stickToBottom && preEl) {
			preEl.scrollTop = preEl.scrollHeight;
		}
	}

	async function seedRecent(d: LogDomain, id: string, isCancelled: () => boolean) {
		loadingOlder = true;
		try {
			const page = await api.logs(d, id, { limit: HISTORY_PAGE });
			if (isCancelled()) return;
			const seeded = page.logs.slice().reverse().map(fromRestRow);
			logs = mergeLogs(logs, seeded);
			hasMoreHistory = page.has_more;
			if (seeded.length > 0) historyLoaded = true;
			void scrollIfNeeded();
		} catch {
			if (isCancelled()) return;
			historyHint = 'Failed to load log history';
		} finally {
			if (!isCancelled()) loadingOlder = false;
		}
	}

	async function loadOlder() {
		if (loadingOlder || !hasMoreHistory) return;
		loadingOlder = true;
		historyHint = 'Loading older…';
		const oldest = logs[0];
		const prevHeight = preEl?.scrollHeight ?? 0;
		const prevTop = preEl?.scrollTop ?? 0;
		try {
			const page = await api.logs(domain, streamId, {
				...(oldest ? { beforeTs: oldest.ts, beforeId: oldest.dbId } : {}),
				limit: HISTORY_PAGE
			});
			const seen = new Set(logs.map((e) => e.id));
			// API returns newest-first; reverse so oldest is first when prepending.
			const older = page.logs
				.slice()
				.reverse()
				.filter((row) => !seen.has(row.id))
				.map(fromRestRow);
			hasMoreHistory = page.has_more;
			if (older.length === 0) {
				historyHint = hasMoreHistory ? '' : 'Beginning of history';
				return;
			}
			historyLoaded = true;
			logs = [...older, ...logs];
			historyHint = hasMoreHistory ? '' : 'Beginning of history';
			await tick();
			if (preEl) {
				preEl.scrollTop = prevTop + (preEl.scrollHeight - prevHeight);
			}
		} catch {
			historyHint = 'Failed to load older logs';
		} finally {
			loadingOlder = false;
		}
	}

	function onScroll() {
		if (!preEl) return;
		const gap = preEl.scrollHeight - preEl.scrollTop - preEl.clientHeight;
		stickToBottom = gap < 40;
		if (preEl.scrollTop < SCROLL_TOP_THRESHOLD) {
			void loadOlder();
		}
	}

	async function copyAll() {
		const body = text || '(no log lines yet)\n';
		try {
			await navigator.clipboard.writeText(body);
			copied = true;
			setTimeout(() => {
				copied = false;
			}, 1500);
		} catch {
			/* ignore */
		}
	}

	function selectAll() {
		if (!preEl) return;
		const range = document.createRange();
		range.selectNodeContents(preEl);
		const sel = window.getSelection();
		sel?.removeAllRanges();
		sel?.addRange(range);
	}

	$effect(() => {
		const d = domain;
		const id = streamId;
		logs = [];
		status = 'connecting';
		hasMoreHistory = true;
		historyLoaded = false;
		historyHint = '';
		closeCode = undefined;
		closeReason = '';
		sawDisconnect = false;
		copied = false;
		stickToBottom = true;
		downloadOpen = false;

		let cancelled = false;
		void seedRecent(d, id, () => cancelled);
		const stop = connectDomainLog(
			d,
			id,
			(entry) => {
				if (cancelled) return;
				if (logs.some((e) => e.id === entry.id)) return;
				const next = [...logs, entry];
				// Cap the live/Redis window only until the user has pulled DB history.
				logs = historyLoaded ? next : next.slice(-LIVE_CAP);
				void scrollIfNeeded();
			},
			(s, detail) => {
				if (cancelled) return;
				status = s;
				if (s === 'closed' || s === 'error') {
					sawDisconnect = true;
					if (detail) {
						closeCode = detail.code;
						closeReason = detail.reason ?? '';
					}
				}
				if (s === 'open') {
					sawDisconnect = false;
					closeCode = undefined;
					closeReason = '';
				}
			}
		);
		return () => {
			cancelled = true;
			stop();
		};
	});
</script>

<section class="log-page">
	<header class="log-head">
		<div>
			<h1>{title}</h1>
			{#if subtitle}
				<p class="sub">{subtitle}</p>
			{/if}
			<p class="meta">
				<code>/ws/{domain}/{streamId}</code>
				<span class="status" data-status={status}>{status}</span>
				{#if status === 'closed' || status === 'error'}
					<span class="hint">{status === 'error' ? 'connection error' : connectionHint()}</span>
				{/if}
				{#if historyHint}
					<span class="hint">{historyHint}</span>
				{/if}
			</p>
		</div>
		<div class="actions">
			<button type="button" class="secondary" onclick={() => (downloadOpen = true)}>
				Download
			</button>
			<button type="button" class="secondary" onclick={selectAll} disabled={!text}>
				Select all
			</button>
			<button type="button" onclick={copyAll} disabled={!text}>
				{copied ? 'Copied' : 'Copy'}
			</button>
		</div>
	</header>

	<pre
		class="term"
		bind:this={preEl}
		onscroll={onScroll}
		aria-live="polite"
		aria-label={`${title} terminal`}>{#if !text}<span class="empty">{emptyMessage}</span>{:else}{text}{/if}</pre>
</section>

<LogDownloadModal
	{domain}
	{streamId}
	open={downloadOpen}
	onclose={() => (downloadOpen = false)}
/>

<style>
	.log-page {
		display: grid;
		grid-template-rows: auto 1fr;
		gap: 1rem;
		min-height: 0;
		height: 100%;
	}

	.log-head {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		align-items: end;
		gap: 1rem;
	}

	.log-head h1 {
		margin: 0;
		font-size: 1.5rem;
		letter-spacing: 0.04em;
	}

	.sub {
		margin: 0.25rem 0 0;
		color: var(--muted);
	}

	.meta {
		margin: 0.65rem 0 0;
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem;
		align-items: center;
		font-family: var(--font);
		font-size: 0.8rem;
		color: var(--muted);
	}

	code {
		color: var(--text);
	}

	.status[data-status='open'] {
		color: var(--ok);
	}
	.status[data-status='closed'] {
		color: var(--warn);
	}
	.status[data-status='error'] {
		color: var(--err);
	}

	.hint {
		color: var(--muted);
	}

	.actions {
		display: flex;
		gap: 0.5rem;
	}

	.term {
		margin: 0;
		background: #0b0f14;
		color: #d7e0ea;
		border: 1px solid var(--border);
		border-radius: 4px;
		min-height: 420px;
		max-height: calc(100vh - 12rem);
		overflow: auto;
		padding: 0.85rem 1rem;
		font-family: var(--font);
		font-size: 0.78rem;
		line-height: 1.55;
		white-space: pre;
		tab-size: 4;
		user-select: text;
		-webkit-user-select: text;
	}

	.empty {
		color: var(--muted);
	}
</style>
