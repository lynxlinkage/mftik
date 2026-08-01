<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { connectDomainLog, type LogDomain, type LogEntry } from '$lib/logging/session';

	interface Props {
		domain: LogDomain;
		streamId: string;
		title?: string;
		subtitle?: string;
	}

	let { domain, streamId, title = 'Session log', subtitle }: Props = $props();

	let status = $state<'connecting' | 'open' | 'closed' | 'error'>('connecting');
	let logs = $state<LogEntry[]>([]);
	let copied = $state(false);
	let preEl = $state<HTMLPreElement | null>(null);
	let disconnect: (() => void) | null = null;
	let stickToBottom = true;

	function formatTime(ts: number): string {
		return new Date(ts * 1000).toLocaleTimeString('en-GB', { hour12: false });
	}

	function formatLine(entry: LogEntry): string {
		const t = formatTime(entry.ts);
		const level = entry.level.toUpperCase().padEnd(5, ' ');
		const source = entry.source.padEnd(16, ' ');
		return `${t}  ${level}  ${source}  ${entry.message}`;
	}

	const text = $derived(
		logs.length === 0 ? '' : logs.map(formatLine).join('\n') + '\n'
	);

	async function scrollIfNeeded() {
		await tick();
		if (stickToBottom && preEl) {
			preEl.scrollTop = preEl.scrollHeight;
		}
	}

	function onScroll() {
		if (!preEl) return;
		const gap = preEl.scrollHeight - preEl.scrollTop - preEl.clientHeight;
		stickToBottom = gap < 40;
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

	onMount(() => {
		disconnect = connectDomainLog(
			domain,
			streamId,
			(entry) => {
				logs = [...logs, entry].slice(-500);
				void scrollIfNeeded();
			},
			(s) => {
				status = s;
			}
		);
	});

	onDestroy(() => {
		disconnect?.();
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
			</p>
		</div>
		<div class="actions">
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
		aria-label={`${title} terminal`}>{#if !text}<span class="empty">Waiting for log lines…</span>{:else}{text}{/if}</pre>
</section>

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
	.status[data-status='error'] {
		color: var(--err);
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
