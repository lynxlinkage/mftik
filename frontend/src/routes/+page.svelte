<script lang="ts">
	import { onDestroy } from 'svelte';
	import { connectSessionLog, newSessionId, type LogEntry } from '$lib/logging/session';

	let sessionId = $state(newSessionId());
	let status = $state<'idle' | 'connecting' | 'open' | 'closed' | 'error'>('idle');
	let logs = $state<LogEntry[]>([]);
	let disconnect: (() => void) | null = null;

	function start() {
		stop();
		logs = [];
		disconnect = connectSessionLog(
			sessionId,
			(entry) => {
				logs = [...logs, entry].slice(-500);
			},
			(s) => {
				status = s;
			}
		);
	}

	function stop() {
		disconnect?.();
		disconnect = null;
		if (status !== 'idle') status = 'closed';
	}

	function regenerate() {
		stop();
		sessionId = newSessionId();
		logs = [];
		status = 'idle';
	}

	onDestroy(stop);

	function formatTs(ts: number): string {
		return new Date(ts * 1000).toLocaleTimeString();
	}

	function levelClass(level: string): string {
		switch (level) {
			case 'error':
				return 'err';
			case 'warn':
			case 'warning':
				return 'warn';
			case 'debug':
				return 'muted';
			default:
				return 'ok';
		}
	}
</script>

<main>
	<header>
		<h1>MFT</h1>
		<p>Live logging session</p>
	</header>

	<section class="controls">
		<label>
			Session ID
			<input bind:value={sessionId} disabled={status === 'open' || status === 'connecting'} />
		</label>
		<div class="actions">
			<button type="button" onclick={start} disabled={status === 'open' || status === 'connecting'}>
				Connect
			</button>
			<button type="button" class="secondary" onclick={stop} disabled={status !== 'open' && status !== 'connecting'}>
				Disconnect
			</button>
			<button type="button" class="secondary" onclick={regenerate}>New session</button>
		</div>
		<span class="status" data-status={status}>status: {status}</span>
	</section>

	<section class="log-panel" aria-live="polite">
		{#if logs.length === 0}
			<p class="empty">Connect to stream logs from <code>ws/&lt;session_id&gt;</code></p>
		{:else}
			<ul>
				{#each logs as entry (entry.id)}
					<li>
						<span class="ts">{formatTs(entry.ts)}</span>
						<span class="level {levelClass(entry.level)}">{entry.level}</span>
						<span class="source">{entry.source}</span>
						<span class="msg">{entry.message}</span>
					</li>
				{/each}
			</ul>
		{/if}
	</section>
</main>

<style>
	main {
		max-width: 960px;
		margin: 0 auto;
		padding: 2.5rem 1.25rem 3rem;
	}

	header h1 {
		font-family: var(--font);
		font-size: 2.75rem;
		letter-spacing: 0.08em;
		margin: 0;
	}

	header p {
		margin: 0.35rem 0 0;
		color: var(--muted);
	}

	.controls {
		margin-top: 2rem;
		display: grid;
		gap: 0.85rem;
	}

	label {
		display: grid;
		gap: 0.35rem;
		font-size: 0.85rem;
		color: var(--muted);
	}

	input {
		font-family: var(--font);
		background: var(--surface);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.65rem 0.75rem;
		border-radius: 4px;
		font-size: 1rem;
	}

	.actions {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem;
	}

	button {
		font-family: var(--display);
		background: var(--accent);
		color: #041018;
		border: none;
		padding: 0.55rem 1rem;
		border-radius: 4px;
		font-weight: 600;
		cursor: pointer;
	}

	button:disabled {
		opacity: 0.45;
		cursor: not-allowed;
	}

	button.secondary {
		background: transparent;
		color: var(--text);
		border: 1px solid var(--border);
	}

	.status {
		font-family: var(--font);
		font-size: 0.8rem;
		color: var(--muted);
	}

	.status[data-status='open'] {
		color: var(--ok);
	}

	.status[data-status='error'] {
		color: var(--err);
	}

	.log-panel {
		margin-top: 1.5rem;
		background: linear-gradient(180deg, rgba(26, 35, 50, 0.95), rgba(15, 20, 25, 0.9));
		border: 1px solid var(--border);
		border-radius: 6px;
		min-height: 360px;
		max-height: 60vh;
		overflow: auto;
		padding: 0.75rem;
	}

	.empty {
		color: var(--muted);
		font-family: var(--font);
		font-size: 0.85rem;
		padding: 1rem;
	}

	ul {
		list-style: none;
		margin: 0;
		padding: 0;
		font-family: var(--font);
		font-size: 0.8rem;
	}

	li {
		display: grid;
		grid-template-columns: 5.5rem 3.5rem 7rem 1fr;
		gap: 0.6rem;
		padding: 0.35rem 0.4rem;
		border-bottom: 1px solid rgba(42, 53, 68, 0.6);
	}

	.ts,
	.source {
		color: var(--muted);
	}

	.level.ok {
		color: var(--ok);
	}
	.level.warn {
		color: var(--warn);
	}
	.level.err {
		color: var(--err);
	}
	.level.muted {
		color: var(--muted);
	}

	.msg {
		word-break: break-word;
	}

	@media (max-width: 720px) {
		li {
			grid-template-columns: 1fr;
			gap: 0.15rem;
		}
	}
</style>
