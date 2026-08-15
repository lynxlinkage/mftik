<script lang="ts">
	import { api, type EventLogInfo } from '$lib/api';

	interface Props {
		sessionId: string;
	}

	let { sessionId }: Props = $props();

	let info = $state<EventLogInfo | null>(null);
	let loading = $state(true);
	let busy = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		const id = sessionId;
		loading = true;
		error = null;
		api
			.eventLogInfo(id)
			.then((result) => {
				// The session may have changed while this was in flight.
				if (id === sessionId) info = result;
			})
			.catch((e) => {
				if (id === sessionId) error = e instanceof Error ? e.message : String(e);
			})
			.finally(() => {
				if (id === sessionId) loading = false;
			});
	});

	async function download() {
		busy = true;
		error = null;
		try {
			await api.downloadEventLog(sessionId);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function humanBytes(bytes: number): string {
		if (bytes < 1024) return `${bytes} B`;
		const units = ['KB', 'MB', 'GB'];
		let value = bytes / 1024;
		let unit = 0;
		while (value >= 1024 && unit < units.length - 1) {
			value /= 1024;
			unit += 1;
		}
		return `${value.toFixed(value < 10 ? 1 : 0)} ${units[unit]}`;
	}
</script>

<section class="eventlog">
	<div class="head">
		<h2>Event log</h2>
		{#if info?.available}
			<button type="button" onclick={download} disabled={busy}>
				{busy ? 'Downloading…' : `Download ${humanBytes(info.total_bytes)} .gz`}
			</button>
		{/if}
	</div>

	{#if loading}
		<p class="muted">Checking…</p>
	{:else if error}
		<p class="err">{error}</p>
	{:else if info?.available}
		<p class="muted">
			Every event this session was handed and every call it made, as jsonl.
			{#if info.parts > 1}
				{info.parts} rotated files, oldest first.
			{/if}
			{#if info.live}
				<strong>Still running</strong> — this is the log so far, not the whole of it.
			{/if}
		</p>
	{:else if info && !info.enabled}
		<p class="muted">Not kept in this deployment (STS_EVENTLOG_DIR is unset).</p>
	{:else}
		<p class="muted">
			None for this session. It may predate event logging, or have run on a different STS.
		</p>
	{/if}
</section>

<style>
	.eventlog {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 0.9rem 1rem;
		margin: 0 0 1rem;
		display: grid;
		gap: 0.5rem;
	}

	.head {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
	}

	h2 {
		margin: 0;
		font-size: 1rem;
	}

	.muted {
		margin: 0;
		color: var(--muted);
		font-size: 0.82rem;
	}

	.err {
		margin: 0;
		color: var(--danger, #f87171);
		font-size: 0.82rem;
	}
</style>
