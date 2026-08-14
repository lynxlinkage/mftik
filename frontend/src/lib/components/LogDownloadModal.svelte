<script lang="ts">
	import { api } from '$lib/api';
	import type { LogDomain } from '$lib/logging/session';

	interface Props {
		domain: LogDomain;
		streamId: string;
		open: boolean;
		onclose: () => void;
	}

	let { domain, streamId, open, onclose }: Props = $props();

	function utcToday(): string {
		return new Date().toISOString().slice(0, 10);
	}

	let from = $state(utcToday());
	let to = $state(utcToday());
	let busy = $state(false);
	let error = $state<string | null>(null);

	$effect(() => {
		if (open) {
			const today = utcToday();
			from = today;
			to = today;
			error = null;
			busy = false;
		}
	});

	async function confirm() {
		if (!from || !to) {
			error = 'Pick a from and to date.';
			return;
		}
		if (to < from) {
			error = 'to must be on or after from.';
			return;
		}
		const days = Math.round((Date.parse(to) - Date.parse(from)) / 86_400_000) + 1;
		if (days > 31) {
			error = 'range must be at most 31 days.';
			return;
		}
		busy = true;
		error = null;
		try {
			await api.downloadLogs(domain, streamId, from, to);
			onclose();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function onKey(event: KeyboardEvent) {
		if (event.key === 'Escape' && !busy) onclose();
	}
</script>

{#if open}
	<div
		class="backdrop"
		role="presentation"
		onclick={() => {
			if (!busy) onclose();
		}}
		onkeydown={onKey}
	>
		<div
			class="modal"
			role="dialog"
			aria-modal="true"
			aria-labelledby="log-dl-title"
			tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => e.stopPropagation()}
		>
			<h2 id="log-dl-title">Download logs</h2>
			<p class="muted">UTC days. One day is a .log; several days pack into a .tar.gz.</p>
			<div class="fields">
				<label>
					From
					<input type="date" bind:value={from} disabled={busy} />
				</label>
				<label>
					To
					<input type="date" bind:value={to} disabled={busy} />
				</label>
			</div>
			{#if error}
				<p class="err">{error}</p>
			{/if}
			<div class="actions">
				<button type="button" class="secondary" onclick={onclose} disabled={busy}>Cancel</button>
				<button type="button" onclick={confirm} disabled={busy}>
					{busy ? 'Downloading…' : 'Download'}
				</button>
			</div>
		</div>
	</div>
{/if}

<style>
	.backdrop {
		position: fixed;
		inset: 0;
		z-index: 40;
		background: rgba(4, 8, 14, 0.72);
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 1.5rem;
	}

	.modal {
		width: min(28rem, 100%);
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 1.15rem 1.2rem 1.1rem;
		display: grid;
		gap: 0.85rem;
	}

	h2 {
		margin: 0;
		font-size: 1.1rem;
	}

	.muted {
		margin: 0;
		color: var(--muted);
		font-size: 0.82rem;
	}

	.fields {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 0.75rem;
	}

	label {
		display: grid;
		gap: 0.3rem;
		font-size: 0.75rem;
		color: var(--muted);
	}

	input[type='date'] {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.45rem 0.55rem;
		border-radius: var(--radius);
	}

	.err {
		margin: 0;
		color: var(--err);
		font-size: 0.82rem;
	}

	.actions {
		display: flex;
		justify-content: flex-end;
		gap: 0.5rem;
	}
</style>
