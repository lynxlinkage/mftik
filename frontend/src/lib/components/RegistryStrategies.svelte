<script lang="ts">
	import type { RegistryStrategy, RegistrySyncRow } from '$lib/api';

	interface Props {
		origin: string;
		strategies?: RegistryStrategy[];
		sync?: RegistrySyncRow[];
		loading?: boolean;
		empty: string;
		removing?: string | null;
		onRemove?: (name: string) => void;
	}

	let {
		origin,
		strategies = [],
		sync,
		loading = false,
		empty,
		removing = null,
		onRemove
	}: Props = $props();

	const canRemove = $derived(onRemove != null && sync == null);

	const rows = $derived(
		sync ??
			strategies.map((s) => ({
				name: s.name,
				type: s.type,
				local_digest: s.digest,
				remote_digest: null as string | null,
				status: 'local' as const
			}))
	);

	function qualified(typeName: string): string {
		return `${origin}::${typeName}`;
	}

	function shortDigest(digest: string | null): string {
		if (!digest) return '—';
		const hex = digest.startsWith('sha256:') ? digest.slice(7) : digest;
		return hex.length > 12 ? `sha256:${hex.slice(0, 8)}…` : digest;
	}

	function statusClass(status: string): string {
		switch (status) {
			case 'synced':
				return 'live';
			case 'diverged':
				return 'paused';
			case 'remote_only':
				return 'remote';
			case 'local_only':
				return 'done';
			case 'unknown':
				return 'failed';
			default:
				return '';
		}
	}

	function statusLabel(status: string): string {
		switch (status) {
			case 'synced':
				return 'synced';
			case 'diverged':
				return 'diverged';
			case 'remote_only':
				return 'on remote';
			case 'local_only':
				return 'local only';
			case 'unknown':
				return 'unknown';
			default:
				return 'local';
		}
	}
</script>

{#if rows.length === 0}
	<p class="empty-state">{loading ? 'Loading…' : empty}</p>
{:else}
	<table class="data">
		<thead>
			<tr>
				<th>Type</th>
				<th>Name</th>
				{#if sync}
					<th>Local</th>
					<th>Remote</th>
					<th>Status</th>
				{:else}
					<th>Digest</th>
				{/if}
				{#if canRemove}
					<th></th>
				{/if}
			</tr>
		</thead>
		<tbody>
			{#each rows as s (`${s.name}:${s.type}`)}
				<tr>
					<td><code>{qualified(s.type)}</code></td>
					<td>{s.name}</td>
					{#if sync}
						<td class="muted" title={s.local_digest ?? ''}>{shortDigest(s.local_digest)}</td>
						<td class="muted" title={s.remote_digest ?? ''}>{shortDigest(s.remote_digest)}</td>
						<td>
							<span class="badge {statusClass(s.status)}">
								{statusLabel(s.status)}
							</span>
						</td>
					{:else}
						<td class="muted" title={s.local_digest ?? ''}>{shortDigest(s.local_digest)}</td>
					{/if}
					{#if canRemove}
						<td class="right">
							<button
								type="button"
								class="secondary"
								onclick={() => onRemove?.(s.name)}
								disabled={removing === s.name}
							>
								{removing === s.name ? 'Removing…' : 'Remove'}
							</button>
						</td>
					{/if}
				</tr>
			{/each}
		</tbody>
	</table>
{/if}

<style>
	code {
		font-family: var(--font);
		font-size: 0.82rem;
	}

	.right {
		text-align: right;
	}
</style>
