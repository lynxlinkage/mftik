<script lang="ts">
	import type { Instance } from '$lib/api';

	interface Props {
		instances: Instance[];
		value: string;
		disabled?: boolean;
	}

	let { instances, value = $bindable(''), disabled = false }: Props = $props();
</script>

<!-- Shown only when there is a choice. A single STS has nothing to decide,
     and omitting `instance` is anycast — the same as every deploy before
     instances existed (PI-5). -->
{#if instances.length > 1}
	<label class="type-pick">
		STS instance
		<select bind:value {disabled} aria-label="STS instance">
			<option value="">any (anycast)</option>
			{#each instances as i (i.name)}
				<option value={i.name} disabled={!i.enabled}>
					{i.name}{i.region ? ` — ${i.region}` : ''}{i.enabled ? '' : ' (draining)'}
				</option>
			{/each}
		</select>
	</label>
{/if}

<style>
	.type-pick {
		display: grid;
		gap: 0.3rem;
		font-size: 0.75rem;
		color: var(--muted);
	}

	select {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.55rem 0.65rem;
		border-radius: var(--radius);
		min-width: 10rem;
	}
</style>
