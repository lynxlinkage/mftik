<script lang="ts">
	import { onMount } from 'svelte';
	import { Handle, Position, useUpdateNodeInternals, type NodeProps } from '@xyflow/svelte';
	import { graphActions } from './actions';
	import type { SourceNode } from './graph';

	let { id, data, selected }: NodeProps<SourceNode> = $props();
	const source = $derived(data.source);
	const updateInternals = useUpdateNodeInternals();
	onMount(() => updateInternals(id));
</script>

<div class="card kind-source" class:selected data-kind="source">
	<header>
		<span class="kicker">Source</span>
		<button
			type="button"
			class="ghost nodrag nopan"
			aria-label="Remove source {source.id}"
			onclick={() => graphActions.remove('source', source.id)}
		>
			×
		</button>
	</header>
	<code>{source.domain}:{source.selector}</code>
	<Handle type="source" position={Position.Right} />
</div>

<style>
	.card {
		position: relative;
		min-width: 13.5rem;
		max-width: 16rem;
		padding: 0.75rem 1.15rem 0.85rem 0.85rem;
		border-radius: 10px;
		border: 1px solid rgba(62, 207, 142, 0.35);
		background: linear-gradient(180deg, rgba(24, 42, 36, 0.96), rgba(16, 26, 24, 0.94));
		box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28);
	}
	.card.selected {
		border-color: #3ecf8e;
		box-shadow: 0 0 0 1px rgba(62, 207, 142, 0.35), 0 12px 32px rgba(0, 0, 0, 0.35);
	}
	header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 0.45rem;
	}
	.kicker {
		font-size: 0.68rem;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: #3ecf8e;
	}
	code {
		display: block;
		font-size: 0.86rem;
		color: var(--text);
	}
	button {
		padding: 0;
		width: 1.3rem;
		height: 1.3rem;
		line-height: 1;
		font-size: 1rem;
		font-weight: 400;
	}
</style>
