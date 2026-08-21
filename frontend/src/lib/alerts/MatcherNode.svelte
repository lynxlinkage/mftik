<script lang="ts">
	import { onMount } from 'svelte';
	import { Handle, Position, useUpdateNodeInternals, type NodeProps } from '@xyflow/svelte';
	import { graphActions } from './actions';
	import { matcherSummary, type MatcherNode } from './graph';

	let { id, data, selected }: NodeProps<MatcherNode> = $props();
	const matcher = $derived(data.matcher);
	const summary = $derived(matcherSummary(matcher));
	const updateInternals = useUpdateNodeInternals();
	onMount(() => updateInternals(id));
</script>

<div class="card matcher" class:selected data-kind="matcher">
	<Handle type="target" position={Position.Left} />
	<header>
		<span class="kicker">Matcher</span>
		<button
			type="button"
			class="ghost nodrag nopan"
			aria-label="Remove matcher {matcher.id}"
			onclick={() => graphActions.remove('matcher', matcher.id)}
		>
			×
		</button>
	</header>
	<strong>{matcher.name}</strong>
	<code>{matcher.kind}</code>
	<p class="summary">{summary}</p>
	{#if matcher.disabled_reason}
		<p class="warn">disabled: {matcher.disabled_reason}</p>
	{/if}
	<Handle type="source" position={Position.Right} />
</div>

<style>
	.card {
		position: relative;
		min-width: 13.5rem;
		max-width: 16rem;
		padding: 0.75rem 1.15rem 0.85rem 1.15rem;
		border-radius: 10px;
		border: 1px solid rgba(240, 180, 41, 0.4);
		background: linear-gradient(180deg, rgba(42, 36, 20, 0.96), rgba(26, 22, 14, 0.94));
		box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28);
	}
	.card.selected {
		border-color: #f0b429;
		box-shadow: 0 0 0 1px rgba(240, 180, 41, 0.35), 0 12px 32px rgba(0, 0, 0, 0.35);
	}
	header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 0.35rem;
	}
	.kicker {
		font-size: 0.68rem;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: #f0b429;
	}
	strong {
		display: block;
		font-size: 0.95rem;
	}
	code {
		color: var(--muted);
		font-size: 0.78rem;
	}
	.summary {
		margin: 0.35rem 0 0;
		color: var(--text);
		font-size: 0.8rem;
		word-break: break-word;
	}
	.warn {
		margin: 0.4rem 0 0;
		color: var(--warn);
		font-size: 0.75rem;
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
