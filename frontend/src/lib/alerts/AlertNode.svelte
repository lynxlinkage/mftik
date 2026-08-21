<script lang="ts">
	import { onMount } from 'svelte';
	import { Handle, Position, useUpdateNodeInternals, type NodeProps } from '@xyflow/svelte';
	import { graphActions } from './actions';
	import type { AlertNode } from './graph';

	let { id, data, selected }: NodeProps<AlertNode> = $props();
	const alert = $derived(data.alert);
	const updateInternals = useUpdateNodeInternals();
	onMount(() => updateInternals(id));
</script>

<div class="card alert" class:selected class:off={!alert.enabled} data-kind="alert">
	<Handle type="target" position={Position.Left} />
	<header>
		<span class="kicker">Alert</span>
		<button
			type="button"
			class="ghost nodrag nopan"
			aria-label="Remove alert {alert.id}"
			onclick={(e) => {
				e.stopPropagation();
				graphActions.remove('alert', alert.id);
			}}
		>
			×
		</button>
	</header>
	<strong>{alert.name}</strong>
	<code>{alert.webhook_masked}</code>
	{#if !alert.enabled}
		<span class="muted">off</span>
	{/if}
	<button
		type="button"
		class="secondary nodrag nopan"
		onclick={(e) => {
			e.stopPropagation();
			graphActions.testAlert(alert.id);
		}}>Test</button
	>
</div>

<style>
	.card {
		position: relative;
		min-width: 14rem;
		max-width: 17rem;
		padding: 0.75rem 0.85rem 0.85rem 1.15rem;
		border-radius: 10px;
		border: 1px solid rgba(61, 156, 240, 0.4);
		background: linear-gradient(180deg, rgba(20, 32, 46, 0.96), rgba(14, 22, 32, 0.94));
		box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28);
	}
	.card.selected {
		border-color: var(--accent);
		box-shadow: 0 0 0 1px rgba(61, 156, 240, 0.4), 0 12px 32px rgba(0, 0, 0, 0.35);
	}
	.card.off {
		opacity: 0.72;
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
		color: var(--accent);
	}
	strong {
		display: block;
		font-size: 0.95rem;
	}
	code {
		display: block;
		margin: 0.25rem 0 0.55rem;
		color: var(--muted);
		font-size: 0.72rem;
		word-break: break-all;
	}
	.muted {
		display: inline-block;
		margin-bottom: 0.45rem;
		color: var(--muted);
		font-size: 0.75rem;
	}
	header button {
		padding: 0;
		width: 1.3rem;
		height: 1.3rem;
		line-height: 1;
		font-size: 1rem;
		font-weight: 400;
	}
</style>
