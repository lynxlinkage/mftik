<script lang="ts">
	/**
	 * A labelled row of chips — symbols filled, venues outlined — matching the
	 * strategy-card bindings. Long lists truncate to `maxChips`; the rest is
	 * on the overflow chip's title (and so available to a hover and to AT).
	 */
	interface Props {
		label?: string;
		items?: string[];
		kind?: 'symbol' | 'venue';
		maxChips?: number;
		empty?: string;
	}

	let {
		label = '',
		items = [],
		kind = 'symbol',
		maxChips = 3,
		empty = 'none'
	}: Props = $props();

	const parts = $derived({
		shown: items.slice(0, maxChips),
		rest: items.slice(maxChips)
	});
</script>

<div class="binding" class:bare={!label}>
	{#if label}
		<span class="key">{label}</span>
	{/if}
	<span class="chips">
		{#each parts.shown as item (item)}
			<span class="chip" class:venue={kind === 'venue'} title={item}>{item}</span>
		{/each}
		{#if parts.rest.length}
			<span
				class="chip more"
				class:venue={kind === 'venue'}
				title={parts.rest.join(', ')}
			>
				+{parts.rest.length}
			</span>
		{/if}
		{#if !items.length}
			<span class="none">{empty}</span>
		{/if}
	</span>
</div>

<style>
	.binding {
		display: grid;
		grid-template-columns: max-content 1fr;
		gap: 0.875rem;
		align-items: start;
	}

	.binding.bare {
		display: block;
	}

	.key {
		font-family: var(--font);
		font-size: 0.75rem;
		color: var(--muted);
		letter-spacing: 0.04em;
		padding-top: 0.125rem;
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 0.25rem;
	}

	.chip {
		font-family: var(--font);
		font-size: 0.6875rem;
		line-height: 1;
		padding: 0.25rem 0.375rem;
		border-radius: 3px;
		background: rgba(255, 255, 255, 0.06);
		border: 1px solid transparent;
		white-space: nowrap;
	}

	.chip.venue {
		background: transparent;
		border-color: var(--border);
		color: var(--muted);
	}

	.chip.more {
		color: var(--muted);
		cursor: default;
	}

	.none {
		font-family: var(--font);
		font-size: 0.6875rem;
		color: var(--muted);
		opacity: 0.8;
		padding-top: 0.1875rem;
		display: inline-block;
	}
</style>
