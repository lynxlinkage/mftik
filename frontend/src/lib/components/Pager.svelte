<script lang="ts">
	/**
	 * Numbered pages for a limit/offset list. Hidden when everything fits
	 * on page one. The window around the current page keeps the strip
	 * short when the total runs into the hundreds.
	 */
	interface Props {
		page: number;
		pageCount: number;
		disabled?: boolean;
		onchange: (page: number) => void;
	}

	let { page, pageCount, disabled = false, onchange }: Props = $props();

	const show = $derived(pageCount > 1);
	const canPrev = $derived(page > 1);
	const canNext = $derived(page < pageCount);
	const items = $derived(pageItems(page, pageCount));

	function pageItems(current: number, last: number): Array<number | 'ellipsis'> {
		if (last <= 7) {
			return Array.from({ length: last }, (_, i) => i + 1);
		}
		const keep = new Set<number>([1, last, current]);
		for (const n of [current - 1, current + 1]) {
			if (n >= 1 && n <= last) keep.add(n);
		}
		if (current <= 3) {
			keep.add(2);
			keep.add(3);
			keep.add(4);
		}
		if (current >= last - 2) {
			keep.add(last - 3);
			keep.add(last - 2);
			keep.add(last - 1);
		}
		const ordered = [...keep].filter((n) => n >= 1 && n <= last).sort((a, b) => a - b);
		const out: Array<number | 'ellipsis'> = [];
		for (const n of ordered) {
			const prev = out.at(-1);
			if (typeof prev === 'number' && n > prev + 1) {
				// A gap of one is the page itself: "1 … 3" is wider than
				// "1 2 3" and costs a click to reach a number that fits.
				if (n === prev + 2) out.push(prev + 1);
				else out.push('ellipsis');
			}
			out.push(n);
		}
		return out;
	}

	function go(next: number) {
		if (disabled || next < 1 || next > pageCount || next === page) return;
		onchange(next);
	}
</script>

{#if show}
	<nav class="pager" aria-label="Pagination">
		<button
			type="button"
			class="secondary"
			onclick={() => go(page - 1)}
			disabled={disabled || !canPrev}
		>
			Previous
		</button>
		{#each items as item, i (typeof item === 'number' ? item : `e${i}`)}
			{#if item === 'ellipsis'}
				<span class="gap" aria-hidden="true">…</span>
			{:else}
				<button
					type="button"
					class:active={item === page}
					aria-current={item === page ? 'page' : undefined}
					aria-label={`Page ${item}`}
					onclick={() => go(item)}
					disabled={disabled}
				>
					{item}
				</button>
			{/if}
		{/each}
		<button
			type="button"
			class="secondary"
			onclick={() => go(page + 1)}
			disabled={disabled || !canNext}
		>
			Next
		</button>
	</nav>
{/if}

<style>
	.pager {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: center;
		gap: 0.35rem;
		padding-top: 1rem;
	}

	.pager button {
		min-width: 2.25rem;
	}

	.pager button.active {
		color: var(--text);
		border-color: var(--accent);
		background: var(--accent-dim);
	}

	.gap {
		min-width: 1.25rem;
		text-align: center;
		color: var(--muted);
		font-size: 0.85rem;
	}
</style>
