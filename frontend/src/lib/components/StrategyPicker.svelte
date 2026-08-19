<script lang="ts">
	import type { StrategyTemplate } from '$lib/api';

	interface Props {
		templates: StrategyTemplate[];
		value: string;
		disabled?: boolean;
		onchange: (type: string) => void;
	}

	let { templates, value, disabled = false, onchange }: Props = $props();

	let open = $state(false);
	let query = $state('');
	let activeIndex = $state(0);
	let rootEl = $state<HTMLDivElement | null>(null);
	let inputEl = $state<HTMLInputElement | null>(null);

	const selected = $derived(templates.find((t) => t.type === value) ?? null);

	function originOf(t: StrategyTemplate): string {
		if (t.source !== 'registry') return '';
		const sep = t.type.indexOf('::');
		return sep > 0 ? t.type.slice(0, sep) : '';
	}

	function matches(t: StrategyTemplate, q: string): boolean {
		if (!q) return true;
		const hay = `${t.label} ${t.type} ${originOf(t)}`.toLowerCase();
		return hay.includes(q);
	}

	const filtered = $derived.by(() => {
		const q = query.trim().toLowerCase();
		return templates.filter((t) => matches(t, q));
	});
	const mine = $derived(filtered.filter((t) => t.source === 'registry'));
	const bundled = $derived(filtered.filter((t) => t.source !== 'registry'));
	const flat = $derived([...mine, ...bundled]);

	$effect(() => {
		const idx = flat.findIndex((t) => t.type === value);
		activeIndex = idx >= 0 ? idx : 0;
	});

	$effect(() => {
		if (open) inputEl?.focus();
	});

	function openPicker() {
		if (disabled || templates.length === 0) return;
		open = true;
		query = '';
	}

	function closePicker() {
		open = false;
		query = '';
	}

	function togglePicker() {
		if (open) closePicker();
		else openPicker();
	}

	function pick(type: string) {
		closePicker();
		if (type !== value) onchange(type);
	}

	function onDocPointerDown(event: PointerEvent) {
		if (rootEl?.contains(event.target as Node)) return;
		closePicker();
	}

	function onTriggerKey(event: KeyboardEvent) {
		if (disabled) return;
		if (event.key === 'ArrowDown') {
			event.preventDefault();
			openPicker();
		}
	}

	function onSearchKey(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.preventDefault();
			closePicker();
			return;
		}
		if (event.key === 'ArrowDown') {
			event.preventDefault();
			if (flat.length === 0) return;
			activeIndex = (activeIndex + 1) % flat.length;
			return;
		}
		if (event.key === 'ArrowUp') {
			event.preventDefault();
			if (flat.length === 0) return;
			activeIndex = (activeIndex - 1 + flat.length) % flat.length;
			return;
		}
		if (event.key === 'Enter') {
			event.preventDefault();
			const hit = flat[activeIndex];
			if (hit) pick(hit.type);
		}
	}

	function optionId(type: string): string {
		return `strategy-opt-${type.replaceAll(':', '-')}`;
	}
</script>

<svelte:window onpointerdown={open ? onDocPointerDown : undefined} />

<div class="picker" bind:this={rootEl}>
	<button
		type="button"
		class="trigger"
		disabled={disabled || templates.length === 0}
		aria-haspopup="listbox"
		aria-expanded={open}
		aria-controls="strategy-list"
		onclick={togglePicker}
		onkeydown={onTriggerKey}
	>
		<span>{selected?.label || 'Select a strategy'}</span>
		<span class="chevron" aria-hidden="true">▾</span>
	</button>
	{#if open}
		<div class="menu">
			<input
				bind:this={inputEl}
				class="search"
				type="text"
				role="combobox"
				aria-autocomplete="list"
				aria-expanded="true"
				aria-controls="strategy-list"
				aria-activedescendant={flat[activeIndex] ? optionId(flat[activeIndex].type) : undefined}
				placeholder="Filter by name, type, origin"
				autocomplete="off"
				bind:value={query}
				onkeydown={onSearchKey}
			/>
			<div class="list" id="strategy-list" role="listbox" aria-label="Strategies">
				{#if mine.length > 0}
					<div class="group" role="presentation">My strategies</div>
					{#each mine as t (t.type)}
						<button
							type="button"
							id={optionId(t.type)}
							role="option"
							tabindex="-1"
							class="option"
							class:current={t.type === value}
							class:active={flat[activeIndex]?.type === t.type}
							aria-selected={t.type === value}
							onpointerenter={() => {
								activeIndex = flat.findIndex((x) => x.type === t.type);
							}}
							onclick={() => pick(t.type)}
						>
							<span class="opt-label">{t.label}</span>
							{#if t.requires?.length && t.env_ok === false}
								<span class="badge paused">needs {t.requires.join(', ')}</span>
							{/if}
						</button>
					{/each}
				{/if}
				{#if bundled.length > 0}
					<div class="group" role="presentation">Built-in examples</div>
					{#each bundled as t (t.type)}
						<button
							type="button"
							id={optionId(t.type)}
							role="option"
							tabindex="-1"
							class="option"
							class:current={t.type === value}
							class:active={flat[activeIndex]?.type === t.type}
							aria-selected={t.type === value}
							onpointerenter={() => {
								activeIndex = flat.findIndex((x) => x.type === t.type);
							}}
							onclick={() => pick(t.type)}
						>
							<span class="opt-label">{t.label}</span>
							{#if t.requires?.length && t.env_ok === false}
								<span class="badge paused">needs {t.requires.join(', ')}</span>
							{/if}
						</button>
					{/each}
				{/if}
				{#if flat.length === 0}
					<p class="empty">No matches</p>
				{/if}
			</div>
		</div>
	{/if}
</div>

<style>
	.picker {
		position: relative;
		min-width: 14rem;
	}

	.trigger {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.6rem;
		width: 100%;
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.45rem 0.6rem;
		border-radius: var(--radius);
		font-weight: 400;
		text-align: left;
	}

	.trigger:hover:not(:disabled) {
		border-color: var(--accent);
	}

	.trigger:disabled {
		opacity: 0.45;
	}

	.chevron {
		color: var(--muted);
		font-size: 0.7rem;
		line-height: 1;
	}

	.menu {
		position: absolute;
		z-index: 30;
		top: calc(100% + 0.25rem);
		left: 0;
		right: 0;
		min-width: 16rem;
		background: var(--bg-elevated);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
		overflow: hidden;
	}

	.search {
		width: 100%;
		background: var(--bg);
		border: none;
		border-bottom: 1px solid var(--border);
		color: var(--text);
		padding: 0.5rem 0.65rem;
		border-radius: 0;
	}

	.search:focus {
		outline: none;
	}

	.list {
		max-height: 18rem;
		overflow-y: auto;
		padding: 0.25rem 0;
	}

	.group {
		padding: 0.45rem 0.65rem 0.25rem;
		color: var(--muted);
		font-size: 0.68rem;
		font-weight: 600;
		letter-spacing: 0.06em;
		text-transform: uppercase;
	}

	.option {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.6rem;
		width: 100%;
		background: none;
		border: none;
		border-radius: 0;
		color: var(--text);
		font-weight: 400;
		text-align: left;
		padding: 0.4rem 0.65rem;
	}

	.opt-label {
		min-width: 0;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.option.current {
		color: var(--accent);
	}

	.option.active {
		background: var(--accent-dim);
	}

	.empty {
		margin: 0;
		padding: 0.7rem 0.65rem;
		color: var(--muted);
		font-size: 0.78rem;
	}
</style>
