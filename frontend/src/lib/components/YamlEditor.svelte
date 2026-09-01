<script lang="ts">
	import type { ApiCredential, StsField } from '$lib/api';
	import {
		applyHint,
		hintContext,
		hintItems,
		insertTdAccount,
		newlineInsert,
		tdAccountKeys,
		type HintItem
	} from '$lib/ymlHints';

	interface Props {
		value: string;
		accounts?: ApiCredential[];
		fields?: StsField[];
		disabled?: boolean;
		rows?: number;
	}

	let {
		value = $bindable(),
		accounts = [],
		fields = [],
		disabled = false,
		rows = 12
	}: Props = $props();

	let ta = $state<HTMLTextAreaElement | null>(null);
	let wrap = $state<HTMLDivElement | null>(null);
	let open = $state(false);
	let items = $state<HintItem[]>([]);
	let active = $state(0);
	let menuTop = $state(0);
	let menuLeft = $state(0);
	let measureCanvas: HTMLCanvasElement | null = null;

	const attached = $derived(new Set(tdAccountKeys(value)));

	function cursorOf(): number {
		return ta?.selectionStart ?? value.length;
	}

	function refresh(force = false) {
		if (disabled || !ta) {
			open = false;
			return;
		}
		const ctx = hintContext(value, cursorOf());
		if (!ctx) {
			open = false;
			return;
		}
		const next = hintItems(ctx, { accounts, text: value, fields });
		items = next;
		active = 0;
		if (
			next.length &&
			(force || ctx.prefix.length > 0 || ctx.kind === 'td-key' || ctx.kind === 'sts-key')
		) {
			open = true;
			placeMenu();
		} else {
			open = false;
		}
	}

	function placeMenu() {
		if (!ta || !wrap) return;
		const style = getComputedStyle(ta);
		const lineHeight = parseFloat(style.lineHeight) || 19.55;
		const padTop = parseFloat(style.paddingTop) || 0;
		const padLeft = parseFloat(style.paddingLeft) || 0;
		const cursor = cursorOf();
		const before = value.slice(0, cursor);
		const lineIdx = before.split('\n').length - 1;
		const col = before.length - (before.lastIndexOf('\n') + 1);
		const metrics = measure(style.font, before.slice(before.lastIndexOf('\n') + 1));
		const x = padLeft + metrics - ta.scrollLeft;
		const y = padTop + (lineIdx + 1) * lineHeight - ta.scrollTop;
		menuLeft = Math.max(8, Math.min(x, ta.clientWidth - 16));
		menuTop = Math.max(8, y + 4);
	}

	function measure(font: string, text: string): number {
		const canvas = measureCanvas ?? (measureCanvas = document.createElement('canvas'));
		const ctx = canvas.getContext('2d');
		if (!ctx) return text.length * 8;
		ctx.font = font;
		return ctx.measureText(text).width;
	}

	function setText(next: string, cursor: number) {
		value = next;
		queueMicrotask(() => {
			if (!ta) return;
			ta.focus();
			ta.setSelectionRange(cursor, cursor);
			refresh(true);
		});
	}

	function accept(item: HintItem | undefined = items[active]) {
		if (!item) return;
		const ctx = hintContext(value, cursorOf());
		if (!ctx) return;
		const next = applyHint(value, ctx, item);
		open = false;
		setText(next.text, next.cursor);
	}

	function onInput() {
		refresh(false);
	}

	function onSelect() {
		refresh(false);
	}

	function onKeydown(e: KeyboardEvent) {
		if (disabled) return;
		if (open && items.length) {
			if (e.key === 'ArrowDown') {
				e.preventDefault();
				active = (active + 1) % items.length;
				return;
			}
			if (e.key === 'ArrowUp') {
				e.preventDefault();
				active = (active - 1 + items.length) % items.length;
				return;
			}
			if (e.key === 'Enter' || e.key === 'Tab') {
				e.preventDefault();
				accept();
				return;
			}
			if (e.key === 'Escape') {
				e.preventDefault();
				open = false;
				return;
			}
		}
		if ((e.ctrlKey || e.metaKey) && e.key === ' ') {
			e.preventDefault();
			refresh(true);
			return;
		}
		if (e.key === 'Enter' && !e.shiftKey) {
			e.preventDefault();
			const cur = cursorOf();
			const insert = newlineInsert(value, cur);
			setText(value.slice(0, cur) + insert + value.slice(ta?.selectionEnd ?? cur), cur + insert.length);
			return;
		}
		if (e.key === 'Tab' && !e.shiftKey) {
			e.preventDefault();
			const cur = cursorOf();
			const end = ta?.selectionEnd ?? cur;
			setText(value.slice(0, cur) + '  ' + value.slice(end), cur + 2);
		}
	}

	function onBlur(e: FocusEvent) {
		const next = e.relatedTarget;
		if (next instanceof Node && wrap?.contains(next)) return;
		open = false;
	}

	function pickAccount(name: string) {
		const next = insertTdAccount(value, name);
		setText(next.text, next.cursor);
	}
</script>

<div
	class="yml-wrap"
	bind:this={wrap}
	role="group"
	aria-label="strategy.yml editor with account hints"
>
	<textarea
		bind:this={ta}
		class="yml"
		bind:value
		{rows}
		spellcheck="false"
		{disabled}
		aria-label="strategy.yml editor"
		oninput={onInput}
		onkeydown={onKeydown}
		onclick={onSelect}
		onscroll={() => {
			if (open) placeMenu();
		}}
		onblur={onBlur}
	></textarea>
	{#if open && items.length}
		<ul
			id="yml-hints"
			class="hints"
			role="listbox"
			style:top="{menuTop}px"
			style:left="{menuLeft}px"
		>
			{#each items as item, i (item.kind + item.label)}
				<li>
					<button
						type="button"
						id={`yml-hint-${i}`}
						role="option"
						class="hint"
						class:active={i === active}
						aria-selected={i === active}
						onpointerenter={() => (active = i)}
						onmousedown={(e) => {
							e.preventDefault();
							accept(item);
						}}
					>
						<code>{item.label}</code>
						<span class="detail">{item.detail}</span>
					</button>
				</li>
			{/each}
		</ul>
	{/if}
</div>
<div class="hint-row">
	<span class="hint-label">td accounts</span>
	{#if accounts.length === 0}
		<span class="none">
			None on this node. <a href="/keys">Add a key</a> — deploy resolves
			<code>td:</code> by account name.
		</span>
	{:else}
		<span class="chips">
			{#each accounts as row (row.id)}
				<button
					type="button"
					class="chip"
					class:used={attached.has(row.name)}
					disabled={disabled}
					title={attached.has(row.name)
						? `${row.venue}/${row.name} is already under td:`
						: `Insert ${row.name} under td:`}
					onclick={() => pickAccount(row.name)}
				>
					{row.venue}/{row.name}
				</button>
			{/each}
		</span>
		<span class="kbd">Ctrl+Space</span>
	{/if}
</div>

<style>
	.yml-wrap {
		position: relative;
	}

	.yml {
		width: 100%;
		min-height: 16rem;
		resize: vertical;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
		font-size: 0.85rem;
		line-height: 1.45;
		tab-size: 2;
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		border-radius: var(--radius);
		padding: 0.85rem 1rem;
	}

	.yml:focus {
		outline: 2px solid color-mix(in srgb, var(--accent) 55%, transparent);
		outline-offset: 1px;
	}

	.hints {
		position: absolute;
		z-index: 20;
		min-width: 14rem;
		max-width: 22rem;
		max-height: 14rem;
		overflow-y: auto;
		margin: 0;
		padding: 0.25rem 0;
		list-style: none;
		background: var(--bg-elevated);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
	}

	.hint {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: 0.75rem;
		width: 100%;
		background: none;
		border: none;
		border-radius: 0;
		color: var(--text);
		font-weight: 400;
		text-align: left;
		padding: 0.35rem 0.65rem;
	}

	.hint.active {
		background: var(--accent-dim);
	}

	.hint code {
		font-size: 0.82rem;
	}

	.detail {
		color: var(--muted);
		font-size: 0.72rem;
	}

	.hint-row {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 0.5rem 0.75rem;
		margin-top: 0.55rem;
	}

	.hint-label {
		font-size: 0.72rem;
		color: var(--muted);
		letter-spacing: 0.05em;
		text-transform: uppercase;
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 0.35rem;
	}

	.chip {
		font-family: var(--font);
		font-size: 0.72rem;
		font-weight: 500;
		line-height: 1;
		padding: 0.28rem 0.45rem;
		border-radius: 3px;
		background: transparent;
		color: var(--text);
		border: 1px solid var(--border);
	}

	.chip:hover:not(:disabled) {
		border-color: var(--accent);
	}

	.chip.used {
		color: var(--muted);
		border-style: dashed;
	}

	.none {
		font-size: 0.78rem;
		color: var(--muted);
	}

	.kbd {
		margin-left: auto;
		font-size: 0.68rem;
		color: var(--muted);
		border: 1px solid var(--border);
		border-radius: 3px;
		padding: 0.12rem 0.35rem;
	}
</style>
