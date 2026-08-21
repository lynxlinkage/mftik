<script lang="ts">
	import type { ApiCredential, Venue } from '$lib/api';
	import type { GraphKind } from './graph';

	export type SourceDraft = { domain: 'sts' | 'td' | 'md'; selector: string };
	export type MatcherDraft = {
		name: string;
		kind: 'level' | 'regex' | 'extract';
		spec: Record<string, unknown>;
	};
	export type AlertDraft = {
		name: string;
		webhook_url: string;
		flush_interval_s: number;
		max_events_in_payload: number;
		max_buffer_events: number;
		dedupe: boolean;
		enabled: boolean;
	};
	export type CreateDraft = SourceDraft | MatcherDraft | AlertDraft;

	type Props = {
		kind: GraphKind;
		stsOptions: string[];
		liveTypeCounts: Record<string, number>;
		accounts: ApiCredential[];
		venues: Venue[];
		oncancel: () => void;
		oncreate: (draft: CreateDraft) => Promise<void>;
	};

	let {
		kind,
		stsOptions,
		liveTypeCounts,
		accounts,
		venues,
		oncancel,
		oncreate
	}: Props = $props();

	let sourceDomain = $state<'sts' | 'td' | 'md'>('sts');
	let sourceSelector = $state('*');
	let matcherName = $state('');
	let matcherKind = $state<'level' | 'regex' | 'extract'>('level');
	let levelWarn = $state(true);
	let levelError = $state(true);
	let levelInfo = $state(false);
	let matcherPattern = $state('');
	let extractGroup = $state(1);
	let extractAs = $state<'float' | 'int' | 'str'>('float');
	let extractOp = $state('>');
	let extractValue = $state('0.99');
	let alertName = $state('');
	let webhookUrl = $state('');
	let flushInterval = $state(30);
	let maxEvents = $state(15);
	let maxBuffer = $state(200);
	let dedupe = $state(true);
	let alertEnabled = $state(true);
	let busy = $state(false);
	let formError = $state<string | null>(null);

	const title = $derived(
		kind === 'source' ? 'New source' : kind === 'matcher' ? 'New matcher' : 'New alert'
	);
	const submitLabel = $derived(
		kind === 'source' ? 'Add source' : kind === 'matcher' ? 'Add matcher' : 'Add alert'
	);

	function matcherSpec(): Record<string, unknown> {
		if (matcherKind === 'level') {
			const levels = [
				levelInfo ? 'info' : null,
				levelWarn ? 'warn' : null,
				levelError ? 'error' : null
			].filter((x): x is string => x != null);
			return { levels };
		}
		if (matcherKind === 'regex') return { pattern: matcherPattern };
		if (extractAs === 'str') {
			return {
				pattern: matcherPattern,
				group: extractGroup,
				as: extractAs,
				op: extractOp,
				value: String(extractValue)
			};
		}
		const numeric = Number(String(extractValue).trim());
		if (!Number.isFinite(numeric)) {
			throw new Error('extract value must be a number');
		}
		return {
			pattern: matcherPattern,
			group: extractGroup,
			as: extractAs,
			op: extractOp,
			value: extractAs === 'int' ? Math.trunc(numeric) : numeric
		};
	}

	function draft(): CreateDraft {
		if (kind === 'source') return { domain: sourceDomain, selector: sourceSelector };
		if (kind === 'matcher') {
			return { name: matcherName, kind: matcherKind, spec: matcherSpec() };
		}
		return {
			name: alertName,
			webhook_url: webhookUrl,
			flush_interval_s: flushInterval,
			max_events_in_payload: maxEvents,
			max_buffer_events: maxBuffer,
			dedupe,
			enabled: alertEnabled
		};
	}

	async function submit() {
		busy = true;
		formError = null;
		try {
			await oncreate(draft());
		} catch (e) {
			formError = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function onKey(event: KeyboardEvent) {
		if (event.key === 'Escape') oncancel();
	}
</script>

<div
	class="popover"
	role="dialog"
	aria-modal="true"
	aria-labelledby={`create-title-${kind}`}
	tabindex="-1"
	onkeydown={onKey}
>
	<h2 id={`create-title-${kind}`}>{title}</h2>
		<form
			onsubmit={(e) => {
				e.preventDefault();
				void submit();
			}}
		>
			{#if kind === 'source'}
				<label>Domain
					<select bind:value={sourceDomain} onchange={() => (sourceSelector = '*')}>
						<option value="sts">sts</option>
						<option value="td">td</option>
						<option value="md">md</option>
					</select>
				</label>
				<label>Selector
					<select bind:value={sourceSelector} data-testid="sts-picker">
						<option value="*">*</option>
						{#if sourceDomain === 'sts'}
							{#each stsOptions as type (type)}
								<option value={type}
									>{type}{liveTypeCounts[type] ? ` (${liveTypeCounts[type]} live)` : ''}</option
								>
							{/each}
						{:else if sourceDomain === 'td'}
							{#each accounts as account (account.id)}
								<option value={String(account.id)}>{account.name} ({account.id})</option>
							{/each}
						{:else}
							{#each venues as venue (venue.name)}
								<option value={venue.name}>{venue.name}</option>
							{/each}
						{/if}
					</select>
				</label>
			{:else if kind === 'matcher'}
				<label>Matcher name <input bind:value={matcherName} required /></label>
				<label>Kind
					<select bind:value={matcherKind}>
						<option value="level">level</option>
						<option value="regex">regex</option>
						<option value="extract">extract</option>
					</select>
				</label>
				{#if matcherKind === 'level'}
					<label><input type="checkbox" bind:checked={levelInfo} /> info</label>
					<label><input type="checkbox" bind:checked={levelWarn} /> warn</label>
					<label><input type="checkbox" bind:checked={levelError} /> error</label>
				{:else}
					<label>Pattern <input bind:value={matcherPattern} required /></label>
					{#if matcherKind === 'extract'}
						<label>Group <input type="number" min="1" bind:value={extractGroup} /></label>
						<label>As
							<select bind:value={extractAs}>
								<option value="float">float</option>
								<option value="int">int</option>
								<option value="str">str</option>
							</select>
						</label>
						<label>Op
							<select bind:value={extractOp}>
								<option value=">">&gt;</option>
								<option value=">=">&gt;=</option>
								<option value="<">&lt;</option>
								<option value="<=">&lt;=</option>
								<option value="==">==</option>
								<option value="!=">!=</option>
							</select>
						</label>
						{#if extractAs === 'str'}
							<!-- A number input coerces the binding, so `str` would send a
							     number and 422 — and a non-numeric string cannot be typed
							     into one at all. -->
							<label>Value <input bind:value={extractValue} required /></label>
						{:else}
							<label>Value
								<input type="number" step="any" bind:value={extractValue} required />
							</label>
						{/if}
					{/if}
				{/if}
			{:else}
				<label>Alert name <input bind:value={alertName} required /></label>
				<label>Webhook URL
					<input type="password" bind:value={webhookUrl} required autocomplete="off" />
				</label>
				<label>Window (s) <input type="number" min="1" bind:value={flushInterval} /></label>
				<label>Lines in embed <input type="number" min="1" bind:value={maxEvents} /></label>
				<label>Buffer cap <input type="number" min="1" bind:value={maxBuffer} /></label>
				<label><input type="checkbox" bind:checked={dedupe} /> Dedupe identical messages</label>
				<label><input type="checkbox" bind:checked={alertEnabled} /> Enable</label>
			{/if}
			{#if formError}
				<p class="form-error">{formError}</p>
			{/if}
			<div class="actions">
				<button type="button" class="secondary" onclick={oncancel} disabled={busy}>Cancel</button>
				<button type="submit" data-testid={`submit-${kind}`} disabled={busy}>{submitLabel}</button>
			</div>
		</form>
</div>

<style>
	.popover {
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: 8px;
		padding: 1.15rem 1.2rem 1.1rem;
	}
	h2 {
		margin: 0 0 0.85rem;
		font-size: 1.1rem;
		color: #fff;
	}
	form {
		display: flex;
		flex-direction: column;
		gap: 0.55rem;
	}
	label {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
		font-size: 0.82rem;
		color: var(--muted);
	}
	label:has(input[type='checkbox']) {
		flex-direction: row;
		align-items: center;
	}
	input,
	select {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.5rem 0.6rem;
		border-radius: var(--radius);
	}
	.form-error {
		margin: 0;
		color: var(--err);
		font-size: 0.78rem;
	}
	.actions {
		display: flex;
		justify-content: flex-end;
		gap: 0.5rem;
		margin-top: 0.4rem;
	}
</style>
