<script lang="ts">
	import { onMount, untrack } from 'svelte';
	import { api, formatTs, type ApiCredential, type Venue } from '$lib/api';
	import { maskApiKey } from '$lib/mask';

	let rows = $state<ApiCredential[]>([]);
	let venues = $state<Venue[]>([]);
	// Instruments the symbol plane holds per venue. Advisory only — sym being
	// down must not block registering a credential.
	let symCounts = $state<Record<string, number>>({});
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);

	let name = $state('');
	let venue = $state('');
	let apiKey = $state('');
	let apiSecret = $state('');
	let type = $state('HMAC');
	let passphrase = $state('');

	/** Inline rename of the account column (double-click). */
	let editingId = $state<number | null>(null);
	let editingName = $state('');
	let renameInput = $state<HTMLInputElement | null>(null);

	const selected = $derived(venues.find((v) => v.name === venue) ?? null);
	const types = $derived(selected?.api_types ?? []);
	const needsPassphrase = $derived(selected?.requires_passphrase ?? false);
	const canSubmit = $derived(
		!busy &&
			!!venue &&
			!!name.trim() &&
			!!apiKey.trim() &&
			!!apiSecret &&
			(!needsPassphrase || !!passphrase.trim())
	);

	// Venues differ in which algorithms they sign with, so a type carried over
	// from a previous selection can be one this venue would reject with a 400.
	// Clamp it here rather than letting the form submit something invalid.
	$effect(() => {
		const allowed = types;
		untrack(() => {
			if (!allowed.includes(type)) type = allowed[0] ?? 'HMAC';
			if (!needsPassphrase) passphrase = '';
		});
	});

	async function loadVenues() {
		const res = await api.venues();
		venues = res.venues;
		// Keep the selection valid across reloads; default to the first venue.
		if (!venues.some((v) => v.name === venue)) {
			venue = venues[0]?.name ?? '';
		}
	}

	async function loadSymCounts() {
		try {
			const res = await api.symVenues();
			symCounts = res.counts;
		} catch {
			symCounts = {};
		}
	}

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [res] = await Promise.all([api.apis(), loadVenues()]);
			rows = res.apis;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
		await loadSymCounts();
	}

	async function create() {
		busy = true;
		error = null;
		try {
			await api.createApi({
				name: name.trim(),
				venue,
				api_key: apiKey.trim(),
				api_secret: apiSecret,
				type,
				passphrase: needsPassphrase ? passphrase.trim() : undefined
			});
			name = '';
			apiKey = '';
			apiSecret = '';
			passphrase = '';
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function remove(row: ApiCredential) {
		if (!confirm(`Delete API ${row.id} (${row.name})? This also removes its account.`)) {
			return;
		}
		busy = true;
		error = null;
		try {
			await api.deleteApi(row.id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function startRename(row: ApiCredential) {
		if (busy) return;
		editingId = row.id;
		editingName = row.name;
		queueMicrotask(() => {
			renameInput?.focus();
			renameInput?.select();
		});
	}

	function cancelRename() {
		editingId = null;
	}

	async function commitRename(row: ApiCredential) {
		if (editingId !== row.id) return;
		const next = editingName.trim();
		editingId = null;
		if (!next || next === row.name) return;
		busy = true;
		error = null;
		try {
			await api.renameApi(row.id, next);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>API Key</h1>
		<p>
			Venue credentials bound 1-1 to trading accounts. Reference the account
			<strong>name</strong> in strategy.yml <code>td</code> (resolved to api_id on deploy).
		</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<section class="panel create">
	<label>
		Account name
		<input bind:value={name} disabled={busy} placeholder="paper trader" />
	</label>
	<label>
		Venue
		<select bind:value={venue} disabled={busy || venues.length === 0}>
			{#each venues as v (v.name)}
				<option value={v.name}>{v.label}{v.simulated ? ' (simulated)' : ''}</option>
			{/each}
		</select>
	</label>
	<label>
		Type
		<select bind:value={type} disabled={busy || types.length < 2}>
			{#each types as t}
				<option value={t}>{t}</option>
			{/each}
		</select>
	</label>
	<label>
		API key
		<input bind:value={apiKey} disabled={busy} placeholder="paper-key-3" autocomplete="off" />
	</label>
	<label>
		API secret
		<input
			bind:value={apiSecret}
			disabled={busy}
			type="password"
			placeholder="secret"
			autocomplete="new-password"
		/>
	</label>
	{#if needsPassphrase}
		<label>
			Passphrase
			<input
				bind:value={passphrase}
				disabled={busy}
				type="password"
				placeholder="required"
				autocomplete="new-password"
			/>
		</label>
	{/if}
	<button type="button" onclick={create} disabled={!canSubmit}>Add</button>
	{#if selected}
		<p class="venue-hint">
			<code>{selected.name}</code>
			· signs with {selected.api_types.join(' / ')}
			{#if selected.ticker_example}
				· tickers like <code>{selected.ticker_example}</code>
			{/if}
			{#if symCounts[selected.name] != null}
				· {symCounts[selected.name]} tracked by sym
			{/if}
		</p>
	{/if}
</section>

<section class="panel table-wrap">
	{#if rows.length === 0}
		<p class="empty-state">{loading ? 'Loading…' : 'No API keys yet.'}</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>API ID</th>
					<th>Account</th>
					<th>Venue</th>
					<th>Key</th>
					<th>Type</th>
					<th>Created</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each rows as row (row.id)}
					<tr>
						<td>
							<a href={`/td/${row.id}`}>{row.id}</a>
						</td>
						<td>
							{#if editingId === row.id}
								<input
									class="rename-input"
									bind:this={renameInput}
									bind:value={editingName}
									disabled={busy}
									aria-label="Rename account"
									onkeydown={(e) => {
										if (e.key === 'Enter') {
											e.preventDefault();
											void commitRename(row);
										} else if (e.key === 'Escape') {
											e.preventDefault();
											cancelRename();
										}
									}}
									onblur={() => void commitRename(row)}
								/>
							{:else}
								<span
									class="renameable"
									title={`account_id=${row.account_id} — double-click to rename`}
									ondblclick={() => startRename(row)}
								>
									{row.name}
								</span>
							{/if}
						</td>
						<td><code>{row.venue}</code></td>
						<td><code>{maskApiKey(row.api_key)}</code></td>
						<td>{row.type}</td>
						<td class="muted">{formatTs(row.created_at)}</td>
						<td>
							<button
								type="button"
								class="danger"
								disabled={busy}
								onclick={() => remove(row)}
							>
								Delete
							</button>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

<style>
	.create {
		display: flex;
		flex-wrap: wrap;
		align-items: end;
		gap: 0.85rem;
		margin-bottom: 1rem;
	}

	label {
		display: grid;
		gap: 0.35rem;
		font-size: 0.8rem;
		color: var(--muted);
		min-width: 9rem;
	}

	input,
	select {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.55rem 0.65rem;
		border-radius: var(--radius);
		min-width: 10rem;
	}

	.venue-hint {
		flex-basis: 100%;
		margin: 0;
		font-size: 0.78rem;
		color: var(--muted);
	}

	.table-wrap {
		overflow-x: auto;
	}

	code {
		font-family: var(--font);
		font-size: 0.82rem;
	}

	.renameable {
		cursor: text;
		border-bottom: 1px dashed transparent;
	}

	.renameable:hover {
		border-bottom-color: var(--muted);
	}

	.rename-input {
		min-width: 8rem;
		width: 100%;
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.25rem 0.4rem;
		border-radius: var(--radius);
		font: inherit;
	}
</style>
