<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatTs, type ApiCredential } from '$lib/api';

	const VENUES = ['paper'] as const;
	const TYPES = ['HMAC', 'ED25519'] as const;

	let rows = $state<ApiCredential[]>([]);
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);

	let name = $state('');
	let venue = $state<(typeof VENUES)[number]>('paper');
	let apiKey = $state('');
	let apiSecret = $state('');
	let type = $state<(typeof TYPES)[number]>('HMAC');
	let passphrase = $state('');

	async function refresh() {
		loading = true;
		error = null;
		try {
			const res = await api.apis();
			rows = res.apis;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
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
				passphrase: passphrase.trim() || undefined
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

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>APIs</h1>
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
		<select bind:value={venue} disabled={busy}>
			{#each VENUES as v}
				<option value={v}>{v}</option>
			{/each}
		</select>
	</label>
	<label>
		Type
		<select bind:value={type} disabled={busy}>
			{#each TYPES as t}
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
	<label>
		Passphrase
		<input bind:value={passphrase} disabled={busy} placeholder="optional" />
	</label>
	<button
		type="button"
		onclick={create}
		disabled={busy || !name.trim() || !apiKey.trim() || !apiSecret}
	>
		Add
	</button>
</section>

<section class="panel table-wrap">
	{#if rows.length === 0}
		<p class="empty-state">{loading ? 'Loading…' : 'No APIs yet.'}</p>
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
							<span title={`account_id=${row.account_id}`}>{row.name}</span>
						</td>
						<td><code>{row.venue}</code></td>
						<td><code>{row.api_key}</code></td>
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

	.table-wrap {
		overflow-x: auto;
	}

	code {
		font-family: var(--font);
		font-size: 0.82rem;
	}
</style>
