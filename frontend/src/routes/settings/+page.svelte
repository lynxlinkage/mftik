<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatTs, type AuthKey, type AuthKeyCreated } from '$lib/api';

	/**
	 * The Owner's account page. Keys today; linked identities join it when
	 * OAuth lands.
	 *
	 * The one thing this page has to get right is the mint. A key is returned
	 * by the call that creates it and never again — the database holds a
	 * SHA-256 — so the reveal is a deliberate stop, not a row appearing in the
	 * table. Get it wrong and you produce keys nobody has, and an owner who
	 * copes by never revoking anything.
	 */
	let keys = $state<AuthKey[]>([]);
	let name = $state('');
	let minted = $state<AuthKeyCreated | null>(null);
	let copied = $state(false);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);

	async function refresh() {
		loading = true;
		try {
			keys = (await api.authKeys()).keys;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	onMount(refresh);

	async function mint(event: SubmitEvent) {
		event.preventDefault();
		if (busy || !name.trim()) return;
		busy = true;
		error = null;
		copied = false;
		try {
			minted = await api.authKeyCreate(name.trim());
			name = '';
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function copy() {
		if (!minted) return;
		try {
			await navigator.clipboard.writeText(minted.token);
			copied = true;
		} catch {
			error = 'Could not reach the clipboard — select the key and copy it by hand.';
		}
	}

	async function revoke(key: AuthKey) {
		if (busy) return;
		busy = true;
		error = null;
		try {
			await api.authKeyRevoke(key.id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}
</script>

<div class="page-head">
	<div>
		<h1>Settings</h1>
		<p>Credentials this instance has issued. One owner, several ways to act as them.</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>
		{loading ? 'Loading…' : 'Refresh'}
	</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

{#if minted}
	<!-- Deliberately loud, and deliberately dismissed by hand: this is the only
	     time this value exists outside the database's hash of it. -->
	<section class="reveal">
		<header>
			<h2>Copy <strong>{minted.name}</strong> now</h2>
			<p>
				This is the only time it will be shown. The server keeps a hash, not the key —
				closing this box loses it for good, and the way back is a new key.
			</p>
		</header>
		<div class="token">
			<code>{minted.token}</code>
			<button type="button" onclick={copy}>{copied ? 'Copied' : 'Copy'}</button>
		</div>
		<button
			type="button"
			class="secondary"
			onclick={() => {
				minted = null;
				copied = false;
			}}
		>
			I have saved it
		</button>
	</section>
{/if}

<section class="mint">
	<h2>New API key</h2>
	<p class="hint">
		Acts as you on every domain route, and on nothing that changes how you log in or
		issues more keys. Name it after whatever will hold it.
	</p>
	<form onsubmit={mint}>
		<input
			bind:value={name}
			placeholder="ci, laptop, backfill…"
			maxlength="64"
			disabled={busy}
			required
		/>
		<button type="submit" disabled={busy || !name.trim()}>
			{busy ? 'Working…' : 'Create'}
		</button>
	</form>
</section>

<section>
	<h2>Keys</h2>
	{#if loading && keys.length === 0}
		<p class="hint">Loading…</p>
	{:else if keys.length === 0}
		<p class="hint">None yet.</p>
	{:else}
		<table>
			<thead>
				<tr>
					<th>Name</th>
					<th>Key</th>
					<th>Kind</th>
					<th>Created</th>
					<th>Last used</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each keys as key (key.id)}
					<tr class:revoked={key.revoked_at !== null}>
						<td>{key.name}</td>
						<td><code>{key.prefix}</code></td>
						<td>{key.kind}</td>
						<td>{formatTs(key.created_at)}</td>
						<td>{key.last_used_at ? formatTs(key.last_used_at) : 'never'}</td>
						<td class="right">
							{#if key.revoked_at !== null}
								<span class="badge">revoked {formatTs(key.revoked_at)}</span>
							{:else}
								<button
									type="button"
									class="secondary"
									onclick={() => revoke(key)}
									disabled={busy}
								>
									Revoke
								</button>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

<style>
	section {
		margin-bottom: 2rem;
	}

	h2 {
		font-size: 1rem;
		margin: 0 0 0.4rem;
	}

	.hint {
		margin: 0 0 0.8rem;
		color: var(--muted);
		font-size: 0.82rem;
		max-width: 46rem;
		line-height: 1.5;
	}

	.reveal {
		border: 1px solid var(--warn);
		border-radius: var(--radius);
		background: rgba(240, 180, 41, 0.07);
		padding: 1.1rem 1.25rem;
	}

	.reveal header p {
		margin: 0.3rem 0 0.9rem;
		color: var(--muted);
		font-size: 0.82rem;
		max-width: 46rem;
		line-height: 1.5;
	}

	.reveal h2 {
		margin: 0;
	}

	.token {
		display: flex;
		gap: 0.6rem;
		align-items: center;
		margin-bottom: 0.9rem;
	}

	.token code {
		flex: 1;
		padding: 0.6rem 0.7rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		background: var(--bg);
		font-family: var(--font);
		font-size: 0.82rem;
		overflow-x: auto;
		white-space: nowrap;
		user-select: all;
	}

	.mint form {
		display: flex;
		gap: 0.6rem;
		max-width: 30rem;
	}

	.mint input {
		flex: 1;
	}

	table {
		width: 100%;
		border-collapse: collapse;
	}

	th,
	td {
		text-align: left;
		padding: 0.55rem 0.7rem;
		border-bottom: 1px solid var(--border);
		font-size: 0.85rem;
	}

	th {
		color: var(--muted);
		font-weight: 500;
		font-size: 0.74rem;
		text-transform: uppercase;
		letter-spacing: 0.06em;
	}

	td code {
		font-family: var(--font);
		color: var(--muted);
	}

	.right {
		text-align: right;
	}

	tr.revoked td {
		opacity: 0.5;
	}

	.badge {
		font-size: 0.72rem;
		color: var(--muted);
	}
</style>
