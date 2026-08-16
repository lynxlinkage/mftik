<script lang="ts">
	import { onMount } from 'svelte';
	import {
		api,
		formatTs,
		startOAuth,
		type AuthKey,
		type AuthKeyCreated,
		type Identity
	} from '$lib/api';

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
	let identities = $state<Identity[]>([]);
	let providers = $state<string[]>([]);
	let name = $state('');
	let kind = $state<'api' | 'registry'>('api');
	let minted = $state<AuthKeyCreated | null>(null);
	let copied = $state(false);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);

	async function refresh() {
		loading = true;
		try {
			const [keyList, identityList, status] = await Promise.all([
				api.authKeys(),
				api.authIdentities(),
				api.authStatus()
			]);
			keys = keyList.keys;
			identities = identityList.identities;
			providers = status.providers;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	/** Configured providers this Owner has not attached yet. */
	const connectable = $derived(
		providers.filter(
			(p) => p !== 'password' && !identities.some((i) => i.provider === p)
		)
	);

	async function unlink(identity: Identity) {
		if (busy || identity.id === null) return;
		busy = true;
		error = null;
		try {
			await api.authIdentityUnlink(identity.id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
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
			minted = await api.authKeyCreate(name.trim(), kind);
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
				{#if minted.kind === 'registry'}
					Give it to the node that will pull from this one; it goes in the key field
					of that node's Connect form.
				{/if}
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

<section>
	<h2>Identities</h2>
	<p class="hint">
		Ways of proving you are this instance's owner — not separate accounts. Connecting
		one attaches it to you; an account nobody has connected cannot sign in here at all.
	</p>
	{#if identities.length > 0}
		<table>
			<thead>
				<tr>
					<th>Provider</th>
					<th>Account</th>
					<th>Linked</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each identities as identity (identity.provider + (identity.id ?? ''))}
					<tr>
						<td>{identity.provider}</td>
						<td>{identity.label ?? '—'}</td>
						<td>{identity.linked_at ? formatTs(identity.linked_at) : '—'}</td>
						<td class="right">
							{#if identity.removable}
								<button
									type="button"
									class="secondary"
									onclick={() => unlink(identity)}
									disabled={busy}
								>
									Disconnect
								</button>
							{:else}
								<!-- The password is a column on the user, not a row. There is
								     no way to unlink yourself out of your own instance. -->
								<span class="badge">always available</span>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
	{#if connectable.length > 0}
		<div class="connect">
			{#each connectable as provider (provider)}
				<button type="button" onclick={() => startOAuth(provider, 'connect')}>
					Connect {provider}
				</button>
			{/each}
		</div>
	{/if}
</section>

<section class="mint">
	<h2>New key</h2>
	<p class="hint">
		{#if kind === 'api'}
			Acts as you on every domain route, and on nothing that changes how you log in or
			issues more keys. Name it after whatever will hold it.
		{:else}
			For another node. It reads the strategies this one publishes and is refused
			everywhere else, which is what makes it safe to give away. Name it after the
			peer you are giving it to.
		{/if}
	</p>
	<form onsubmit={mint}>
		<select bind:value={kind} disabled={busy} aria-label="Key kind">
			<option value="api">API</option>
			<option value="registry">Registry</option>
		</select>
		<input
			bind:value={name}
			placeholder={kind === 'api' ? 'ci, laptop, backfill…' : 'node2, alice…'}
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

	.connect {
		display: flex;
		gap: 0.6rem;
		margin-top: 0.9rem;
	}

	.mint form {
		display: flex;
		gap: 0.6rem;
		max-width: 34rem;
	}

	.mint select {
		flex: 0 0 auto;
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
