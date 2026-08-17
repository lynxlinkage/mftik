<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import { api, type RegistryRemote, type RegistryStrategy } from '$lib/api';

	let published = $state<RegistryStrategy[]>([]);
	let privateStrategies = $state<RegistryStrategy[]>([]);
	let remotes = $state<RegistryRemote[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);
	let connectOpen = $state(false);
	let name = $state('');
	let url = $state('');
	let token = $state('');
	let connectError = $state<string | null>(null);
	let removing = $state<string | null>(null);
	let blocked = $state<{ name: string; message: string } | null>(null);

	const canConnect = $derived(!busy && !!name.trim() && !!url.trim());

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [listed, hidden, peers] = await Promise.all([
				api.registryStrategies(),
				api.registryPrivate(),
				api.registryRemotes()
			]);
			published = listed.strategies;
			privateStrategies = hidden.strategies;
			remotes = peers.remotes;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	function openConnect() {
		connectOpen = true;
		name = '';
		url = '';
		token = '';
		connectError = null;
	}

	function closeConnect() {
		if (!busy) connectOpen = false;
	}

	async function connect() {
		busy = true;
		connectError = null;
		try {
			const result = await api.connectRegistry({
				name: name.trim(),
				url: url.trim(),
				token: token.trim() || undefined
			});
			connectOpen = false;
			name = '';
			url = '';
			token = '';
			await refresh();
			await goto(`/registry/${result.name}`);
		} catch (e) {
			connectError = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function onConnectKey(event: KeyboardEvent) {
		if (event.key === 'Escape') closeConnect();
	}

	async function disconnect(remoteName: string) {
		removing = remoteName;
		error = null;
		try {
			await api.disconnectRegistry(remoteName);
			remotes = remotes.filter((r) => r.name !== remoteName);
		} catch (e) {
			blocked = {
				name: remoteName,
				message: e instanceof Error ? e.message : String(e)
			};
		} finally {
			removing = null;
		}
	}

	function closeBlocked() {
		blocked = null;
	}

	function onBlockedKey(event: KeyboardEvent) {
		if (event.key === 'Escape') closeBlocked();
	}

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>Registry</h1>
		<p>Public strategies other nodes can pull, private ones that stay here, and copies from connected nodes.</p>
	</div>
	<div class="head-actions">
		<button type="button" class="secondary" onclick={refresh} disabled={loading}>
			{loading ? 'Loading…' : 'Refresh'}
		</button>
		<button type="button" onclick={openConnect}>Connect</button>
	</div>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<div class="cards">
	<a class="card" href="/registry/public">
		<header>
			<span class="title">public</span>
			<span class="badge live">this node</span>
		</header>
		<div class="figure">
			<span class="count">{published.length}</span>
			<span class="unit">strategies</span>
		</div>
		<footer>
			<span class="go">View strategies →</span>
		</footer>
	</a>
	<a class="card" href="/registry/private">
		<header>
			<span class="title">private</span>
			<span class="badge live">this node</span>
		</header>
		<div class="figure">
			<span class="count">{privateStrategies.length}</span>
			<span class="unit">strategies</span>
		</div>
		<footer>
			<span class="go">View strategies →</span>
		</footer>
	</a>
	{#each remotes as remote (remote.name)}
		<article class="card">
			<button
				type="button"
				class="dismiss ghost"
				aria-label={`Disconnect ${remote.name}`}
				disabled={removing === remote.name}
				onclick={() => disconnect(remote.name)}
			>
				<svg width="11" height="11" viewBox="0 0 12 12" aria-hidden="true">
					<path
						d="M2 2l8 8M10 2L2 10"
						fill="none"
						stroke="currentColor"
						stroke-width="1.5"
						stroke-linecap="square"
					/>
				</svg>
			</button>
			<a href={`/registry/${remote.name}`}>
				<header>
					<span class="title">{remote.name}</span>
					<span class="badge live">connected</span>
					{#if remote.authenticated}
						<span class="badge" title="A registry key is stored for this peer">keyed</span>
					{/if}
				</header>
				<div class="figure">
					<span class="count">{remote.count}</span>
					<span class="unit">strategies</span>
				</div>
				<p class="url" title={remote.url}>{remote.url}</p>
				<footer>
					<span class="go">View strategies →</span>
				</footer>
			</a>
		</article>
	{/each}
</div>

{#if connectOpen}
	<div
		class="backdrop"
		role="presentation"
		onclick={closeConnect}
		onkeydown={onConnectKey}
	>
		<div
			class="modal"
			role="dialog"
			aria-modal="true"
			aria-labelledby="connect-title"
			tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => e.stopPropagation()}
		>
			<h2 id="connect-title">Connect</h2>
			<p class="hint">Name this peer and the URL of its public registry.</p>
			<label>
				Name
				<input bind:value={name} disabled={busy} placeholder="node1" />
			</label>
			<label>
				URL
				<input
					bind:value={url}
					disabled={busy}
					placeholder="http://host.docker.internal:8000"
				/>
			</label>
			<label>
				Registry key <span class="optional">optional</span>
				<input
					bind:value={token}
					disabled={busy}
					placeholder="mftik_rk_…"
					autocomplete="off"
				/>
			</label>
			<p class="hint">
				A peer that does not publish openly issues you one of these. It is kept with
				the remote and sent on every pull from it — leave it blank for a peer that
				needs none.
			</p>
			{#if connectError}
				<p class="err">{connectError}</p>
			{/if}
			<div class="modal-actions">
				<button type="button" class="secondary" onclick={closeConnect} disabled={busy}>
					Cancel
				</button>
				<button type="button" onclick={connect} disabled={!canConnect}>
					{busy ? 'Connecting…' : 'Connect'}
				</button>
			</div>
		</div>
	</div>
{/if}

{#if blocked}
	<div
		class="backdrop"
		role="presentation"
		onclick={closeBlocked}
		onkeydown={onBlockedKey}
	>
		<div
			class="modal"
			role="dialog"
			aria-modal="true"
			aria-labelledby="blocked-title"
			tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => e.stopPropagation()}
		>
			<h2 id="blocked-title">Cannot disconnect {blocked.name}</h2>
			<p class="hint">{blocked.message}</p>
			<div class="modal-actions">
				<button type="button" onclick={closeBlocked}>OK</button>
			</div>
		</div>
	</div>
{/if}

<style>
	.head-actions {
		display: flex;
		align-items: center;
		gap: 0.6rem;
	}

	.cards {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(17rem, 1fr));
		gap: 1rem;
		margin-bottom: 1rem;
	}

	.card {
		position: relative;
		display: flex;
		flex-direction: column;
		gap: 0.85rem;
		padding: 1rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		background: linear-gradient(180deg, rgba(24, 32, 43, 0.9), rgba(18, 24, 32, 0.85));
		color: inherit;
		text-decoration: none;
	}

	.card > a {
		display: flex;
		flex-direction: column;
		gap: 0.85rem;
		color: inherit;
		text-decoration: none;
		flex: 1;
	}

	.card > a:hover {
		text-decoration: none;
	}

	.dismiss {
		position: absolute;
		top: 0.35rem;
		left: 0.35rem;
		z-index: 1;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 1.45rem;
		height: 1.45rem;
		padding: 0;
		color: var(--muted);
	}

	.dismiss:hover:not(:disabled) {
		color: var(--err);
	}

	.card:has(.dismiss) header {
		padding-left: 1.35rem;
	}

	.card:hover {
		border-color: var(--accent);
		text-decoration: none;
	}

	.card header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.5rem;
	}

	.title {
		font-family: var(--font);
		font-weight: 600;
		letter-spacing: 0.04em;
	}

	.figure {
		display: flex;
		align-items: baseline;
		gap: 0.4rem;
	}

	.count {
		font-family: var(--font);
		font-size: 2rem;
		font-weight: 500;
		line-height: 1;
	}

	.unit {
		color: var(--muted);
		font-size: 0.85rem;
	}

	.url {
		margin: 0;
		color: var(--muted);
		font-family: var(--font);
		font-size: 0.75rem;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.card footer {
		display: flex;
		align-items: center;
		justify-content: flex-end;
		padding-top: 0.6rem;
		border-top: 1px solid var(--border);
		margin-top: auto;
	}

	.go {
		color: var(--muted);
		font-size: 0.85rem;
		font-weight: 600;
	}

	.card:hover .go {
		color: var(--accent);
	}

	.backdrop {
		position: fixed;
		inset: 0;
		z-index: 40;
		background: rgba(4, 8, 14, 0.72);
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 1.5rem;
	}

	.modal {
		width: min(28rem, 100%);
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 1.15rem 1.2rem 1.1rem;
		display: grid;
		gap: 0.85rem;
	}

	.modal h2 {
		margin: 0;
		font-size: 1.1rem;
	}

	.optional {
		color: var(--muted);
		font-size: 0.7rem;
		text-transform: none;
		letter-spacing: 0;
	}

	.hint {
		margin: 0;
		color: var(--muted);
		font-size: 0.82rem;
	}

	label {
		display: grid;
		gap: 0.35rem;
		font-size: 0.8rem;
		color: var(--muted);
	}

	input {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.55rem 0.65rem;
		border-radius: var(--radius);
	}

	.err {
		margin: 0;
		color: var(--err);
		font-size: 0.82rem;
	}

	.modal-actions {
		display: flex;
		justify-content: flex-end;
		gap: 0.5rem;
	}
</style>
