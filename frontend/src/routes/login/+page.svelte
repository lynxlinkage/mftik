<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import { api, startOAuth, type AuthStatus } from '$lib/api';
	import { hasBrandMark, providerLabel } from '$lib/brands';
	import BrandMark from '$lib/components/BrandMark.svelte';

	/**
	 * One form, two jobs. An instance with no password yet is claimed here;
	 * afterwards the same fields sign in. `/auth/status` says which, and it is
	 * public precisely so this page can ask before proving anything.
	 *
	 * A passwordless owner is not a missing one — `seed` creates the row so
	 * foreign keys resolve. So "claim" fills that row in rather than making a
	 * second, and there is never more than one person here.
	 */
	let status = $state<AuthStatus | null>(null);
	let username = $state('');
	let password = $state('');
	let error = $state<string | null>(null);
	let busy = $state(false);

	const claiming = $derived(status?.setup_required === true);
	/**
	 * "Nothing to sign in to" — which is true with the gate off, but only
	 * once somebody owns this node. Claiming it is still a real act while the
	 * gate is off: it is the thing you have to do *before* turning the gate
	 * on, because the moment it comes on an unclaimed instance is claimable
	 * by whoever loads this page first.
	 */
	const off = $derived(status?.enabled === false && !claiming);
	// Password is always there and is not a button; the rest are.
	const oauth = $derived((status?.providers ?? []).filter((p) => p !== 'password'));

	onMount(async () => {
		try {
			status = await api.authStatus();
			// Only bounce a signed-in visitor away. An unclaimed instance with
			// the gate off still has something for them to do here.
			if (status.enabled && status.authenticated) {
				await goto('/');
				return;
			}
			if (status.username) username = status.username;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	});

	async function submit(event: SubmitEvent) {
		event.preventDefault();
		if (busy || !status) return;
		busy = true;
		error = null;
		try {
			if (claiming) {
				await api.authSetup(username.trim(), password);
			} else {
				await api.authLogin(username.trim(), password);
			}
			password = '';
			await goto('/');
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}
</script>

<div class="gate">
	{#if off}
		<div class="notice">
			<h1>Authentication is off</h1>
			<p class="lede">
				This instance runs with <code>MFTIK_AUTH_ENABLED=0</code>, so every request already
				acts as the Owner and there is nothing to sign in to. Whatever sits in front of
				it is the gate.
			</p>
			<a href="/">Back to the control panel</a>
		</div>
	{:else}
	<form onsubmit={submit}>
		<h1>{claiming ? 'Claim this instance' : 'Sign in'}</h1>
		<p class="lede">
			{#if status === null}
				Checking this instance…
			{:else if claiming && status.enabled === false}
				Nobody owns this node yet, and the gate is not on — whatever is in front
				of it is doing the deciding for now. Claim it here first: once the gate
				comes on, an unclaimed instance belongs to whoever loads this page next.
			{:else if claiming}
				Nobody owns this node yet. The username and password you set here are the
				ones that will always work, whatever else gets connected later.
			{:else}
				One instance, one owner.
			{/if}
		</p>

		<label>
			<span>Username</span>
			<!-- svelte-ignore a11y_autofocus -->
			<input
				name="username"
				autocomplete="username"
				autofocus
				bind:value={username}
				disabled={status === null || busy}
				required
			/>
		</label>

		<label>
			<span>Password</span>
			<input
				name="password"
				type="password"
				autocomplete={claiming ? 'new-password' : 'current-password'}
				bind:value={password}
				disabled={status === null || busy}
				minlength={claiming ? (status?.min_password_length ?? undefined) : undefined}
				required
			/>
			{#if claiming}
				<small>
					At least {status?.min_password_length} characters. There is no reset that does
					not involve a shell.
				</small>
			{/if}
		</label>

		{#if error}
			<div class="error-banner">{error}</div>
		{/if}

		<button type="submit" disabled={status === null || busy}>
			{#if busy}
				Working…
			{:else if claiming}
				Claim
			{:else}
				Sign in
			{/if}
		</button>

		{#if !claiming && oauth.length > 0}
			<!-- Only ever a shortcut to the same Owner. An account nobody has
			     connected is refused here, which is what stops a stranger
			     signing in with one. -->
			<div class="or"><span>or</span></div>
			{#each oauth as provider (provider)}
				<button
					type="button"
					class="secondary brand"
					disabled={busy}
					onclick={() => startOAuth(provider, 'login')}
				>
					{#if hasBrandMark(provider)}
						<BrandMark name={provider} size={16} />
					{/if}
					Continue with {providerLabel(provider)}
				</button>
			{/each}
		{/if}
	</form>
	{/if}
</div>

<style>
	.gate {
		display: flex;
		justify-content: center;
		padding-top: 4rem;
	}

	form,
	.notice {
		width: min(26rem, 100%);
		display: grid;
		gap: 1rem;
		padding: 1.75rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		background: var(--bg-elevated);
	}

	h1 {
		margin: 0;
	}

	.lede {
		margin: 0;
		color: var(--muted);
		font-size: 0.85rem;
		line-height: 1.5;
	}

	label {
		display: grid;
		gap: 0.35rem;
	}

	label span {
		font-size: 0.78rem;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--muted);
	}

	small {
		color: var(--muted);
		font-size: 0.72rem;
	}

	button {
		margin-top: 0.25rem;
	}

	button.brand {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		gap: 0.5rem;
	}

	.or {
		display: flex;
		align-items: center;
		gap: 0.6rem;
		color: var(--muted);
		font-size: 0.72rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
	}

	.or::before,
	.or::after {
		content: '';
		flex: 1;
		border-top: 1px solid var(--border);
	}
</style>
