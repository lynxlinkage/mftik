<script lang="ts">
	import { page } from '$app/state';
	import { api, type RegistryStrategy, type RegistrySyncRow } from '$lib/api';
	import RegistryStrategies from '$lib/components/RegistryStrategies.svelte';

	const name = $derived(page.params.name ?? '');
	const isOwn = $derived(name === 'public' || name === 'private');

	let url = $state('');
	let reachable = $state(true);
	let remoteError = $state<string | null>(null);
	let localStrategies = $state<RegistryStrategy[]>([]);
	let sync = $state<RegistrySyncRow[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);
	let removing = $state<string | null>(null);
	let blocked = $state<{ name: string; message: string } | null>(null);
	let unloadError = $state<string | null>(null);

	$effect(() => {
		const origin = name;
		loading = true;
		error = null;
		remoteError = null;
		unloadError = null;
		if (origin === 'public' || origin === 'private') {
			const fetchOwn =
				origin === 'public' ? api.registryStrategies() : api.registryPrivate();
			fetchOwn
				.then((listed) => {
					if (origin !== name) return;
					localStrategies = listed.strategies;
					sync = [];
					url = '';
					reachable = true;
				})
				.catch((e) => {
					if (origin !== name) return;
					error = e instanceof Error ? e.message : String(e);
					localStrategies = [];
				})
				.finally(() => {
					if (origin === name) loading = false;
				});
			return;
		}
		api
			.registryDiff(origin)
			.then((detail) => {
				if (origin !== name) return;
				url = detail.url;
				reachable = detail.reachable;
				remoteError = detail.error;
				sync = detail.strategies;
				localStrategies = [];
			})
			.catch((e) => {
				if (origin !== name) return;
				error = e instanceof Error ? e.message : String(e);
				sync = [];
				url = '';
			})
			.finally(() => {
				if (origin === name) loading = false;
			});
	});

	async function syncNow() {
		if (!url || isOwn) return;
		const origin = name;
		busy = true;
		error = null;
		try {
			await api.connectRegistry({ name: origin, url });
			if (origin !== name) return;
			const detail = await api.registryDiff(origin);
			if (origin !== name) return;
			sync = detail.strategies;
			reachable = detail.reachable;
			remoteError = detail.error;
		} catch (e) {
			if (origin === name) error = e instanceof Error ? e.message : String(e);
		} finally {
			if (origin === name) busy = false;
		}
	}

	async function removeStrategy(strategyName: string) {
		if (!isOwn || removing) return;
		const origin = name;
		if (origin !== 'public' && origin !== 'private') return;
		removing = strategyName;
		error = null;
		unloadError = null;
		try {
			const out = await api.registryDelete(strategyName, origin);
			if (origin !== name) return;
			localStrategies = localStrategies.filter((s) => s.name !== strategyName);
			if (!out.unloaded) unloadError = out.unload_error;
		} catch (e) {
			if (origin !== name) return;
			blocked = {
				name: strategyName,
				message: e instanceof Error ? e.message : String(e)
			};
		} finally {
			if (origin === name) removing = null;
		}
	}

	function closeBlocked() {
		blocked = null;
	}

	function onBlockedKey(event: KeyboardEvent) {
		if (event.key === 'Escape') closeBlocked();
	}
</script>

<p class="back"><a href="/registry">← Registry</a></p>

<div class="page-head">
	<div>
		<h1>
			{name}
			{#if !isOwn && !loading}
				{#if reachable}
					<span class="badge live">connected</span>
				{:else}
					<span class="badge failed">unreachable</span>
				{/if}
			{/if}
		</h1>
		<p>
			{#if name === 'public'}
				Strategies this node publishes. Other nodes can pull these.
			{:else if name === 'private'}
				Strategies that stay on this node. Other nodes cannot pull these.
			{:else if url}
				<code>{url}</code>
			{:else}
				Strategies pulled from this node.
			{/if}
		</p>
	</div>
	{#if !isOwn}
		<div class="actions">
			<button type="button" onclick={syncNow} disabled={busy || !url || loading}>
				{busy ? 'Syncing…' : 'Sync'}
			</button>
		</div>
	{/if}
</div>

{#if error}
	<div class="error-banner">{error}</div>
{:else if remoteError && !reachable}
	<div class="error-banner">Peer unreachable — showing the local copy. {remoteError}</div>
{/if}
{#if unloadError}
	<div class="error-banner">{unloadError}</div>
{/if}

<section class="panel table-wrap">
	<RegistryStrategies
		origin={name}
		strategies={localStrategies}
		sync={isOwn ? undefined : sync}
		{loading}
		{removing}
		onRemove={isOwn ? removeStrategy : undefined}
		empty={name === 'public'
			? 'No strategies in the public registry yet.'
			: name === 'private'
				? 'No strategies in the private registry yet.'
				: 'No strategies on this node or its local copy.'}
	/>
</section>

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
			<h2 id="blocked-title">Cannot remove {blocked.name}</h2>
			<p class="hint">{blocked.message}</p>
			<div class="modal-actions">
				<button type="button" onclick={closeBlocked}>OK</button>
			</div>
		</div>
	</div>
{/if}

<style>
	.back {
		margin: 0 0 1rem;
		font-size: 0.9rem;
	}

	.table-wrap {
		overflow-x: auto;
	}

	code {
		font-family: var(--font);
		font-size: 0.82rem;
	}

	h1 {
		display: flex;
		align-items: center;
		gap: 0.65rem;
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

	.hint {
		margin: 0;
		color: var(--muted);
		font-size: 0.82rem;
	}

	.modal-actions {
		display: flex;
		justify-content: flex-end;
		gap: 0.5rem;
	}
</style>
