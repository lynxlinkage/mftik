<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatBytes, formatTs, type ArtifactObject, type Instance } from '$lib/api';

	let instances = $state<Instance[]>([]);
	let instance = $state('');
	let objects = $state<ArtifactObject[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);
	let key = $state('');
	let file = $state<File | null>(null);
	let fileInput = $state<HTMLInputElement | null>(null);

	const canAdd = $derived(!!instance && !!file && !!key.trim() && !busy);

	function message(e: unknown): string {
		return e instanceof Error ? e.message : String(e);
	}

	async function loadInstances() {
		const listed = await api.instances('sts');
		instances = listed.instances;
		if (instances.length === 1) instance = instances[0].name;
	}

	async function refresh() {
		const name = instance;
		if (!name) {
			objects = [];
			loading = false;
			return;
		}
		loading = true;
		error = null;
		try {
			const listed = await api.artifacts(name);
			if (name !== instance) return;
			objects = listed.objects;
		} catch (e) {
			if (name !== instance) return;
			objects = [];
			error = message(e);
		} finally {
			if (name === instance) loading = false;
		}
	}

	function onFile(event: Event) {
		const input = event.target as HTMLInputElement;
		file = input.files?.[0] ?? null;
	}

	async function add() {
		const name = instance;
		const chosen = file;
		const path = key.trim();
		if (!name || !chosen || !path) return;
		if (path === 'sessions' || path.startsWith('sessions/')) {
			error = 'A key under sessions/ belongs to a session. The catalog cannot put it.';
			return;
		}
		busy = true;
		error = null;
		try {
			await api.putArtifact(name, path, chosen);
			key = '';
			file = null;
			if (fileInput) fileInput.value = '';
			await refresh();
		} catch (e) {
			error = message(e);
		} finally {
			busy = false;
		}
	}

	async function remove(row: ArtifactObject) {
		const name = row.instance || instance;
		if (!name) return;
		if (!confirm(`Remove ${row.path} from ${name}?`)) return;
		busy = true;
		error = null;
		try {
			await api.deleteArtifact(name, row.path);
			await refresh();
		} catch (e) {
			error = message(e);
		} finally {
			busy = false;
		}
	}

	onMount(() => {
		void (async () => {
			try {
				await loadInstances();
				await refresh();
			} catch (e) {
				error = message(e);
				loading = false;
			}
		})();
	});
</script>

<div class="page-head">
	<div>
		<h1>Artifact</h1>
		<p>Uploaded objects on one STS. A strategy reads them by path. This page does not list what a session has written.</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading || !instance}>
		Refresh
	</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<section class="panel create">
	<label>
		STS
		<select
			bind:value={instance}
			disabled={busy || instances.length === 0}
			onchange={() => void refresh()}
		>
			{#if instances.length !== 1}
				<option value="">Choose…</option>
			{/if}
			{#each instances as row (row.id)}
				<option value={row.name}>{row.name}</option>
			{/each}
		</select>
	</label>
	<label>
		File
		<input bind:this={fileInput} type="file" disabled={busy || !instance} onchange={onFile} />
	</label>
	<label class="key">
		Key
		<input
			bind:value={key}
			disabled={busy || !instance}
			placeholder="weights/model.pt"
			spellcheck="false"
		/>
	</label>
	<button type="button" onclick={add} disabled={!canAdd}>Add</button>
</section>

<section class="panel">
	{#if !instance}
		<p class="empty-state">
			{instances.length === 0 && !loading ? 'No STS instance is declared.' : 'Choose an STS.'}
		</p>
	{:else if objects.length === 0}
		<p class="empty-state">{loading ? 'Loading…' : `No uploaded objects on ${instance}.`}</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Path</th>
					<th>Size</th>
					<th>Modified</th>
					<th>Digest</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each objects as row (row.path)}
					<tr>
						<td class="path">{row.path}</td>
						<td>{formatBytes(row.size)}</td>
						<td class="muted">{formatTs(row.mtime)}</td>
						<td class="digest" title={row.digest}>{row.digest}</td>
						<td>
							<button type="button" class="danger" disabled={busy} onclick={() => remove(row)}>
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

	label.key {
		flex: 1;
		min-width: 14rem;
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

	.path,
	.digest {
		font-family: var(--font);
		word-break: break-all;
	}
</style>
