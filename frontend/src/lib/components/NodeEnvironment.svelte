<script lang="ts">
	import { onMount } from 'svelte';
	import {
		api,
		type Environment,
		type EnvironmentImport,
		type EnvImportRow
	} from '$lib/api';

	let env = $state<Environment | null>(null);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);
	let force = $state(false);
	let broken = $state<string | null>(null);

	let pkgName = $state('');
	let pkgVersion = $state('');
	let pkgDist = $state('');

	let importUrl = $state('');
	let importToken = $state('');
	let preview = $state<EnvironmentImport | null>(null);
	let distEdits = $state<Record<string, string>>({});

	const names = $derived(env ? Object.keys(env.packages).sort() : []);
	const unfixedGuessed = $derived(
		(preview?.added ?? []).some(
			(row) => row.guessed && (distEdits[row.name] ?? row.dist) === row.name
		)
	);
	// Rows the peer published without a pin. Unlike a guessed dist there is
	// nothing to correct here, so this blocks confirm on its own and the fix
	// is a key in the field above.
	const unpinnedAdded = $derived((preview?.added ?? []).filter((row) => !row.pinned));
	// Unpinned rows can only be ``added``: a name this node already has takes
	// the kept branch, so the server's list and the added rows agree.
	const unpinnedNames = $derived((preview?.unpinned ?? []).join(', '));

	function formatBytes(n: number): string {
		if (n < 1024) return `${n} B`;
		if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
		return `${(n / (1024 * 1024)).toFixed(1)} MB`;
	}

	function py(parts: number[]): string {
		return parts.slice(0, 2).join('.');
	}

	async function refresh() {
		loading = true;
		try {
			env = await api.environment();
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		void refresh();
		if (location.hash === '#extras') {
			document.getElementById('extras')?.scrollIntoView();
		}
	});

	async function addPackage(event: SubmitEvent) {
		event.preventDefault();
		if (busy || !pkgName.trim() || !pkgVersion.trim()) return;
		busy = true;
		error = null;
		broken = null;
		try {
			env = await api.upsertEnvironmentPackage({
				name: pkgName.trim(),
				version: pkgVersion.trim(),
				dist: pkgDist.trim() || undefined,
				force
			});
			pkgName = '';
			pkgVersion = '';
			pkgDist = '';
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function removePackage(name: string) {
		if (busy) return;
		busy = true;
		error = null;
		try {
			env = await api.deleteEnvironmentPackage(name, force);
			if (env.broken.length) {
				broken = env.broken
					.map((row) => `${row.origin}::${row.type} needs ${row.requires.join(', ')}`)
					.join('; ');
			} else {
				broken = null;
			}
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function rowDist(row: EnvImportRow): string {
		return distEdits[row.name] ?? row.dist;
	}

	async function previewImport() {
		if (busy || !importUrl.trim()) return;
		busy = true;
		error = null;
		try {
			preview = await api.importEnvironment({
				url: importUrl.trim(),
				token: importToken.trim() || undefined
			});
			distEdits = Object.fromEntries(
				preview.added.filter((row) => row.guessed).map((row) => [row.name, row.dist])
			);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
			preview = null;
		} finally {
			busy = false;
		}
	}

	async function confirmImport() {
		if (busy || !importUrl.trim() || !preview) return;
		busy = true;
		error = null;
		try {
			const out = await api.importEnvironment({
				url: importUrl.trim(),
				token: importToken.trim() || undefined,
				confirm: true,
				force,
				dist: distEdits
			});
			if (out.environment) env = out.environment;
			preview = out;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}
</script>

<section id="extras">
	<h2>Node extras</h2>
	<p class="hint">
		Third-party packages this node has applied. A strategy may declare them in
		<code>requires</code>; they live on the data volume, not in the image.
	</p>

	{#if error}
		<p class="err">{error}</p>
	{/if}

	{#if env && !env.abi_ok}
		<p class="banner warn">
			The stamp was built for Python {py(env.python)} / {env.platform}, but this
			process is {py(env.runtime_python)} / {env.runtime_platform}. Re-apply extras
			against this interpreter — until then deploys treat the overlay as empty.
		</p>
	{/if}
	{#if env?.restart_required}
		<p class="banner warn">
			STS is still on an older generation. Restart the STS container so it picks up
			the stamp.
		</p>
	{/if}
	{#if env?.load_error}
		<p class="banner warn">{env.load_error}</p>
	{/if}
	{#if broken}
		<p class="banner warn">These stored trees now need extras this node no longer has: {broken}</p>
	{/if}

	{#if loading && !env}
		<p class="hint">Loading…</p>
	{:else if env}
		<p class="meta">
			generation {env.generation} · {formatBytes(env.bytes)} · {py(env.python)} /
			{env.platform}
		</p>
		{#if names.length === 0}
			<p class="hint">None yet. The node is the image: stdlib and the SDK.</p>
		{:else}
			<table>
				<thead>
					<tr>
						<th>Import</th>
						<th>Version</th>
						<th>Dist</th>
						<th>Source</th>
						<th></th>
					</tr>
				</thead>
				<tbody>
					{#each names as name (name)}
						<tr>
							<td><code>{name}</code></td>
							<td>{env.packages[name].version}</td>
							<td><code>{env.packages[name].dist}</code></td>
							<td>{env.packages[name].source}</td>
							<td class="right">
								<button
									type="button"
									class="secondary"
									onclick={() => removePackage(name)}
									disabled={busy}
								>
									Remove
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/if}

	<form class="add" onsubmit={addPackage}>
		<input bind:value={pkgName} placeholder="numpy" disabled={busy} required />
		<input bind:value={pkgVersion} placeholder="2.2.1" disabled={busy} required />
		<input bind:value={pkgDist} placeholder="dist (if different)" disabled={busy} />
		<button type="submit" disabled={busy || !pkgName.trim() || !pkgVersion.trim()}>
			{busy ? 'Working…' : 'Add'}
		</button>
	</form>
	<label class="force">
		<input type="checkbox" bind:checked={force} disabled={busy} />
		Force changes while sessions are live
	</label>

	<h3>Import from a peer</h3>
	<p class="hint">
		Preview first. Confirm is what installs. A row whose dist was guessed from
		the import name must be corrected before confirm — <code>sklearn</code> is not
		on PyPI. A peer with its auth gate on publishes names without versions
		unless you send a registry key it issued you.
	</p>
	<div class="import">
		<input bind:value={importUrl} placeholder="http://peer:8000" disabled={busy} />
		<input
			bind:value={importToken}
			placeholder="registry key (optional)"
			disabled={busy}
			autocomplete="off"
		/>
		<button type="button" class="secondary" onclick={previewImport} disabled={busy || !importUrl.trim()}>
			Preview
		</button>
	</div>

	{#if unpinnedNames}
		<p class="banner warn">
			This peer published names without versions: <code>{unpinnedNames}</code>. Its
			<code>/info</code> keeps pins for authenticated callers, so ask that node for a
			registry key and preview again with it in the field above. Nothing on this page
			can supply the version — a dist is not what is missing.
		</p>
	{/if}

	{#if preview}
		<table>
			<thead>
				<tr>
					<th>Name</th>
					<th>Status</th>
					<th>Version</th>
					<th>Dist</th>
				</tr>
			</thead>
			<tbody>
				{#each [...preview.added, ...preview.kept, ...preview.conflicts] as row (row.name + row.status)}
					<tr>
						<td><code>{row.name}</code></td>
						<td>
							{row.status}
							{#if !row.pinned}
								<span class="badge failed">no version</span>
							{/if}
							{#if row.guessed}
								<span class="badge paused">guessed dist</span>
							{/if}
							{#if row.status === 'conflict'}
								<span class="badge failed">local {row.local_version}</span>
							{/if}
						</td>
						<td>{row.pinned ? row.version : '—'}</td>
						<td>
							{#if row.status === 'added' && row.guessed}
								<input
									value={rowDist(row)}
									oninput={(e) => {
										distEdits = { ...distEdits, [row.name]: e.currentTarget.value };
									}}
									disabled={busy}
								/>
							{:else}
								<code>{row.dist}</code>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
		<div class="import-actions">
			<button
				type="button"
				onclick={confirmImport}
				disabled={busy ||
					preview.conflicts.length > 0 ||
					preview.applied ||
					unfixedGuessed ||
					unpinnedAdded.length > 0}
			>
				{preview.applied ? 'Applied' : busy ? 'Applying…' : 'Confirm'}
			</button>
			{#if preview.conflicts.length > 0}
				<p class="hint">Resolve pin clashes on this node before confirming.</p>
			{/if}
		</div>
	{/if}
</section>

<style>
	section {
		margin-bottom: 2rem;
	}

	h2,
	h3 {
		font-size: 1rem;
		margin: 0 0 0.4rem;
	}

	h3 {
		margin-top: 1.4rem;
	}

	.hint {
		margin: 0 0 0.8rem;
		color: var(--muted);
		font-size: 0.82rem;
		max-width: 46rem;
		line-height: 1.5;
	}

	.meta {
		margin: 0 0 0.8rem;
		color: var(--muted);
		font-size: 0.82rem;
	}

	.err {
		color: var(--err);
		font-size: 0.85rem;
	}

	.banner {
		margin: 0 0 0.8rem;
		padding: 0.65rem 0.8rem;
		border-radius: var(--radius);
		font-size: 0.82rem;
		line-height: 1.45;
		max-width: 46rem;
	}

	.banner.warn {
		border: 1px solid var(--warn);
		background: rgba(240, 180, 41, 0.07);
		color: var(--text);
	}

	.add,
	.import {
		display: flex;
		gap: 0.6rem;
		max-width: 40rem;
		margin: 0.9rem 0 0.6rem;
		flex-wrap: wrap;
	}

	.add input,
	.import input {
		flex: 1;
		min-width: 7rem;
	}

	.force {
		display: flex;
		align-items: center;
		gap: 0.45rem;
		color: var(--muted);
		font-size: 0.82rem;
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

	.import-actions {
		display: flex;
		align-items: center;
		gap: 0.8rem;
		margin-top: 0.8rem;
	}

	.import-actions .hint {
		margin: 0;
	}
</style>
