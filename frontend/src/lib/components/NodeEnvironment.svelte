<script lang="ts">
	import { onMount } from 'svelte';
	import {
		api,
		type Environment,
		type EnvInstalled
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


	const names = $derived(env ? Object.keys(env.packages).sort() : []);
	// What the resolver pulled in that nobody approved. Importable, but a
	// strategy cannot declare it: `requires` is checked against the stamp.
	const dependencies = $derived((env?.installed ?? []).filter((row) => !row.approved));

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
		if (busy || !pkgName.trim()) return;
		busy = true;
		error = null;
		broken = null;
		try {
			env = await api.upsertEnvironmentPackage({
				name: pkgName.trim(),
				// Blank means "resolver, you pick". The node stamps the version it
				// resolved to, so the table below fills in with an exact pin.
				version: pkgVersion.trim() || undefined,
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

	async function approve(row: EnvInstalled) {
		if (busy || !row.suggested_name) return;
		busy = true;
		error = null;
		try {
			// The version already on disk, so the installer has nothing to do
			// and no live session is disturbed. Typing a different one would
			// re-resolve the whole set.
			env = await api.upsertEnvironmentPackage({
				name: row.suggested_name,
				version: row.version,
				dist: row.dist,
				force
			});
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
		Leave the version blank to let the resolver pick — the node records the
		pin it resolved to, and every later change reinstalls from that, so an
		untouched package does not move when you add another one.
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
		<input bind:value={pkgVersion} placeholder="version (optional)" disabled={busy} />
		<input bind:value={pkgDist} placeholder="dist (if different)" disabled={busy} />
		<button type="submit" disabled={busy || !pkgName.trim()}>
			{busy ? 'Working…' : 'Add'}
		</button>
	</form>
	<label class="force">
		<input type="checkbox" bind:checked={force} disabled={busy} />
		Force changes while sessions are live
	</label>

	{#if dependencies.length > 0}
		<h3>Came along as dependencies</h3>
		<p class="hint">
			Installed because something above needs them. They are on
			<code>sys.path</code> and importable, but a strategy cannot put one in
			<code>requires</code> until you approve it — the deploy check reads the
			stamp, not the directory. Approving pins it at the version already here,
			so nothing is reinstalled and no session is disturbed. Most of these are
			nobody's business but the package that asked for them; the column says
			which one that is.
		</p>
		<table>
			<thead>
				<tr>
					<th>Dist</th>
					<th>Version</th>
					<th>Needed by</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each dependencies as row (row.dist)}
					<tr>
						<td><code>{row.dist}</code></td>
						<td>{row.version}</td>
						<td>
							{#if row.needed_by.length}
								{#each row.needed_by as who, i (who)}<code>{who}</code>{i <
									row.needed_by.length - 1
										? ', '
										: ''}{/each}
							{:else}
								<span class="hint">—</span>
							{/if}
						</td>
						<td class="right">
							{#if row.suggested_name}
								<button
									type="button"
									class="secondary"
									onclick={() => approve(row)}
									disabled={busy}
								>
									Approve as <code>{row.suggested_name}</code>
								</button>
							{:else}
								<span class="hint">
									Import name differs from <code>{row.dist}</code> — add it above.
								</span>
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

	.add {
		display: flex;
		gap: 0.6rem;
		max-width: 40rem;
		margin: 0.9rem 0 0.6rem;
		flex-wrap: wrap;
	}

	.add input {
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


</style>
