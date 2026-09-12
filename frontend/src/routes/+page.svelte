<script lang="ts">
	import { onMount } from 'svelte';
	import { api, type DomainStats } from '$lib/api';

	const PLANES = ['sts', 'td', 'md'] as const;
	type Plane = (typeof PLANES)[number];
	type Draft = { name: string; region: string };

	const emptyDraft = (): Draft => ({ name: '', region: '' });

	/** Same rule as ``validate_instance_name``: one Redis subject segment. */
	const INSTANCE_NAME = /^[a-z][a-z0-9-]{0,63}$/;

	function legalName(name: string): boolean {
		return INSTANCE_NAME.test(name);
	}

	function errMsg(e: unknown): string {
		return e instanceof Error ? e.message : String(e);
	}

	let domains = $state<DomainStats[]>([]);
	/** `/stats` has no id; PATCH/DELETE do. Joined on the unique instance name. */
	let idByName = $state<Record<string, number>>({});
	let drafts = $state<Record<Plane, Draft>>({
		sts: emptyDraft(),
		td: emptyDraft(),
		md: emptyDraft()
	});
	let error = $state<string | null>(null);
	let loading = $state(true);
	let busy = $state(false);

	let editingId = $state<number | null>(null);
	let editingRegion = $state('');
	let regionInput = $state<HTMLInputElement | null>(null);

	async function refresh() {
		loading = true;
		error = null;
		const [statsSettled, instSettled] = await Promise.allSettled([
			api.stats(),
			api.instances()
		]);
		if (statsSettled.status === 'fulfilled') {
			domains = statsSettled.value.domains;
		} else {
			error = errMsg(statsSettled.reason);
		}
		if (instSettled.status === 'fulfilled') {
			idByName = Object.fromEntries(
				instSettled.value.instances.map((i) => [i.name, i.id])
			);
		} else {
			// Health still renders. Cards without an id lose their buttons.
			idByName = {};
			error ??= errMsg(instSettled.reason);
		}
		loading = false;
	}

	onMount(refresh);

	/**
	 * One group per plane, in a fixed order so the page does not reshuffle as
	 * instances are declared and retired. Empty groups stay: retiring the last
	 * row of a plane must not take the declare form with it.
	 */
	let planes = $derived(
		PLANES.map((domain) => ({
			domain,
			rows: domains.filter((d) => d.domain === domain)
		}))
	);

	function rowName(d: DomainStats): string {
		return d.instance ?? d.domain;
	}

	function rowId(d: DomainStats): number | undefined {
		return idByName[rowName(d)];
	}

	/**
	 * `down` is not `!healthy`. It says a row declared this instance and
	 * nothing answered — a machine to go and look at, which is exactly what
	 * would be invisible if a dead plane simply vanished from this page.
	 */
	function stateLabel(d: DomainStats): string {
		if (!d.enabled) return 'draining';
		return d.state === 'connected' ? 'up' : 'down';
	}

	function sessionCounts(d: DomainStats): boolean {
		return d.live > 0 || d.done > 0 || d.failed > 0 || d.interrupted > 0 || d.ack > 0;
	}

	function locked(): boolean {
		return busy || loading;
	}

	async function declareInstance(domain: Plane) {
		const name = drafts[domain].name.trim();
		const region = drafts[domain].region.trim();
		if (!legalName(name) || locked()) return;
		busy = true;
		error = null;
		try {
			await api.createInstance({
				name,
				domain,
				...(region ? { region } : {})
			});
			drafts[domain] = emptyDraft();
			await refresh();
		} catch (e) {
			error = errMsg(e);
		} finally {
			busy = false;
		}
	}

	function startAnnotate(id: number, region: string | null) {
		if (locked()) return;
		editingId = id;
		editingRegion = region ?? '';
		queueMicrotask(() => {
			regionInput?.focus();
			regionInput?.select();
		});
	}

	function cancelAnnotate() {
		editingId = null;
	}

	async function persistRegion(id: number, current: string | null): Promise<void> {
		if (editingId !== id) return;
		const next = editingRegion.trim();
		editingId = null;
		if (next === (current ?? '').trim()) return;
		await api.patchInstance(id, { region: next });
	}

	async function commitAnnotate(id: number, current: string | null) {
		if (editingId !== id) return;
		if (locked()) return;
		const next = editingRegion.trim();
		editingId = null;
		if (next === (current ?? '').trim()) return;
		busy = true;
		error = null;
		try {
			await api.patchInstance(id, { region: next });
			await refresh();
		} catch (e) {
			error = errMsg(e);
		} finally {
			busy = false;
		}
	}

	function regionFocusOut(event: FocusEvent, id: number, current: string | null) {
		// A click on Drain/Retire fires blur first. If we take the lock here
		// that click lands on locked() and does nothing — no confirm, no
		// request. Let the button own the save.
		const dest = event.relatedTarget;
		if (dest instanceof HTMLElement && dest.closest('.card-actions')) return;
		void commitAnnotate(id, current);
	}

	async function setEnabled(id: number, enabled: boolean, current: string | null) {
		if (locked()) return;
		busy = true;
		error = null;
		try {
			await persistRegion(id, current);
			await api.patchInstance(id, { enabled });
			await refresh();
		} catch (e) {
			error = errMsg(e);
		} finally {
			busy = false;
		}
	}

	async function retire(id: number, name: string) {
		if (locked()) return;
		// An uncommitted region is dropped rather than saved first, unlike
		// Drain. The row is about to stop existing, so the PATCH buys an audit
		// entry for nothing — and a region the API refused would block the
		// retire behind an edit the user has already abandoned.
		if (!confirm(`Retire ${name}? This only removes the declaration — a running process is a deploy.`)) {
			return;
		}
		busy = true;
		error = null;
		try {
			await api.deleteInstance(id);
			await refresh();
		} catch (e) {
			error = errMsg(e);
		} finally {
			busy = false;
		}
	}
</script>

<div class="page-head">
	<div>
		<h1>Home</h1>
		<p>
			Every declared instance, and whether it answered. A row that says <em>down</em> is
			declared and silent — nothing here starts a plane, so making it true is a deploy.
		</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>
		{loading ? 'Loading…' : 'Refresh'}
	</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

{#each planes as group (group.domain)}
	<section class="plane">
		<div class="plane-head">
			<h2>{group.domain}</h2>
			<form
				class="declare"
				onsubmit={(e) => {
					e.preventDefault();
					void declareInstance(group.domain);
				}}
			>
				<input
					bind:value={drafts[group.domain].name}
					disabled={locked()}
					aria-label="Instance name"
					placeholder="name"
					maxlength="64"
					spellcheck="false"
					autocapitalize="off"
					autocomplete="off"
					pattern={"[a-z][a-z0-9-]{0,63}"}
					title="lowercase letters, digits and hyphens — this is MFTIK_INSTANCE and there is no rename"
				/>
				<input
					bind:value={drafts[group.domain].region}
					disabled={locked()}
					aria-label="Declare region"
					placeholder="region"
					maxlength="64"
				/>
				<button type="submit" disabled={locked() || !legalName(drafts[group.domain].name.trim())}>
					Declare
				</button>
			</form>
		</div>
		{#if group.rows.length > 0}
			<div class="stats">
				{#each group.rows as d (rowName(d))}
					<div class="stat">
						{@render card(d)}
					</div>
				{/each}
			</div>
		{:else if !loading && !error}
			<p class="plane-empty">No instances declared.</p>
		{/if}
	</section>
{/each}

{#snippet card(d: DomainStats)}
	{@const id = rowId(d)}
	{@const name = rowName(d)}
	<header>
		{#if d.domain === 'sts'}
			<a class="domain" href="/strategy" data-sveltekit-preload-data="hover">{name}</a>
		{:else}
			<span class="domain">{name}</span>
		{/if}
		<span
			class="badge"
			class:live={d.state === 'connected' && d.enabled}
			class:down={d.state !== 'connected'}
			class:draining={!d.enabled}
		>
			{stateLabel(d)}
		</span>
	</header>
	{#if id != null || d.region || d.version}
	<div class="meta">
		{#if id != null && editingId === id}
			<input
				class="region-input"
				bind:this={regionInput}
				bind:value={editingRegion}
				disabled={busy}
				aria-label="Annotate region for {name}"
				maxlength="64"
				onkeydown={(e) => {
					if (e.key === 'Enter') {
						e.preventDefault();
						void commitAnnotate(id, d.region);
					} else if (e.key === 'Escape') {
						e.preventDefault();
						cancelAnnotate();
					}
				}}
				onfocusout={(e) => regionFocusOut(e, id, d.region)}
			/>
		{:else if id != null}
			<button
				type="button"
				class="region"
				class:placeholder={!d.region}
				disabled={locked()}
				title="Click to annotate region"
				onclick={() => startAnnotate(id, d.region)}
			>
				{d.region || 'region'}
			</button>
		{:else if d.region}
			<span>{d.region}</span>
		{/if}
		{#if d.version}<span class="muted-n">{d.version}</span>{/if}
	</div>
	{/if}
	<!-- Counts are this instance's. A card with nothing attributed stays
	     blank so a permanent zero does not train people to stop reading. -->
	{#if sessionCounts(d)}
		<div class="nums">
			<div>
				<span class="n">{d.live}</span>
				<span class="l">live</span>
			</div>
			<div>
				<span class="n muted-n">{d.done}</span>
				<span class="l">history</span>
			</div>
			<!-- Only shown when there is something to see: a permanent zero
			     trains people to stop reading it. -->
			{#if d.failed > 0}
				<div>
					<span class="n failed-n">{d.failed}</span>
					<span class="l">failed</span>
				</div>
			{/if}
			{#if d.interrupted > 0}
				<div>
					<span class="n stopped-n">{d.interrupted}</span>
					<span class="l">interrupted</span>
				</div>
			{/if}
			{#if d.ack > 0}
				<div>
					<span class="n muted-n">{d.ack}</span>
					<span class="l">ack</span>
				</div>
			{/if}
		</div>
	{/if}
	{#if id != null}
		<div class="card-actions">
			<button
				type="button"
				class="ghost"
				disabled={locked()}
				onmousedown={(e) => e.preventDefault()}
				onclick={() => void setEnabled(id, !d.enabled, d.region)}
			>
				{d.enabled ? 'Drain' : 'Enable'}
			</button>
			<button
				type="button"
				class="danger"
				disabled={locked()}
				onmousedown={(e) => e.preventDefault()}
				onclick={() => void retire(id, name)}
			>
				Retire
			</button>
		</div>
	{/if}
{/snippet}

<style>
	.plane + .plane {
		margin-top: 1.5rem;
	}

	.plane-head {
		display: flex;
		flex-wrap: wrap;
		align-items: end;
		justify-content: space-between;
		gap: 0.75rem;
		margin-bottom: 0.6rem;
	}

	.plane-head h2 {
		margin: 0;
		font-family: var(--font);
		font-size: 0.82rem;
		letter-spacing: 0.16em;
		text-transform: uppercase;
		color: var(--muted);
	}

	.declare {
		display: flex;
		flex-wrap: wrap;
		align-items: end;
		gap: 0.5rem;
	}

	.declare input {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.4rem 0.55rem;
		border-radius: var(--radius);
		min-width: 8rem;
	}

	.plane-empty {
		margin: 0;
		color: var(--muted);
		font-size: 0.85rem;
	}

	/* Auto-fill rather than a fixed three: the number of instances in a plane
	   is a deployment's business, not this page's. */
	.stats {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
		gap: 1rem;
	}

	.stat {
		display: grid;
		gap: 1.25rem;
		padding: 1.25rem 1.2rem 1.15rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		background:
			linear-gradient(135deg, rgba(61, 156, 240, 0.08), transparent 45%),
			linear-gradient(180deg, rgba(24, 32, 43, 0.95), rgba(14, 18, 26, 0.9));
		color: inherit;
		align-content: start;
	}

	header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: 0.75rem;
	}

	.domain {
		font-family: var(--font);
		font-size: 1.1rem;
		letter-spacing: 0.06em;
		color: inherit;
	}

	a.domain:hover {
		color: var(--accent);
		text-decoration: none;
	}

	.meta {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 0.6rem;
		margin-top: -0.75rem;
		font-size: 0.78rem;
		color: var(--muted);
	}

	.region {
		background: none;
		border: none;
		border-bottom: 1px dashed transparent;
		color: inherit;
		font: inherit;
		font-weight: 400;
		padding: 0;
		border-radius: 0;
		cursor: text;
	}

	.region:hover:not(:disabled) {
		border-bottom-color: var(--muted);
	}

	.region.placeholder {
		color: var(--muted);
		opacity: 0.7;
	}

	.region-input {
		min-width: 8rem;
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.2rem 0.4rem;
		border-radius: var(--radius);
		font: inherit;
	}

	/* Auto-fit rather than a fixed 1fr 1fr: only sts ever shows a third
	   number, and a hard two-column grid would drop it onto its own row and
	   leave that card taller than the others. */
	.nums {
		display: grid;
		grid-auto-flow: column;
		grid-auto-columns: 1fr;
		gap: 0.75rem;
	}

	.n {
		display: block;
		font-family: var(--font);
		font-size: 2rem;
		line-height: 1;
	}

	.muted-n {
		color: var(--muted);
	}

	.failed-n {
		color: var(--err);
	}

	.stopped-n {
		color: var(--warn);
	}

	.l {
		display: block;
		margin-top: 0.35rem;
		color: var(--muted);
		font-size: 0.78rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
	}

	.card-actions {
		display: flex;
		flex-wrap: wrap;
		gap: 0.4rem;
	}

	.card-actions button {
		font-size: 0.78rem;
		padding: 0.3rem 0.55rem;
		font-weight: 500;
	}
</style>
