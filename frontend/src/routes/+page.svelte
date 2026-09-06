<script lang="ts">
	import { onMount } from 'svelte';
	import { api, type DomainStats } from '$lib/api';

	let domains = $state<DomainStats[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const res = await api.stats();
			domains = res.domains;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	onMount(refresh);

	/**
	 * One group per plane, in a fixed order so the page does not reshuffle as
	 * instances are declared and retired.
	 */
	const PLANES = ['sts', 'td', 'md'];

	let planes = $derived(
		PLANES.map((domain) => ({
			domain,
			rows: domains.filter((d) => d.domain === domain)
		})).filter((group) => group.rows.length > 0)
	);

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
		<h2>{group.domain}</h2>
		<div class="stats">
			{#each group.rows as d (d.instance ?? d.domain)}
				{#if d.domain === 'sts'}
					<a class="stat" href="/strategy" data-sveltekit-preload-data="hover">
						{@render card(d)}
					</a>
				{:else}
					<div class="stat">
						{@render card(d)}
					</div>
				{/if}
			{/each}
		</div>
	</section>
{:else}
	{#if !loading && !error}
		<p class="empty-state">No instances declared yet.</p>
	{/if}
{/each}

{#snippet card(d: DomainStats)}
	<header>
		<span class="domain">{d.instance ?? d.domain}</span>
		<span
			class="badge"
			class:live={d.state === 'connected' && d.enabled}
			class:down={d.state !== 'connected'}
			class:draining={!d.enabled}
		>
			{stateLabel(d)}
		</span>
	</header>
	{#if d.region || d.version}
		<div class="meta">
			{#if d.region}<span>{d.region}</span>{/if}
			{#if d.version}<span class="muted-n">{d.version}</span>{/if}
		</div>
	{/if}
	<!-- Session counts belong to the plane, not to one of its processes, so
	     they ride the first instance of each plane and the rest show none.
	     Repeating them per card would claim a split the tables do not record. -->
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
{/snippet}

<style>
	.plane + .plane {
		margin-top: 1.5rem;
	}

	.plane h2 {
		margin: 0 0 0.6rem;
		font-family: var(--font);
		font-size: 0.82rem;
		letter-spacing: 0.16em;
		text-transform: uppercase;
		color: var(--muted);
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
		text-decoration: none;
		align-content: start;
		transition:
			border-color 180ms ease,
			transform 180ms ease,
			box-shadow 180ms ease;
	}

	a.stat:hover {
		border-color: rgba(61, 156, 240, 0.45);
		transform: translateY(-2px);
		box-shadow: 0 10px 28px rgba(0, 0, 0, 0.25);
		text-decoration: none;
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
	}

	.meta {
		display: flex;
		gap: 0.6rem;
		margin-top: -0.75rem;
		font-size: 0.78rem;
		color: var(--muted);
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
</style>
