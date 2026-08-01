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

	function healthLabel(h: boolean | null): string {
		if (h === true) return 'up';
		if (h === false) return 'down';
		return 'n/a';
	}
</script>

<div class="page-head">
	<div>
		<h1>Home</h1>
		<p>Live and historical session counts across STS, TD, and MD.</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>
		{loading ? 'Loading…' : 'Refresh'}
	</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<div class="stats">
	{#each domains as d (d.domain)}
		<a class="stat" href={`/${d.domain}`} data-sveltekit-preload-data="hover">
			<header>
				<span class="domain">{d.domain}</span>
				<span class="badge" class:live={d.healthy === true} class:down={d.healthy === false}>
					{healthLabel(d.healthy)}
				</span>
			</header>
			<div class="nums">
				<div>
					<span class="n">{d.live}</span>
					<span class="l">live</span>
				</div>
				<div>
					<span class="n muted-n">{d.done}</span>
					<span class="l">history</span>
				</div>
			</div>
		</a>
	{:else}
		{#if !loading && !error}
			<p class="empty-state">No domain stats yet.</p>
		{/if}
	{/each}
</div>

<style>
	.stats {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
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
		transition:
			border-color 180ms ease,
			transform 180ms ease,
			box-shadow 180ms ease;
	}

	.stat:hover {
		border-color: rgba(61, 156, 240, 0.45);
		transform: translateY(-2px);
		box-shadow: 0 10px 28px rgba(0, 0, 0, 0.25);
		text-decoration: none;
	}

	header {
		display: flex;
		justify-content: space-between;
		align-items: center;
	}

	.domain {
		font-family: var(--font);
		font-size: 1.1rem;
		letter-spacing: 0.16em;
		text-transform: uppercase;
	}

	.nums {
		display: grid;
		grid-template-columns: 1fr 1fr;
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

	.l {
		display: block;
		margin-top: 0.35rem;
		color: var(--muted);
		font-size: 0.78rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
	}

	@media (max-width: 900px) {
		.stats {
			grid-template-columns: 1fr;
		}
	}
</style>
