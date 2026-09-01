<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatTs, type Audit } from '$lib/api';
	import { hasBrandMark } from '$lib/brands';
	import BrandMark from '$lib/components/BrandMark.svelte';
	import Pager from '$lib/components/Pager.svelte';
	import { pageCountOf } from '$lib/paging';

	const PAGE_SIZE = 50;

	let audits = $state<Audit[]>([]);
	let page = $state(1);
	let total = $state(0);
	/** How far the API will page this list; it says so in every response. */
	let maxOffset = $state<number | undefined>(undefined);
	let error = $state<string | null>(null);
	let loading = $state(true);

	let listEpoch = 0;

	const pageCount = $derived(pageCountOf(total, PAGE_SIZE, maxOffset));

	async function load() {
		const epoch = ++listEpoch;
		let myPage = page;
		loading = true;
		error = null;
		try {
			let offset = Math.max(0, (myPage - 1) * PAGE_SIZE);
			let res = await api.audits({ limit: PAGE_SIZE, offset });
			if (epoch !== listEpoch) return;
			if (offset > 0 && offset >= res.total) {
				myPage = pageCountOf(res.total, PAGE_SIZE, res.max_offset);
				offset = (myPage - 1) * PAGE_SIZE;
				page = myPage;
				res = await api.audits({ limit: PAGE_SIZE, offset });
				if (epoch !== listEpoch) return;
			}
			audits = res.audits;
			total = res.total ?? 0;
			maxOffset = res.max_offset;
		} catch (e) {
			if (epoch !== listEpoch) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (epoch === listEpoch) loading = false;
		}
	}

	function setPage(next: number) {
		if (next === page || next < 1) return;
		page = next;
		audits = [];
		void load();
	}

	onMount(load);

	function formatIdentity(
		via: string | null | undefined,
		keyKind: string | null | undefined
	): string {
		if (via == null || via === '') return '—';
		if (via === 'password') return 'Password';
		if (via === 'discord') return 'Discord';
		if (via === 'google') return 'Google';
		if (via === 'disabled') return 'Auth disabled';
		if (via.startsWith('key:')) {
			const name = via.slice(4);
			if (keyKind === 'api') return `API key · ${name}`;
			if (keyKind === 'registry') return `Registry key · ${name}`;
			return `Key · ${name}`;
		}
		return via;
	}
</script>

<div class="page-head">
	<div>
		<h1>Audit</h1>
		<p>Append-only record of control-plane operations.</p>
	</div>
	<button type="button" class="secondary" onclick={load} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<section class="panel">
	{#if audits.length === 0}
		<p class="empty-state">{loading ? 'Loading…' : 'No audit entries yet.'}</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>When</th>
					<th>Identity</th>
					<th>Operation</th>
					<th>Result</th>
				</tr>
			</thead>
			<tbody>
				{#each audits as a (a.id)}
					<tr>
						<td class="muted">{formatTs(a.created_at)}</td>
						<td>
							<span class="identity">
								{#if a.via && hasBrandMark(a.via)}
									<BrandMark name={a.via} size={14} />
								{/if}
								{formatIdentity(a.via, a.key_kind)}
							</span>
						</td>
						<td><code>{a.operation}</code></td>
						<td class="result">{a.result}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
	<Pager {page} {pageCount} disabled={loading} onchange={setPage} />
</section>

<style>
	.identity {
		display: inline-flex;
		align-items: center;
		gap: 0.4rem;
	}

	code {
		font-family: var(--font);
		font-size: 0.82rem;
	}

	.result {
		font-family: var(--font);
		font-size: 0.8rem;
		color: var(--muted);
		word-break: break-word;
	}
</style>
