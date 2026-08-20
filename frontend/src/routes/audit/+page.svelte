<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatTs, type Audit } from '$lib/api';
	import { hasBrandMark } from '$lib/brands';
	import BrandMark from '$lib/components/BrandMark.svelte';

	let audits = $state<Audit[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const res = await api.audits(200);
			audits = res.audits;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	onMount(refresh);

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
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
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
