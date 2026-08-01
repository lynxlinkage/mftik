<script lang="ts">
	import { onMount } from 'svelte';
	import { api, formatTs, type Audit } from '$lib/api';

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
					<th>User</th>
					<th>Operation</th>
					<th>Result</th>
				</tr>
			</thead>
			<tbody>
				{#each audits as a (a.id)}
					<tr>
						<td class="muted">{formatTs(a.created_at)}</td>
						<td>{a.user_id}</td>
						<td><code>{a.operation}</code></td>
						<td class="result">{a.result}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

<style>
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
