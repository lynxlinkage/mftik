<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import { api, formatTs, shortId, type ApiCredential, type Session } from '$lib/api';

	let sessions = $state<Session[]>([]);
	let strategies = $state<string[]>(['noop']);
	let apis = $state<ApiCredential[]>([]);
	let selected = $state('noop');
	/** Select value as string (HTML); parsed to int on deploy. */
	let selectedApiId = $state('');
	let error = $state<string | null>(null);
	let busy = $state(false);
	let loading = $state(true);

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [s, st, a] = await Promise.all([
				api.stsSessions('live'),
				api.strategies(),
				api.apis()
			]);
			sessions = s.sessions;
			strategies = st.strategies.length ? st.strategies : ['noop'];
			apis = a.apis;
			if (!strategies.includes(selected)) selected = strategies[0];
			const ids = new Set(apis.map((x) => String(x.id)));
			if (!ids.has(selectedApiId)) {
				selectedApiId = apis[0] ? String(apis[0].id) : '';
			}
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function deploy() {
		busy = true;
		error = null;
		try {
			const apiId = Number(selectedApiId);
			if (!Number.isFinite(apiId) || apiId <= 0) {
				throw new Error('Select a TD API credential');
			}
			const created = await api.deploySts(selected, { td: [apiId] });
			await refresh();
			await goto(`/sts/${created.session_id}`);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function togglePause(s: Session) {
		busy = true;
		error = null;
		try {
			if (s.paused) await api.resumeSts(s.session_id);
			else await api.pauseSts(s.session_id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	async function stop(s: Session) {
		busy = true;
		error = null;
		try {
			await api.stopSts(s.session_id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>STS</h1>
		<p>Deploy a strategy (API attaches TD), pause / resume, or stop — then open its log stream.</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

<section class="panel create">
	<label>
		Strategy
		<select bind:value={selected} disabled={busy}>
			{#each strategies as name}
				<option value={name}>{name}</option>
			{/each}
		</select>
	</label>
	<label>
		TD API
		<select bind:value={selectedApiId} disabled={busy || apis.length === 0}>
			{#if apis.length === 0}
				<option value="">No APIs seeded</option>
			{:else}
				{#each apis as a}
					<option value={String(a.id)}>{a.id} — {a.venue} / {a.api_key}</option>
				{/each}
			{/if}
		</select>
	</label>
	<button type="button" onclick={deploy} disabled={busy || !selectedApiId}>
		Deploy
	</button>
</section>

<section class="panel table-wrap">
	{#if sessions.length === 0}
		<p class="empty-state">{loading ? 'Loading…' : 'No live STS sessions.'}</p>
	{:else}
		<table class="data">
			<thead>
				<tr>
					<th>Session</th>
					<th>Strategy</th>
					<th>State</th>
					<th>Created</th>
					<th></th>
				</tr>
			</thead>
			<tbody>
				{#each sessions as s (s.session_id)}
					<tr>
						<td>
							<a href={`/sts/${s.session_id}`} title={s.session_id}>
								{shortId(s.session_id)}
							</a>
						</td>
						<td>{s.strategy ?? '—'}</td>
						<td>
							{#if s.paused}
								<span class="badge paused">paused</span>
							{:else}
								<span class="badge live">running</span>
							{/if}
						</td>
						<td class="muted">{formatTs(s.created_at)}</td>
						<td>
							<div class="actions">
								<button
									type="button"
									class="secondary"
									disabled={busy}
									onclick={() => togglePause(s)}
								>
									{s.paused ? 'Resume' : 'Pause'}
								</button>
								<button type="button" class="danger" disabled={busy} onclick={() => stop(s)}>
									Stop
								</button>
								<a class="link-btn" href={`/sts/${s.session_id}`}>Logs</a>
							</div>
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
		min-width: 12rem;
	}

	select {
		background: var(--bg);
		border: 1px solid var(--border);
		color: var(--text);
		padding: 0.55rem 0.65rem;
		border-radius: var(--radius);
	}

	.table-wrap {
		overflow-x: auto;
	}

	.link-btn {
		display: inline-flex;
		align-items: center;
		padding: 0.5rem 0.75rem;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		text-decoration: none;
		font-weight: 600;
		font-size: 0.9rem;
	}

	.link-btn:hover {
		border-color: var(--accent);
		text-decoration: none;
	}
</style>
