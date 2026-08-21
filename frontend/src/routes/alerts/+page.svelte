<script lang="ts">
	import { onMount } from 'svelte';
	import {
		api,
		formatTs,
		type Alert,
		type AlertDelivery,
		type AlertMatcher,
		type AlertSource,
		type ApiCredential,
		type Venue
	} from '$lib/api';

	let sources = $state<AlertSource[]>([]);
	let matchers = $state<AlertMatcher[]>([]);
	let alerts = $state<Alert[]>([]);
	let deliveries = $state<AlertDelivery[]>([]);
	let stsTypes = $state<string[]>([]);
	let liveTypeCounts = $state<Record<string, number>>({});
	let accounts = $state<ApiCredential[]>([]);
	let venues = $state<Venue[]>([]);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let selectedAlert = $state<number | null>(null);

	let sourceDomain = $state<'sts' | 'td' | 'md'>('sts');
	let sourceSelector = $state('*');
	let matcherName = $state('');
	let matcherKind = $state<'level' | 'regex' | 'extract'>('level');
	let levelWarn = $state(true);
	let levelError = $state(true);
	let levelInfo = $state(false);
	let matcherPattern = $state('');
	let extractGroup = $state(1);
	let extractAs = $state<'float' | 'int' | 'str'>('float');
	let extractOp = $state('>');
	let extractValue = $state('0.99');
	let alertName = $state('');
	let webhookUrl = $state('');
	let flushInterval = $state(30);
	let maxEvents = $state(15);
	let maxBuffer = $state(200);
	let dedupe = $state(true);
	let alertEnabled = $state(true);
	let wireMatcherId = $state<number | ''>('');
	let wireAlertId = $state<number | ''>('');

	const stsOptions = $derived(
		[...new Set([...stsTypes, ...Object.keys(liveTypeCounts)])].sort()
	);

	function matcherSpec(): Record<string, unknown> {
		if (matcherKind === 'level') {
			const levels = [
				levelInfo ? 'info' : null,
				levelWarn ? 'warn' : null,
				levelError ? 'error' : null
			].filter((x): x is string => x != null);
			return { levels };
		}
		if (matcherKind === 'regex') return { pattern: matcherPattern };
		return {
			pattern: matcherPattern,
			group: extractGroup,
			as: extractAs,
			op: extractOp,
			value: extractAs === 'str' ? extractValue : Number(extractValue)
		};
	}

	async function refresh() {
		loading = true;
		error = null;
		try {
			const [src, mat, al, types, live, apis, venueList] = await Promise.all([
				api.alertSources(),
				api.alertMatchers(),
				api.alerts(),
				api.strategyTypes(),
				api.strategies({ status: 'live', limit: 200 }),
				api.apis(),
				api.venues()
			]);
			sources = src.sources;
			matchers = mat.matchers;
			alerts = al.alerts;
			stsTypes = types.types;
			accounts = apis.apis;
			venues = venueList.venues;
			const counts: Record<string, number> = {};
			for (const row of live.strategies) {
				if (!row.type) continue;
				counts[row.type] = (counts[row.type] ?? 0) + 1;
			}
			liveTypeCounts = counts;
			if (selectedAlert != null) {
				const listed = await api.alertDeliveries(selectedAlert);
				deliveries = listed.deliveries;
			}
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	async function addSource() {
		error = null;
		try {
			await api.createAlertSource({ domain: sourceDomain, selector: sourceSelector });
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function addMatcher() {
		error = null;
		try {
			await api.createAlertMatcher({
				name: matcherName,
				kind: matcherKind,
				spec: matcherSpec()
			});
			matcherName = '';
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function addAlert() {
		error = null;
		try {
			const created = await api.createAlert({
				name: alertName,
				webhook_url: webhookUrl,
				flush_interval_s: flushInterval,
				max_events_in_payload: maxEvents,
				max_buffer_events: maxBuffer,
				dedupe,
				enabled: alertEnabled
			});
			if ('webhook_url' in created) {
				error = 'GET must not return webhook_url';
			}
			alertName = '';
			webhookUrl = '';
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function fireTest(id: number) {
		error = null;
		try {
			await api.testAlert(id);
			await selectAlert(id);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function selectAlert(id: number) {
		selectedAlert = id;
		const listed = await api.alertDeliveries(id);
		deliveries = listed.deliveries;
	}

	async function wireSource(sourceId: number) {
		if (wireMatcherId === '') return;
		await api.wireSourceMatcher(sourceId, Number(wireMatcherId));
		await refresh();
	}

	async function wireMatcher(matcherId: number) {
		if (wireAlertId === '') return;
		await api.wireMatcherAlert(matcherId, Number(wireAlertId));
		await refresh();
	}

	onMount(refresh);
</script>

<div class="page-head">
	<div>
		<h1>Alerts</h1>
		<p>Live logs to a Discord webhook. Source → Matcher → Alert.</p>
	</div>
	<button type="button" class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if error}
	<div class="error-banner">{error}</div>
{/if}

{#if !loading && sources.length === 0 && matchers.length === 0 && alerts.length === 0}
	<p class="empty-state">No graph yet. Add a Source, a Matcher, and an Alert, then wire them.</p>
{/if}

<div class="cols">
	<section class="panel">
		<h2>Sources</h2>
		{#if sources.length === 0}
			<p class="empty-state">None.</p>
		{:else}
			<ul>
				{#each sources as source (source.id)}
					<li>
						<code>{source.domain}:{source.selector}</code>
						<span class="muted">→ {source.matcher_ids.length}</span>
						<select bind:value={wireMatcherId} aria-label="Matcher for source {source.id}">
							<option value="">Wire matcher…</option>
							{#each matchers as matcher (matcher.id)}
								<option value={matcher.id}>{matcher.name}</option>
							{/each}
						</select>
						<button type="button" class="secondary" onclick={() => wireSource(source.id)}>Wire</button>
					</li>
				{/each}
			</ul>
		{/if}
		<form
			onsubmit={(e) => {
				e.preventDefault();
				void addSource();
			}}
		>
			<label>Domain
				<select bind:value={sourceDomain} onchange={() => (sourceSelector = '*')}>
					<option value="sts">sts</option>
					<option value="td">td</option>
					<option value="md">md</option>
				</select>
			</label>
			<label>Selector
				<select bind:value={sourceSelector} data-testid="sts-picker">
					<option value="*">*</option>
					{#if sourceDomain === 'sts'}
						{#each stsOptions as type (type)}
							<option value={type}
								>{type}{liveTypeCounts[type] ? ` (${liveTypeCounts[type]} live)` : ''}</option
							>
						{/each}
					{:else if sourceDomain === 'td'}
						{#each accounts as account (account.id)}
							<option value={String(account.id)}>{account.name} ({account.id})</option>
						{/each}
					{:else}
						{#each venues as venue (venue.name)}
							<option value={venue.name}>{venue.name}</option>
						{/each}
					{/if}
				</select>
			</label>
			<button type="submit">Add source</button>
		</form>
	</section>

	<section class="panel">
		<h2>Matchers</h2>
		{#if matchers.length === 0}
			<p class="empty-state">None.</p>
		{:else}
			<ul>
				{#each matchers as matcher (matcher.id)}
					<li>
						<strong>{matcher.name}</strong>
						<code>{matcher.kind}</code>
						{#if matcher.disabled_reason}
							<span class="warn">disabled: {matcher.disabled_reason}</span>
						{/if}
						<select bind:value={wireAlertId} aria-label="Alert for matcher {matcher.id}">
							<option value="">Wire alert…</option>
							{#each alerts as alert (alert.id)}
								<option value={alert.id}>{alert.name}</option>
							{/each}
						</select>
						<button type="button" class="secondary" onclick={() => wireMatcher(matcher.id)}
							>Wire</button
						>
					</li>
				{/each}
			</ul>
		{/if}
		<form
			onsubmit={(e) => {
				e.preventDefault();
				void addMatcher();
			}}
		>
			<label>Matcher name <input bind:value={matcherName} required /></label>
			<label>Kind
				<select bind:value={matcherKind}>
					<option value="level">level</option>
					<option value="regex">regex</option>
					<option value="extract">extract</option>
				</select>
			</label>
			{#if matcherKind === 'level'}
				<label><input type="checkbox" bind:checked={levelInfo} /> info</label>
				<label><input type="checkbox" bind:checked={levelWarn} /> warn</label>
				<label><input type="checkbox" bind:checked={levelError} /> error</label>
			{:else}
				<label>Pattern <input bind:value={matcherPattern} required /></label>
				{#if matcherKind === 'extract'}
					<label>Group <input type="number" min="1" bind:value={extractGroup} /></label>
					<label>As
						<select bind:value={extractAs}>
							<option value="float">float</option>
							<option value="int">int</option>
							<option value="str">str</option>
						</select>
					</label>
					<label>Op
						<select bind:value={extractOp}>
							<option value=">">&gt;</option>
							<option value=">=">&gt;=</option>
							<option value="<">&lt;</option>
							<option value="<=">&lt;=</option>
							<option value="==">==</option>
							<option value="!=">!=</option>
						</select>
					</label>
					<label>Value <input bind:value={extractValue} /></label>
				{/if}
			{/if}
			<button type="submit">Add matcher</button>
		</form>
	</section>

	<section class="panel">
		<h2>Alerts</h2>
		{#if alerts.length === 0}
			<p class="empty-state">None.</p>
		{:else}
			<ul>
				{#each alerts as alert (alert.id)}
					<li>
						<button type="button" class="linkish" onclick={() => selectAlert(alert.id)}>
							<strong>{alert.name}</strong>
						</button>
						<code>{alert.webhook_masked}</code>
						{#if !alert.enabled}<span class="muted">off</span>{/if}
						<button type="button" class="secondary" onclick={() => fireTest(alert.id)}>Test</button>
					</li>
				{/each}
			</ul>
		{/if}
		<form
			onsubmit={(e) => {
				e.preventDefault();
				void addAlert();
			}}
		>
			<label>Alert name <input bind:value={alertName} required /></label>
			<label>Webhook URL <input type="password" bind:value={webhookUrl} required autocomplete="off" /></label>
			<label>Window (s) <input type="number" min="1" bind:value={flushInterval} /></label>
			<label>Lines in embed <input type="number" min="1" bind:value={maxEvents} /></label>
			<label>Buffer cap <input type="number" min="1" bind:value={maxBuffer} /></label>
			<label><input type="checkbox" bind:checked={dedupe} /> Dedupe identical messages</label>
			<label><input type="checkbox" bind:checked={alertEnabled} /> Enable</label>
			<button type="submit">Add alert</button>
		</form>

		{#if selectedAlert != null}
			<h3>Deliveries</h3>
			{#if deliveries.length === 0}
				<p class="empty-state">No fires yet.</p>
			{:else}
				<ul>
					{#each deliveries as row (row.id)}
						<li class="muted">
							{formatTs(row.ts)} · events={row.event_count} dropped={row.dropped_count}
							{row.http_status ?? '—'}
							{row.error ?? ''}
						</li>
					{/each}
				</ul>
			{/if}
		{/if}
	</section>
</div>

<style>
	.cols {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 1rem;
		align-items: start;
	}
	ul {
		list-style: none;
		padding: 0;
		margin: 0 0 1rem;
	}
	li {
		display: flex;
		flex-wrap: wrap;
		gap: 0.4rem;
		align-items: center;
		margin-bottom: 0.5rem;
	}
	form {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
	}
	label {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
		font-size: 0.85rem;
		color: var(--muted);
	}
	label:has(input[type='checkbox']) {
		flex-direction: row;
		align-items: center;
	}
	.linkish {
		background: none;
		border: 0;
		padding: 0;
		color: inherit;
		cursor: pointer;
		text-align: left;
	}
	.warn {
		color: var(--warn, #c9a227);
	}
	@media (max-width: 900px) {
		.cols {
			grid-template-columns: 1fr;
		}
	}
</style>
