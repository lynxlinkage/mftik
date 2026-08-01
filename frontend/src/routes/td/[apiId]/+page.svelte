<script lang="ts">
	import { page } from '$app/state';
	import { api, apiLabel } from '$lib/api';
	import LogViewer from '$lib/components/LogViewer.svelte';

	const apiId = $derived(page.params.apiId ?? '');
	const apiIdNum = $derived(Number(apiId));

	let label = $state<string>('');

	$effect(() => {
		const id = apiIdNum;
		const raw = apiId;
		let cancelled = false;
		(async () => {
			if (!Number.isFinite(id) || id <= 0) {
				if (!cancelled) label = raw;
				return;
			}
			try {
				const res = await api.apis();
				if (cancelled) return;
				const row = res.apis.find((a) => a.id === id);
				label = row
					? apiLabel({ api_id: row.id, venue: row.venue, name: row.name })
					: String(id);
			} catch {
				if (!cancelled) label = String(id);
			}
		})();
		return () => {
			cancelled = true;
		};
	});
</script>

<p class="back"><a href="/td">← TD</a></p>
<LogViewer
	domain="td"
	streamId={apiId}
	title="TD log"
	subtitle={label ? `Trading account ${label}` : 'Trading account stream'}
/>

<style>
	.back {
		margin: 0 0 1rem;
		font-size: 0.9rem;
	}
</style>
