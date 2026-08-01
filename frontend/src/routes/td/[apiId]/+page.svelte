<script lang="ts">
	import { page } from '$app/state';
	import LogViewer from '$lib/components/LogViewer.svelte';

	const apiId = $derived(page.params.apiId ?? '');
</script>

<p class="back"><a href="/td">← TD</a></p>
<LogViewer
	domain="td"
	streamId={apiId}
	title="TD log"
	subtitle="Trading account stream (indexed by api_id)"
/>

<style>
	.back {
		margin: 0 0 1rem;
		font-size: 0.9rem;
	}
</style>
