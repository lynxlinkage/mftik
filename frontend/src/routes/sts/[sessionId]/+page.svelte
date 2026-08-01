<script lang="ts">
	import { page } from '$app/state';
	import LogViewer from '$lib/components/LogViewer.svelte';

	const sessionId = $derived(page.params.sessionId ?? '');
</script>

<p class="back"><a href="/sts">← STS</a></p>
<LogViewer
	domain="sts"
	streamId={sessionId}
	title="STS log"
	subtitle="Strategy session stream (indexed by session_id)"
/>

<style>
	.back {
		margin: 0 0 1rem;
		font-size: 0.9rem;
	}
</style>
