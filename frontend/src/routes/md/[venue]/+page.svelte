<script lang="ts">
	import { page } from '$app/state';
	import LogViewer from '$lib/components/LogViewer.svelte';

	const venue = $derived(page.params.venue ?? '');
</script>

<p class="back"><a href="/md">← MD</a></p>
<LogViewer
	domain="md"
	streamId={venue}
	title={`MD / ${venue}`}
	subtitle="Venue log — refcount changes and dispatch fan-out"
/>

<style>
	.back {
		margin: 0 0 1rem;
		font-size: 0.9rem;
	}
</style>
