<script lang="ts">
	import { SiDiscord, SiGithub, SiGoogle } from '@icons-pack/svelte-simple-icons';

	interface Props {
		name: string;
		size?: number;
	}

	let { name, size = 16 }: Props = $props();

	const brand = $derived(name.trim().toLowerCase());
</script>

<!-- Brand marks are decorative next to their label; the SVG title would
     otherwise become a second accessible name. -->
<span class="mark" aria-hidden="true">
	{#if brand === 'discord'}
		<SiDiscord {size} color="currentColor" title="" />
	{:else if brand === 'github'}
		<SiGithub {size} color="currentColor" title="" />
	{:else if brand === 'google'}
		<SiGoogle {size} color="currentColor" title="" />
	{/if}
</span>

<style>
	.mark {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		line-height: 0;
	}
</style>
