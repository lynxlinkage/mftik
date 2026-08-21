<script lang="ts">
	import RiArchive2Line from 'svelte-remixicon/RiArchive2Line.svelte';
	import RiDashboardLine from 'svelte-remixicon/RiDashboardLine.svelte';
	import RiFileList3Line from 'svelte-remixicon/RiFileList3Line.svelte';
	import RiHome5Line from 'svelte-remixicon/RiHome5Line.svelte';
	import RiKey2Line from 'svelte-remixicon/RiKey2Line.svelte';
	import RiNotification3Line from 'svelte-remixicon/RiNotification3Line.svelte';
	import RiPulseLine from 'svelte-remixicon/RiPulseLine.svelte';
	import RiSettings3Line from 'svelte-remixicon/RiSettings3Line.svelte';
	import RiStockLine from 'svelte-remixicon/RiStockLine.svelte';

	interface Props {
		href: string;
	}

	let { href }: Props = $props();

	const size = '1.1em';
</script>

{#if href === '/'}
	<RiHome5Line {size} />
{:else if href === '/board'}
	<RiDashboardLine {size} />
{:else if href === '/apis'}
	<RiKey2Line {size} />
{:else if href === '/strategy'}
	<RiPulseLine {size} />
{:else if href === '/alerts'}
	<RiNotification3Line {size} />
{:else if href === '/registry'}
	<RiArchive2Line {size} />
{:else if href === '/sym'}
	<RiStockLine {size} />
{:else if href === '/audit'}
	<RiFileList3Line {size} />
{:else if href === '/settings'}
	<RiSettings3Line {size} />
{/if}
