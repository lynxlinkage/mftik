<script lang="ts">
	import { page } from '$app/state';
	import '../app.css';

	let { children } = $props();

	const nav = [
		{ href: '/', label: 'Home' },
		{ href: '/apis', label: 'APIs' },
		{ href: '/sts', label: 'STS' },
		{ href: '/td', label: 'TD' },
		{ href: '/md', label: 'MD' },
		{ href: '/audit', label: 'Audit' }
	] as const;

	function isActive(href: string, pathname: string): boolean {
		if (href === '/') return pathname === '/';
		return pathname === href || pathname.startsWith(`${href}/`);
	}
</script>

<div class="shell">
	<aside class="nav">
		<a class="brand" href="/">
			<span class="mark">MFT</span>
			<span class="tag">control</span>
		</a>
		<nav>
			{#each nav as item}
				<a
					href={item.href}
					class:active={isActive(item.href, page.url.pathname)}
					data-sveltekit-preload-data="hover"
				>
					{item.label}
				</a>
			{/each}
		</nav>
	</aside>

	<main class="content">
		{@render children()}
	</main>
</div>

<style>
	.shell {
		display: grid;
		grid-template-columns: var(--nav-width) 1fr;
		min-height: 100vh;
	}

	.nav {
		position: sticky;
		top: 0;
		align-self: start;
		height: 100vh;
		padding: 1.4rem 1rem;
		border-right: 1px solid var(--border);
		background: linear-gradient(180deg, rgba(14, 20, 28, 0.96), rgba(10, 14, 20, 0.92));
		display: flex;
		flex-direction: column;
		gap: 1.75rem;
	}

	.brand {
		display: flex;
		align-items: baseline;
		gap: 0.55rem;
		text-decoration: none;
		color: inherit;
	}

	.mark {
		font-family: var(--font);
		font-size: 1.55rem;
		font-weight: 500;
		letter-spacing: 0.14em;
	}

	.tag {
		font-size: 0.7rem;
		color: var(--muted);
		text-transform: uppercase;
		letter-spacing: 0.12em;
	}

	nav {
		display: grid;
		gap: 0.25rem;
	}

	nav a {
		display: block;
		padding: 0.55rem 0.7rem;
		border-radius: var(--radius);
		color: var(--muted);
		text-decoration: none;
		font-weight: 500;
		letter-spacing: 0.02em;
		transition:
			background 160ms ease,
			color 160ms ease,
			transform 160ms ease;
	}

	nav a:hover {
		color: var(--text);
		background: rgba(255, 255, 255, 0.03);
		text-decoration: none;
	}

	nav a.active {
		color: var(--text);
		background: var(--accent-dim);
		transform: translateX(2px);
	}

	.content {
		padding: 1.75rem 1.75rem 2.5rem;
		min-width: 0;
	}

	@media (max-width: 800px) {
		.shell {
			grid-template-columns: 1fr;
		}

		.nav {
			position: static;
			height: auto;
			border-right: none;
			border-bottom: 1px solid var(--border);
			padding: 1rem;
			gap: 1rem;
		}

		nav {
			grid-auto-flow: column;
			grid-auto-columns: max-content;
			overflow-x: auto;
		}

		.content {
			padding: 1.25rem 1rem 2rem;
		}
	}
</style>
