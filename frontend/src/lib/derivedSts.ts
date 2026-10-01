import type { Instance } from '$lib/api';

/**
 * Account names directly under `td:`, the same keys deploy resolves.
 *
 * A list or a nested settings block is not a name. Indentation is two
 * spaces, which is what the editor's templates use.
 */
export function tdAccountNames(yaml: string): string[] {
	const names: string[] = [];
	let inTd = false;
	for (const raw of yaml.split('\n')) {
		const line = raw.replace(/\t/g, '  ');
		const trimmed = line.trim();
		if (!trimmed || trimmed.startsWith('#')) continue;
		if (!/^\s/.test(line)) {
			inTd = /^td\s*:/.test(trimmed);
			continue;
		}
		if (!inTd) continue;
		const match = /^  ([^\s:#][^:]*?)\s*:\s*(?:#.*)?$/.exec(line);
		if (match) names.push(match[1].trim());
	}
	return names;
}

type AccountRef = { name: string; instance: string | null };

/**
 * The STS an unpinned deploy is sent to.
 *
 * Same rule as `InstanceRepository.derived_sts`: every named account's TD
 * instance shares one region, and that region has exactly one enabled STS.
 * Anything else is not a guess — the deploy asks the operator to pin one.
 */
export function derivedStsName(
	yaml: string,
	accounts: AccountRef[],
	planes: Pick<Instance, 'name' | 'domain' | 'region' | 'enabled'>[]
): string | null {
	const names = tdAccountNames(yaml);
	if (names.length === 0) return null;
	const regions: string[] = [];
	for (const name of names) {
		const account = accounts.find((row) => row.name === name);
		if (!account?.instance) return null;
		const td = planes.find((row) => row.domain === 'td' && row.name === account.instance);
		if (!td?.region) return null;
		regions.push(td.region);
	}
	if (new Set(regions).size !== 1) return null;
	const region = regions[0];
	const sts = planes.filter(
		(row) => row.domain === 'sts' && row.enabled && row.region === region
	);
	if (sts.length !== 1) return null;
	return sts[0].name;
}
