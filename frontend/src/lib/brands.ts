/** Public repo this control plane is built from. */
export const GITHUB_REPO = 'https://github.com/lynxlinkage/mftik';

export function providerLabel(name: string): string {
	const key = name.trim().toLowerCase();
	if (key === 'discord') return 'Discord';
	if (key === 'google') return 'Google';
	if (key === 'github') return 'GitHub';
	if (key === 'password') return 'Password';
	return name;
}

export function hasBrandMark(name: string): boolean {
	const key = name.trim().toLowerCase();
	return key === 'discord' || key === 'google' || key === 'github';
}
