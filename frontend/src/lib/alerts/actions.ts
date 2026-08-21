export type GraphKind = 'source' | 'matcher' | 'alert';

export const graphActions = {
	selectAlert: (_id: number) => {},
	testAlert: (_id: number) => {},
	remove: (_kind: GraphKind, _id: number) => {}
};

export function bindGraphActions(next: Partial<typeof graphActions>): void {
	Object.assign(graphActions, next);
}
