import { MarkerType, Position, type Edge, type Node, type XYPosition } from '@xyflow/svelte';
import type { Alert, AlertMatcher, AlertSource } from '$lib/api';

export const GAP = 148;
export const LANE_PAD = 20;
export const FALLBACK_SIZE = { w: 960, h: 560 };
const POSITIONS_KEY = 'mftik.alerts.nodePositions';

export type CanvasSize = { w: number; h: number };

const NODE_SIZE: Record<GraphKind, { w: number; h: number }> = {
	source: { w: 230, h: 96 },
	matcher: { w: 230, h: 132 },
	alert: { w: 240, h: 156 }
};

export type GraphKind = 'source' | 'matcher' | 'alert';

export type SourceNode = Node<{ source: AlertSource }, 'source'>;
export type MatcherNode = Node<{ matcher: AlertMatcher }, 'matcher'>;
export type AlertNode = Node<{ alert: Alert }, 'alert'>;
export type AlertGraphNode = SourceNode | MatcherNode | AlertNode;

export function nodeId(kind: GraphKind, id: number): string {
	return `${kind}-${id}`;
}

export function parseNodeId(id: string): { kind: GraphKind; id: number } | null {
	const hit = /^(source|matcher|alert)-(\d+)$/.exec(id);
	if (!hit) return null;
	return { kind: hit[1] as GraphKind, id: Number(hit[2]) };
}

export function isAllowedWire(source: string, target: string): boolean {
	const from = parseNodeId(source);
	const to = parseNodeId(target);
	if (!from || !to) return false;
	return (
		(from.kind === 'source' && to.kind === 'matcher') ||
		(from.kind === 'matcher' && to.kind === 'alert')
	);
}

export function laneX(kind: GraphKind, width: number): number {
	const col = Math.max(width, 720) / 3;
	const index = kind === 'source' ? 0 : kind === 'matcher' ? 1 : 2;
	return Math.round(col * index + LANE_PAD);
}

export function nodeSize(kind: GraphKind): { w: number; h: number } {
	return NODE_SIZE[kind];
}

export function loadPositions(): Map<string, XYPosition> {
	const out = new Map<string, XYPosition>();
	try {
		const raw = localStorage.getItem(POSITIONS_KEY);
		if (!raw) return out;
		const parsed = JSON.parse(raw) as Record<string, { x?: unknown; y?: unknown }>;
		for (const [id, pos] of Object.entries(parsed)) {
			if (!parseNodeId(id)) continue;
			if (typeof pos?.x !== 'number' || typeof pos?.y !== 'number') continue;
			if (!Number.isFinite(pos.x) || !Number.isFinite(pos.y)) continue;
			out.set(id, { x: pos.x, y: pos.y });
		}
	} catch {
		/* private mode / quota / bad JSON */
	}
	return out;
}

export function savePositions(positions: Map<string, XYPosition>): void {
	const obj: Record<string, XYPosition> = {};
	for (const [id, pos] of positions) obj[id] = { x: pos.x, y: pos.y };
	try {
		localStorage.setItem(POSITIONS_KEY, JSON.stringify(obj));
	} catch {
		/* private mode / quota */
	}
}

export function clampPosition(
	pos: XYPosition,
	kind: GraphKind,
	bounds: CanvasSize
): XYPosition {
	const size = NODE_SIZE[kind];
	return {
		x: Math.min(Math.max(0, pos.x), Math.max(0, bounds.w - size.w)),
		y: Math.min(Math.max(0, pos.y), Math.max(0, bounds.h - size.h))
	};
}

function placed(
	id: string,
	kind: GraphKind,
	index: number,
	remembered: Map<string, XYPosition>,
	bounds: CanvasSize
): XYPosition {
	const prev = remembered.get(id);
	if (prev) return clampPosition(prev, kind, bounds);
	return clampPosition({ x: laneX(kind, bounds.w), y: 32 + index * GAP }, kind, bounds);
}

export function buildNodes(
	sources: AlertSource[],
	matchers: AlertMatcher[],
	alerts: Alert[],
	remembered: Map<string, XYPosition>,
	bounds: CanvasSize = FALLBACK_SIZE
): AlertGraphNode[] {
	const sourceNodes: SourceNode[] = sources.map((source, i) => ({
		id: nodeId('source', source.id),
		type: 'source',
		position: placed(nodeId('source', source.id), 'source', i, remembered, bounds),
		data: { source },
		dragHandle: '.card',
		sourcePosition: Position.Right,
		targetPosition: Position.Left,
		width: NODE_SIZE.source.w
	}));
	const matcherNodes: MatcherNode[] = matchers.map((matcher, i) => ({
		id: nodeId('matcher', matcher.id),
		type: 'matcher',
		position: placed(nodeId('matcher', matcher.id), 'matcher', i, remembered, bounds),
		data: { matcher },
		dragHandle: '.card',
		sourcePosition: Position.Right,
		targetPosition: Position.Left,
		width: NODE_SIZE.matcher.w
	}));
	const alertNodes: AlertNode[] = alerts.map((alert, i) => ({
		id: nodeId('alert', alert.id),
		type: 'alert',
		position: placed(nodeId('alert', alert.id), 'alert', i, remembered, bounds),
		data: { alert },
		dragHandle: '.card',
		sourcePosition: Position.Right,
		targetPosition: Position.Left,
		width: NODE_SIZE.alert.w
	}));
	return [...sourceNodes, ...matcherNodes, ...alertNodes];
}

export function buildEdges(sources: AlertSource[], matchers: AlertMatcher[]): Edge[] {
	const edges: Edge[] = [];
	for (const source of sources) {
		for (const matcherId of source.matcher_ids) {
			edges.push({
				id: `sm-${source.id}-${matcherId}`,
				source: nodeId('source', source.id),
				target: nodeId('matcher', matcherId),
				type: 'default',
				animated: true,
				markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 }
			});
		}
	}
	for (const matcher of matchers) {
		for (const alertId of matcher.alert_ids) {
			edges.push({
				id: `ma-${matcher.id}-${alertId}`,
				source: nodeId('matcher', matcher.id),
				target: nodeId('alert', alertId),
				type: 'default',
				animated: true,
				markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 }
			});
		}
	}
	return edges;
}

export function matcherSummary(matcher: AlertMatcher): string {
	const spec = matcher.spec ?? {};
	if (matcher.kind === 'level') {
		const levels = Array.isArray(spec.levels) ? spec.levels.map(String) : [];
		return levels.join(', ') || 'no levels';
	}
	if (matcher.kind === 'regex') return String(spec.pattern ?? '');
	const op = spec.op ?? '?';
	const value = spec.value ?? '';
	return `${op} ${value}`;
}
