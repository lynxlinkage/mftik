/**
 * Context-aware completions for the strategy.yml textarea.
 *
 * The document is small and typed by a person, so this walks lines rather
 * than running a YAML parser. A parse error must not take the hints away —
 * the operator is still writing.
 *
 * ``td`` keys are account names. Deploy resolves them via GET /apis
 * (``Account.name``), not api ids and not the venue key string.
 */

export type HintKind = 'root-key' | 'td-key' | 'md-item' | 'restart-value' | 'sts-account';

export type HintAccount = {
	name: string;
	venue?: string;
};

export type HintContext = {
	kind: HintKind;
	/** Unquoted text of the token being edited, up to the cursor. */
	prefix: string;
	replaceStart: number;
	replaceEnd: number;
	/** Mapping key still needs a trailing ``:``. */
	needsColon: boolean;
};

export type HintItem = {
	label: string;
	insert: string;
	detail: string;
	kind: HintKind;
};

export const ROOT_KEYS = ['td', 'md', 'restart', 'sts'] as const;
export const RESTART_MODES = ['always', 'never'] as const;
export const MD_TOPICS = ['orderbook', 'bestquote', 'aggtrade', 'trade'] as const;
export const STS_ACCOUNT_FIELDS = new Set(['quote_account', 'hedge_account']);

const YAML_RESERVED = new Set([
	'y',
	'n',
	'yes',
	'no',
	'true',
	'false',
	'on',
	'off',
	'null',
	'~'
]);

type LineInfo = {
	start: number;
	end: number;
	indent: number;
	prefix: string;
	raw: string;
	isComment: boolean;
	isEmpty: boolean;
	isList: boolean;
	key: string | null;
	keyStart: number;
	keyEnd: number;
	colon: number;
	valueStart: number;
	valueEnd: number;
};

function leadingIndent(line: string): { count: number; prefix: string } {
	let i = 0;
	let count = 0;
	while (i < line.length) {
		if (line[i] === ' ') {
			count += 1;
			i += 1;
		} else if (line[i] === '\t') {
			count += 2;
			i += 1;
		} else {
			break;
		}
	}
	return { count, prefix: line.slice(0, i) };
}

function findUnquotedColon(line: string, from: number): number {
	let quote: string | null = null;
	for (let i = from; i < line.length; i += 1) {
		const c = line[i];
		if (quote) {
			if (c === '\\' && quote === '"') {
				i += 1;
				continue;
			}
			if (c === quote) quote = null;
			continue;
		}
		if (c === '"' || c === "'") {
			quote = c;
			continue;
		}
		if (c === '#') return -1;
		if (c === ':') return i;
	}
	return -1;
}

function commentAt(line: string, from: number): number {
	let quote: string | null = null;
	for (let i = from; i < line.length; i += 1) {
		const c = line[i];
		if (quote) {
			if (c === '\\' && quote === '"') {
				i += 1;
				continue;
			}
			if (c === quote) quote = null;
			continue;
		}
		if (c === '"' || c === "'") {
			quote = c;
			continue;
		}
		if (c === '#') return i;
	}
	return -1;
}

export function unquoteYaml(raw: string): string {
	const t = raw.trim();
	if (t.length >= 2) {
		const a = t[0];
		const b = t[t.length - 1];
		if ((a === '"' && b === '"') || (a === "'" && b === "'")) {
			return t.slice(1, -1).replace(/\\"/g, '"').replace(/\\'/g, "'");
		}
	}
	return t;
}

/** Quote a mapping key only when YAML would not keep it as a plain scalar. */
export function asYamlKey(name: string): string {
	if (!name) return '""';
	if (YAML_RESERVED.has(name.toLowerCase())) {
		return `"${name}"`;
	}
	if (/^[\w./ -]+$/.test(name) && !/^\s|\s$/.test(name) && !/^\d+$/.test(name)) {
		return name;
	}
	return `"${name.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function parseLine(raw: string, start: number): LineInfo {
	const end = start + raw.length;
	const { count: indent, prefix } = leadingIndent(raw);
	const body = raw.slice(prefix.length);
	const isEmpty = body.trim() === '';
	const isComment = body.startsWith('#');
	const isList = body.startsWith('- ') || body === '-';
	let key: string | null = null;
	let keyStart = prefix.length;
	let keyEnd = prefix.length;
	let colon = -1;
	let valueStart = prefix.length;
	let valueEnd = raw.length;
	if (!isEmpty && !isComment && !isList) {
		colon = findUnquotedColon(raw, prefix.length);
		const hash = commentAt(raw, prefix.length);
		if (hash >= 0) valueEnd = hash;
		if (colon >= 0) {
			keyStart = prefix.length;
			keyEnd = colon;
			key = unquoteYaml(raw.slice(keyStart, keyEnd));
			valueStart = colon + 1;
			while (valueStart < valueEnd && raw[valueStart] === ' ') valueStart += 1;
		} else {
			keyStart = prefix.length;
			keyEnd = hash >= 0 ? hash : raw.length;
			while (keyEnd > keyStart && raw[keyEnd - 1] === ' ') keyEnd -= 1;
			key = unquoteYaml(raw.slice(keyStart, keyEnd)) || null;
		}
	} else if (isList) {
		const dash = raw.indexOf('-', prefix.length);
		valueStart = dash + 1;
		while (valueStart < raw.length && raw[valueStart] === ' ') valueStart += 1;
		const hash = commentAt(raw, valueStart);
		valueEnd = hash >= 0 ? hash : raw.length;
	}
	return {
		start,
		end,
		indent,
		prefix,
		raw,
		isComment,
		isEmpty,
		isList,
		key,
		keyStart,
		keyEnd,
		colon,
		valueStart,
		valueEnd
	};
}

export function parseYmlLines(text: string): LineInfo[] {
	const lines: LineInfo[] = [];
	let start = 0;
	while (start <= text.length) {
		const nl = text.indexOf('\n', start);
		const end = nl < 0 ? text.length : nl;
		lines.push(parseLine(text.slice(start, end), start));
		if (nl < 0) break;
		start = nl + 1;
	}
	return lines;
}

function lineAt(lines: LineInfo[], cursor: number): LineInfo {
	for (const line of lines) {
		if (cursor <= line.end) return line;
	}
	return lines[lines.length - 1];
}

function sectionOf(lines: LineInfo[], current: LineInfo): string | null {
	let section: string | null = null;
	for (const line of lines) {
		if (line.start > current.start) break;
		if (line.indent === 0 && line.key && !line.isComment && !line.isEmpty) {
			section = line.key;
		}
	}
	return section;
}

function tokenPrefix(raw: string, from: number, cursorInLine: number): string {
	return unquoteYaml(raw.slice(from, Math.max(from, cursorInLine)));
}

export function hintContext(text: string, cursor: number): HintContext | null {
	if (!text.length && cursor === 0) {
		return {
			kind: 'root-key',
			prefix: '',
			replaceStart: 0,
			replaceEnd: 0,
			needsColon: true
		};
	}
	const lines = parseYmlLines(text);
	if (!lines.length) return null;
	const line = lineAt(lines, Math.max(0, Math.min(cursor, text.length)));
	const col = Math.max(0, cursor - line.start);
	if (line.isComment) {
		const hash = line.raw.indexOf('#');
		if (hash >= 0 && col >= hash) return null;
	}
	const section = sectionOf(lines, line);
	const emptyInSection = line.isEmpty || (line.indent === 0 && !line.key && !line.isList);

	if (line.indent === 0 && !line.isList && !line.isComment && line.key !== null && !line.isEmpty) {
		if (line.colon < 0 || col <= line.colon) {
			return {
				kind: 'root-key',
				prefix: tokenPrefix(line.raw, line.keyStart, Math.min(col, line.keyEnd)),
				replaceStart: line.start + line.keyStart,
				replaceEnd: line.start + line.keyEnd,
				needsColon: line.colon < 0
			};
		}
		if (line.key === 'restart') {
			return {
				kind: 'restart-value',
				prefix: tokenPrefix(line.raw, line.valueStart, Math.min(col, line.valueEnd)),
				replaceStart: line.start + line.valueStart,
				replaceEnd: line.start + line.valueEnd,
				needsColon: false
			};
		}
	}

	if (section === 'restart' && line.indent === 0 && line.colon >= 0 && col > line.colon) {
		return {
			kind: 'restart-value',
			prefix: tokenPrefix(line.raw, line.valueStart, Math.min(col, line.valueEnd)),
			replaceStart: line.start + line.valueStart,
			replaceEnd: line.start + line.valueEnd,
			needsColon: false
		};
	}

	if (section === 'td' && (line.indent > 0 || emptyInSection) && !line.isList && !line.isComment) {
		if (line.colon >= 0 && col > line.colon) return null;
		const keyStart = line.isEmpty ? col : line.keyStart;
		const keyEnd = line.isEmpty ? col : line.keyEnd;
		return {
			kind: 'td-key',
			prefix: line.isEmpty ? '' : tokenPrefix(line.raw, keyStart, Math.min(col, keyEnd)),
			replaceStart: line.start + keyStart,
			replaceEnd: line.start + keyEnd,
			needsColon: line.colon < 0
		};
	}

	if (section === 'md' && (line.isList || emptyInSection)) {
		const from = line.isList ? line.valueStart : col;
		const to = line.isList ? line.valueEnd : col;
		return {
			kind: 'md-item',
			prefix: tokenPrefix(line.raw, from, Math.min(col, to)),
			replaceStart: line.start + from,
			replaceEnd: line.start + to,
			needsColon: false
		};
	}

	if (section === 'sts' && line.indent > 0 && !line.isList && !line.isComment && line.key) {
		if (STS_ACCOUNT_FIELDS.has(line.key) && line.colon >= 0 && col > line.colon) {
			return {
				kind: 'sts-account',
				prefix: tokenPrefix(line.raw, line.valueStart, Math.min(col, line.valueEnd)),
				replaceStart: line.start + line.valueStart,
				replaceEnd: line.start + line.valueEnd,
				needsColon: false
			};
		}
	}

	return null;
}

export function tdAccountKeys(text: string): string[] {
	const lines = parseYmlLines(text);
	const keys: string[] = [];
	let inTd = false;
	for (const line of lines) {
		if (line.indent === 0 && line.key && !line.isComment) {
			inTd = line.key === 'td';
			continue;
		}
		if (!inTd || line.isComment || line.isEmpty || line.isList) continue;
		if (line.indent > 0 && line.key) keys.push(line.key);
	}
	return keys;
}

function matches(name: string, prefix: string): boolean {
	if (!prefix) return true;
	return name.toLowerCase().includes(prefix.toLowerCase());
}

function rank(name: string, prefix: string): number {
	if (!prefix) return 0;
	const n = name.toLowerCase();
	const p = prefix.toLowerCase();
	if (n === p) return 0;
	if (n.startsWith(p)) return 1;
	if (n.includes(p)) return 2;
	return 99;
}

function sortByPrefix<T extends { label: string }>(items: T[], prefix: string): T[] {
	return items.sort((a, b) => {
		const d = rank(a.label, prefix) - rank(b.label, prefix);
		if (d !== 0) return d;
		return a.label.localeCompare(b.label);
	});
}

export function hintItems(
	ctx: HintContext,
	opts: { accounts?: HintAccount[]; text?: string } = {}
): HintItem[] {
	const accounts = opts.accounts ?? [];
	const used = new Set(tdAccountKeys(opts.text ?? ''));
	const prefix = ctx.prefix;

	if (ctx.kind === 'root-key') {
		return sortByPrefix(
			ROOT_KEYS.filter((k) => matches(k, prefix)).map((k) => ({
				label: k,
				insert: k,
				detail: k === 'td' ? 'accounts by name' : k === 'md' ? 'feed keys' : k,
				kind: ctx.kind
			})),
			prefix
		);
	}

	if (ctx.kind === 'restart-value') {
		return RESTART_MODES.filter((k) => matches(k, prefix)).map((k) => ({
			label: k,
			insert: k,
			detail: k === 'always' ? 'restore after STS restart' : 'one-shot',
			kind: ctx.kind
		}));
	}

	if (ctx.kind === 'md-item') {
		return MD_TOPICS.filter((k) => matches(k, prefix) || matches(`${k}.`, prefix)).map((k) => ({
			label: `${k}.`,
			insert: `${k}.`,
			detail: 'topic.UniversalTicker',
			kind: ctx.kind
		}));
	}

	if (ctx.kind === 'sts-account') {
		const fromTd = tdAccountKeys(opts.text ?? '');
		const names = fromTd.length ? fromTd : accounts.map((a) => a.name);
		return sortByPrefix(
			names.filter((n) => matches(n, prefix)).map((n) => ({
				label: n,
				insert: asYamlKey(n),
				detail: 'td account',
				kind: ctx.kind
			})),
			prefix
		);
	}

	// td-key: names this node can resolve, minus ones already attached
	// (keep the token under the cursor so editing it still lists itself).
	const current = unquoteYaml(prefix);
	const items: HintItem[] = [];
	for (const row of accounts) {
		const name = row.name.trim();
		if (!name || !matches(name, prefix)) continue;
		if (used.has(name) && name !== current) continue;
		items.push({
			label: name,
			insert: asYamlKey(name),
			detail: row.venue?.trim() || 'account',
			kind: 'td-key'
		});
	}
	return sortByPrefix(items, prefix);
}

export function applyHint(
	text: string,
	ctx: HintContext,
	item: HintItem
): { text: string; cursor: number } {
	let insert = item.insert;
	if (ctx.needsColon && ctx.kind !== 'md-item' && ctx.kind !== 'restart-value') {
		const after = text.slice(ctx.replaceEnd);
		if (!after.startsWith(':')) insert += ':';
	}
	if (ctx.kind === 'td-key' && ctx.needsColon && insert.endsWith(':')) {
		const before = text.slice(0, ctx.replaceStart);
		const atLineStart = before.endsWith('\n') || before.length === 0;
		const line = text.slice(text.lastIndexOf('\n', ctx.replaceStart - 1) + 1, ctx.replaceStart);
		if (atLineStart && line.trim() === '' && line.length === 0) {
			insert = `  ${insert}`;
		}
	}
	const next = text.slice(0, ctx.replaceStart) + insert + text.slice(ctx.replaceEnd);
	return { text: next, cursor: ctx.replaceStart + insert.length };
}

/**
 * Indent the next line the way a YAML mapping / list expects.
 * The textarea will not do this on its own.
 */
export function newlineInsert(text: string, cursor: number): string {
	const lines = parseYmlLines(text);
	if (!lines.length) return '\n';
	const line = lineAt(lines, cursor);
	const section = sectionOf(lines, line);
	if (line.indent === 0 && line.key && (ROOT_KEYS as readonly string[]).includes(line.key)) {
		if (line.key === 'md') return '\n  - ';
		if (line.key === 'td' || line.key === 'sts') return '\n  ';
		return '\n';
	}
	if (line.isList) return `\n${line.prefix}- `;
	if (line.prefix) return `\n${line.prefix}`;
	if (section === 'td' || section === 'sts') return '\n  ';
	if (section === 'md') return '\n  - ';
	return '\n';
}

/** Add ``name`` under ``td:``, or move the cursor onto it when it is already there. */
export function insertTdAccount(
	text: string,
	name: string
): { text: string; cursor: number } {
	const key = asYamlKey(name);
	const existing = tdAccountKeys(text);
	if (existing.includes(name)) {
		const lines = parseYmlLines(text);
		let inTd = false;
		for (const line of lines) {
			if (line.indent === 0 && line.key && !line.isComment) {
				inTd = line.key === 'td';
				continue;
			}
			if (inTd && line.key === name) {
				return { text, cursor: line.start + line.keyEnd };
			}
		}
		return { text, cursor: text.length };
	}

	const line = `  ${key}:\n`;
	const lines = parseYmlLines(text);
	let tdHeader: LineInfo | null = null;
	let lastTd: LineInfo | null = null;
	let nextRoot: LineInfo | null = null;
	for (const row of lines) {
		if (row.indent === 0 && row.key && !row.isComment) {
			if (row.key === 'td') {
				tdHeader = row;
				lastTd = row;
				nextRoot = null;
				continue;
			}
			if (tdHeader && !nextRoot) {
				nextRoot = row;
				break;
			}
		}
		if (tdHeader && !nextRoot) lastTd = row;
	}

	if (!tdHeader) {
		const block = `td:\n${line}`;
		const pad = text && !text.endsWith('\n') ? '\n' : '';
		const next = `${block}${pad}${text}`;
		return { text: next, cursor: `td:\n`.length + line.length - 1 };
	}

	const insertAt = nextRoot ? nextRoot.start : (lastTd?.end ?? tdHeader.end);
	const before = text.slice(0, insertAt);
	const needNl = before.length > 0 && !before.endsWith('\n');
	const injected = `${needNl ? '\n' : ''}${line}`;
	const after = text.slice(insertAt);
	const next = before + injected + after;
	return { text: next, cursor: before.length + injected.length - 1 };
}
