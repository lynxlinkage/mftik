"""B10-01: td_sessions and md_sessions are read-only history.

The repositories no longer offer a method that inserts or updates a row.
A static walk refuses a non-test module that constructs one of the row
types or passes one to ``session.add``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from mftik_db.repositories import MdSessionRepository, TdSessionRepository

_ROOT = Path(__file__).resolve().parents[3]
_SCAN = ("apps", "packages")
_ROWS = {"TdSessionRow", "MdSessionRow"}
_WRITES = (
    "create_live",
    "attach_live",
    "mark_done",
    "mark_done_session",
    "get_live",
)


def test_td_and_md_repositories_expose_no_write_method() -> None:
    for cls in (TdSessionRepository, MdSessionRepository):
        for name in _WRITES:
            assert not hasattr(cls, name), f"{cls.__name__}.{name}"
        assert callable(getattr(cls, "list_sessions"))
        assert callable(getattr(cls, "count"))
        assert callable(getattr(cls, "count_by_instance"))
    assert callable(getattr(TdSessionRepository, "count_live_for_api"))


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_row_call(node: ast.AST) -> bool:
    return isinstance(node, ast.Call) and _call_name(node.func) in _ROWS


class _Writes(ast.NodeVisitor):
    def __init__(self) -> None:
        self.hits: list[int] = []
        self._bound: list[dict[str, bool]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._enter(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._enter(node)

    def _enter(self, node: ast.AST) -> None:
        self._bound.append({})
        self.generic_visit(node)
        self._bound.pop()

    def _remember(self, target: ast.AST, value: ast.AST) -> None:
        if not self._bound:
            return
        if isinstance(target, ast.Name) and (
            _is_row_call(value) or _contains_row_call(value)
        ):
            self._bound[-1][target.id] = True

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._remember(target, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None and node.target is not None:
            self._remember(node.target, node.value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if _is_row_call(node):
            self.hits.append(node.lineno)
        elif isinstance(node.func, ast.Attribute) and node.func.attr == "add":
            for arg in node.args:
                if _is_row_call(arg) or (
                    isinstance(arg, ast.Name) and self._is_bound(arg.id)
                ):
                    self.hits.append(node.lineno)
        self.generic_visit(node)

    def _is_bound(self, name: str) -> bool:
        return any(frame.get(name) for frame in self._bound)


def _contains_row_call(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if _is_row_call(child):
            return True
    return False


def _modules() -> list[Path]:
    found: list[Path] = []
    for root_name in _SCAN:
        root = _ROOT / root_name
        for path in root.rglob("*.py"):
            if "tests" in path.parts or path.name.startswith("test_"):
                continue
            found.append(path)
    return found


# Walking the tree is over the 50 ms unit cap.
@pytest.mark.component
def test_non_test_modules_do_not_construct_session_history_rows() -> None:
    offenders: list[str] = []
    for path in _modules():
        # Most modules never name these rows. Parsing those files is what
        # pushes the walk past the component cap.
        text = path.read_text()
        if "TdSessionRow" not in text and "MdSessionRow" not in text:
            continue
        tree = ast.parse(text, filename=str(path))
        visitor = _Writes()
        visitor.visit(tree)
        for line in visitor.hits:
            offenders.append(f"{path.relative_to(_ROOT)}:{line}")
    assert offenders == []
