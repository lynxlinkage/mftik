"""strategy.yml — deployment document for TD + MD + STS."""

from __future__ import annotations

from typing import Any

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

from mftik.protocol.topics import Topics

#: The document carries no strategy type. Which strategy runs is chosen at
#: deploy time (``POST /sts/deploy/{type}``), because the type decides what
#: ``sts:`` may contain — a config written for one strategy is meaningless to
#: another, so pairing them in one editable blob invites documents that parse
#: but cannot run. See :mod:`mftik.protocol.strategy_catalog` for per-type
#: templates.


#: Resume this run after an STS restart, or leave it ended.
RESTART_ALWAYS = "always"
RESTART_NEVER = "never"
RESTART_MODES = frozenset({RESTART_ALWAYS, RESTART_NEVER})


class StrategySpec(BaseModel):
    """Parsed strategy.yml document.

    td / md are infra attach lists; sts is the only customized runtime piece.
    """

    model_config = ConfigDict(extra="forbid")

    td: list[str] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    #: Whether this run wants to be restored if STS restarts under it.
    #: ``always`` by default: two gates already stand in front of a rebuild —
    #: the operator has to enable it and the strategy class has to support it
    #: — so a deploy that reaches this question is one whose run was cut short
    #: and would rather continue. Set ``never`` for a one-shot that would be
    #: wrong to resume.
    restart: str = "always"
    #: Flat config for whichever strategy is being deployed. Its keys are the
    #: strategy's own; validation happens in that class's ``on_initialized``.
    sts: dict[str, Any] = Field(default_factory=dict)

    @field_validator("restart", mode="before")
    @classmethod
    def _restart_mode(cls, value: Any) -> str:
        if value is None:
            return RESTART_ALWAYS
        mode = str(value).strip().lower()
        if mode not in RESTART_MODES:
            raise ValueError(
                f"restart must be one of {sorted(RESTART_MODES)}, got {value!r}"
            )
        return mode

    @field_validator("sts", mode="before")
    @classmethod
    def _sts_mapping(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("sts must be a mapping of strategy parameters")
        if "config" in value and isinstance(value.get("config"), dict):
            raise ValueError(
                "sts no longer nests a config block — put the parameters "
                "directly under sts:"
            )
        if "type" in value:
            raise ValueError(
                "sts no longer carries a type — the strategy is chosen at "
                "deploy time (POST /sts/deploy/{type})"
            )
        return dict(value)

    @field_validator("td", mode="before")
    @classmethod
    def _td_names(cls, value: Any) -> list[str]:
        """Account names (resolved to api ids at deploy time)."""
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("td must be a list of api account names")
        out: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    f"td entry must be a non-empty account name, got {item!r}"
                )
            name = item.strip()
            if name in seen:
                raise ValueError(f"duplicate td account name: {name!r}")
            seen.add(name)
            out.append(name)
        return out

    @field_validator("md", mode="before")
    @classmethod
    def _md_feeds(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("md must be a list of feed keys")
        out: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"md entry must be a non-empty string, got {item!r}")
            # Normalized, not just checked: this is YAML a person typed, and
            # what comes out is what MD refcounts on. A ticker typed in
            # lower case and one typed canonically have to end up as one feed,
            # or a single instrument runs two pumps.
            try:
                out.append(Topics.normalize_md_feed(item.strip()))
            except Exception as exc:
                raise ValueError(
                    f"md entry must be topic.UniversalTicker (e.g. "
                    f"bestquote.Gate_Spot_BTCUSDT), got {item!r}: {exc}"
                ) from exc
        return out


class StrategyYamlError(ValueError):
    """Invalid strategy.yml text or structure."""


def parse_strategy_yml(text: str) -> StrategySpec:
    """Parse and validate a strategy.yml document."""
    if not isinstance(text, str) or not text.strip():
        raise StrategyYamlError("strategy.yml is empty")
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise StrategyYamlError(f"invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise StrategyYamlError("strategy.yml root must be a mapping")
    try:
        return StrategySpec.model_validate(raw)
    except ValidationError as exc:
        raise StrategyYamlError(_readable(exc)) from exc
    except Exception as exc:
        raise StrategyYamlError(str(exc)) from exc


def _readable(exc: ValidationError) -> str:
    """One ``field: what is wrong`` line per problem.

    ``str(ValidationError)`` is written for someone debugging the model, not
    for someone who typed the document: it leads with a count, repeats the
    class name, and trails every message with the input value, a type tag and
    a link to pydantic's docs. This text is what a person sees in the editor
    and what ``mftik check`` prints, so what survives is the field and the
    sentence the validator raised.
    """
    lines: list[str] = []
    for error in exc.errors():
        where = ".".join(str(part) for part in error.get("loc", ())) or "strategy.yml"
        message = str(error.get("msg", "")).strip()
        # Pydantic prefixes a raised ValueError with this. The validators here
        # write whole sentences, so the prefix is noise in front of one.
        for prefix in ("Value error, ", "Assertion failed, "):
            if message.startswith(prefix):
                message = message[len(prefix) :]
        lines.append(f"{where}: {message}")
    return "\n".join(lines) or str(exc)


def dump_strategy_yml(spec: StrategySpec) -> str:
    """Serialize a StrategySpec to YAML text."""
    payload = {
        "td": list(spec.td),
        "md": list(spec.md),
        "restart": spec.restart,
        "sts": dict(spec.sts),
    }
    return yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
