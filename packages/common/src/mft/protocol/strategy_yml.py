"""strategy.yml — deployment document for TD + MD + STS."""

from __future__ import annotations

from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mft.protocol.topics import Topics

#: The document carries no strategy type. Which strategy runs is chosen at
#: deploy time (``POST /sts/deploy/{type}``), because the type decides what
#: ``sts:`` may contain — a config written for one strategy is meaningless to
#: another, so pairing them in one editable blob invites documents that parse
#: but cannot run. See :mod:`mft.protocol.strategy_catalog` for per-type
#: templates.


class StrategySpec(BaseModel):
    """Parsed strategy.yml document.

    td / md are infra attach lists; sts is the only customized runtime piece.
    """

    model_config = ConfigDict(extra="forbid")

    td: list[str] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    #: Flat config for whichever strategy is being deployed. Its keys are the
    #: strategy's own; validation happens in that class's ``on_initialized``.
    sts: dict[str, Any] = Field(default_factory=dict)

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
            feed = item.strip()
            try:
                Topics.parse_md_feed(feed)
            except ValueError as exc:
                raise ValueError(
                    f"md entry must be venue.topic.symbol, got {feed!r}"
                ) from exc
            out.append(feed)
        return out

    @model_validator(mode="after")
    def _cross_field(self) -> StrategySpec:
        # Hook for future cross-field rules.
        return self


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
    except Exception as exc:
        raise StrategyYamlError(str(exc)) from exc


def dump_strategy_yml(spec: StrategySpec) -> str:
    """Serialize a StrategySpec to YAML text."""
    payload = {
        "td": list(spec.td),
        "md": list(spec.md),
        "sts": dict(spec.sts),
    }
    return yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
