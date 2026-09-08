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
from yaml.events import (
    AliasEvent,
    MappingEndEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceEndEvent,
    SequenceStartEvent,
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

#: YAML's merge key. Under ``td:`` it is refused rather than expanded — see
#: :func:`_refuse_collapsing_td_keys`.
_MERGE_KEY = "<<"

#: What ``mftik check`` prints when someone still has a list under ``td:``.
#: The parser prefixes the field name, so this sentence starts at "is".
_TD_LIST_HINT = (
    "is now a mapping of account name to settings, not a list. Change:\n"
    "  td: [paper trader]\nto:\n  td:\n    paper trader:"
)


class TdSettings(BaseModel):
    """Per-account attach options.

    Empty this round — ``extra="forbid"`` so a leverage / margin key is a
    parse error rather than a silently dropped bag.
    """

    model_config = ConfigDict(extra="forbid")


class TdAccountRef(BaseModel):
    """One attached account after names have been resolved to api ids."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    settings: TdSettings = Field(default_factory=TdSettings)

    def dump(self) -> dict[str, Any]:
        return {"api_id": self.api_id, "settings": self.settings.model_dump()}


def load_td(raw: Any) -> dict[str, TdAccountRef]:
    """JSON / wire mapping → name → :class:`TdAccountRef`."""
    if not raw:
        return {}
    if isinstance(raw, list):
        return {
            f"account-{int(api_id)}": TdAccountRef(api_id=int(api_id))
            for api_id in raw
        }
    out: dict[str, TdAccountRef] = {}
    for name, value in dict(raw).items():
        if isinstance(value, TdAccountRef):
            out[str(name)] = value
        elif isinstance(value, dict):
            out[str(name)] = TdAccountRef.model_validate(value)
        else:
            out[str(name)] = TdAccountRef(api_id=int(value))
    return out


def dump_td(td: dict[str, TdAccountRef]) -> dict[str, Any]:
    return {name: ref.dump() for name, ref in td.items()}


def td_api_ids_of(td: dict[str, TdAccountRef] | None) -> list[int]:
    return [ref.api_id for ref in (td or {}).values()]


def attached_api_ids(row: Any) -> list[int]:
    """Attach ids from a session row, whether it holds ``td`` or ``td_api_ids``.

    Board / list still speak ``td_api_ids``. The column is gone; a mapping
    row and a test double that only set the old attribute both work.
    """
    raw = getattr(row, "td", None)
    if isinstance(raw, dict) and raw:
        return td_api_ids_of(load_td(raw))
    return [int(x) for x in (getattr(row, "td_api_ids", None) or [])]



#: The key an unpinned feed list is stored under.
#:
#: ``*`` rather than an empty string so a document that reads back is legible,
#: and safe as a sentinel because an instance name is a subject segment
#: and is refused that character where names are declared.
ANY_INSTANCE = "*"

#: What ``mftik check`` prints for a ``md:`` mapping whose keys would fold.
_MD_MERGE_HINT = (
    "md: merge keys (<<) are not accepted — feeds merged in from an anchor "
    "are silently replaced by an explicit key of the same name. Write each "
    "instance out."
)


def load_md(raw: Any) -> dict[str, list[str]]:
    """Wire / YAML ``md:`` → instance name → feed keys.

    A plain list is every feed, unpinned: ``{ANY_INSTANCE: [...]}``. That is
    what every document written before instances existed means, and what one
    written today still means when the author does not care which MD serves
    it.
    """
    if not raw:
        return {}
    if isinstance(raw, list):
        return {ANY_INSTANCE: [str(f) for f in raw]}
    out: dict[str, list[str]] = {}
    for name, feeds in dict(raw).items():
        out[str(name)] = [str(f) for f in (feeds or [])]
    return out


def md_feeds_of(md: dict[str, list[str]] | list[str] | None) -> list[str]:
    """Every feed the document names, whatever instance holds it.

    Flat because that is what a strategy reads: ``TwapStrategy``,
    ``OneCancelOther`` and ``NoopStrategy`` all take ``md_ids[0]`` to find the
    instrument they were configured for. Which MD serves a feed is a
    deployment's business and never a strategy's.
    """
    if not md:
        return []
    if isinstance(md, list):
        return [str(f) for f in md]
    out: list[str] = []
    for feeds in md.values():
        out.extend(str(f) for f in feeds)
    return out


def md_instances_of(md: dict[str, list[str]] | list[str] | None) -> list[str]:
    """Instance names this document pins feeds to, unpinned last."""
    if not md or isinstance(md, list):
        return [ANY_INSTANCE] if md else []
    named = sorted(k for k in md if k != ANY_INSTANCE)
    return named + ([ANY_INSTANCE] if ANY_INSTANCE in md else [])


class StrategySpec(BaseModel):
    """Parsed strategy.yml document.

    ``td`` is account name → settings; ``md`` is instance name → feed keys,
    with a plain list meaning "any MD". ``sts`` is the strategy's own
    parameters.
    """

    model_config = ConfigDict(extra="forbid")

    td: dict[str, TdSettings] = Field(default_factory=dict)
    md: dict[str, list[str]] = Field(default_factory=dict)
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
    def _td_mapping(cls, value: Any) -> dict[str, Any]:
        """Account name → settings (resolved to api ids at deploy time)."""
        if value is None:
            return {}
        if isinstance(value, list):
            raise ValueError(_TD_LIST_HINT)
        if not isinstance(value, dict):
            raise ValueError(
                "td must be a mapping of account name to settings"
            )
        out: dict[str, Any] = {}
        for key, settings in value.items():
            if not isinstance(key, str):
                raise ValueError(
                    f"td account name must be a string, got {key!r}"
                )
            name = key.strip()
            if not name:
                raise ValueError("td account name must be a non-empty string")
            if name in out:
                raise ValueError(f"duplicate account name: {name!r}")
            if settings is None:
                settings = {}
            if not isinstance(settings, dict):
                raise ValueError(
                    f"td[{name!r}] settings must be a mapping, got {settings!r}"
                )
            out[name] = settings
        return out

    @field_validator("md", mode="before")
    @classmethod
    def _md_feeds(cls, value: Any) -> dict[str, list[str]]:
        """A list of feeds, or a mapping of instance name to feeds.

        The list form is not deprecated and will not be: it says the author
        does not care which MD serves these, which is the right thing to say
        for most deployments and the only thing every document written before
        instances existed could say.
        """
        if value is None:
            return {}
        if isinstance(value, list):
            return {ANY_INSTANCE: cls._md_feed_list(value, ANY_INSTANCE)}
        if not isinstance(value, dict):
            raise ValueError(
                "md must be a list of feed keys, or a mapping of instance "
                "name to feed keys"
            )
        out: dict[str, list[str]] = {}
        for key, feeds in value.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(
                    f"md instance name must be a non-empty string, got {key!r}"
                )
            name = key.strip()
            if name in out:
                raise ValueError(f"duplicate md instance name: {name!r}")
            if not isinstance(feeds, list):
                raise ValueError(
                    f"md[{name!r}] must be a list of feed keys, got {feeds!r}"
                )
            out[name] = cls._md_feed_list(feeds, name)

        seen: dict[str, str] = {}
        for name, feeds in out.items():
            for feed in feeds:
                if feed in seen:
                    # Two instances would each open a pump and each fan the
                    # same key out to this session, so the strategy would see
                    # every print twice — and refcounting cannot notice,
                    # because each instance counts its own.
                    raise ValueError(
                        f"feed {feed!r} is named by both {seen[feed]!r} and "
                        f"{name!r}; one feed is held by one instance"
                    )
                seen[feed] = name
        return out

    @staticmethod
    def _md_feed_list(feeds: Any, where: str) -> list[str]:
        out: list[str] = []
        for item in feeds:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    f"md[{where!r}] entry must be a non-empty string, "
                    f"got {item!r}"
                )
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
        _refuse_collapsing_keys(text)
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


def _refuse_collapsing_keys(text: str) -> None:
    """Refuse ``td:`` and ``md:`` mappings whose keys ``safe_load`` folds."""
    _refuse_collapsing_td_keys(text)
    _refuse_collapsing_md_keys(text)


def _refuse_collapsing_md_keys(text: str) -> None:
    """The same scan as :func:`_refuse_collapsing_td_keys`, for ``md:``.

    The consequence here is worse. A folded ``td:`` key loses one account's
    settings; a folded ``md:`` key loses **a whole instance's feed list**, and
    the deploy that follows attaches fewer feeds than the document asks for
    and says nothing about it. The strategy then runs on a subset of what it
    was configured with — the failure PI-8 catches at runtime, caught here
    before it starts.

    A list under ``md:`` has no keys to fold, and this finds none.
    """
    seen: set[str] = set()
    for key in _keys_under(text, "md"):
        name = key.strip()
        if name == _MERGE_KEY:
            raise StrategyYamlError(_MD_MERGE_HINT)
        if name in seen:
            raise StrategyYamlError(f"md: duplicate instance name: {name!r}")
        seen.add(name)


def _refuse_collapsing_td_keys(text: str) -> None:
    """Refuse a ``td:`` mapping whose keys ``safe_load`` would silently fold.

    Two ways one account can end up written twice and loaded once, and the
    loader reports neither. A repeated key keeps the last one. A merge key
    pulls an anchored mapping in, and an explicit key of the same name
    quietly wins over what was merged — so the settings a person wrote under
    the anchor are the ones that disappear.

    Harmless while settings are empty; disastrous once leverage lives here,
    which is the whole reason the scan runs before ``safe_load`` rather than
    inspecting what it returned.
    """
    seen: set[str] = set()
    for key in _keys_under(text, "td"):
        name = key.strip()
        if name == _MERGE_KEY:
            raise StrategyYamlError(
                "td: merge keys (<<) are not accepted — an account merged in "
                "from an anchor is silently replaced by an explicit key of "
                "the same name. Write each account out."
            )
        if name in seen:
            raise StrategyYamlError(f"td: duplicate account name: {name!r}")
        seen.add(name)


def _keys_under(text: str, field: str) -> list[str]:
    """Scalar keys under the root ``field:`` mapping, in document order.

    Parsed from the event stream rather than from what ``safe_load`` returned,
    because the whole point is to see the keys it would have folded.
    """
    events = list(yaml.parse(text, Loader=yaml.SafeLoader))
    i = 0
    while i < len(events) and not isinstance(events[i], MappingStartEvent):
        i += 1
    if i >= len(events):
        return []
    i += 1
    depth = 1
    expecting_key = True
    while i < len(events) and depth > 0:
        ev = events[i]
        if isinstance(ev, (MappingStartEvent, SequenceStartEvent)):
            depth += 1
            expecting_key = False
        elif isinstance(ev, (MappingEndEvent, SequenceEndEvent)):
            depth -= 1
            if depth == 1:
                expecting_key = True
        elif depth == 1 and isinstance(ev, (ScalarEvent, AliasEvent)):
            if expecting_key:
                if isinstance(ev, ScalarEvent) and ev.value == field:
                    nxt = i + 1
                    if nxt < len(events) and isinstance(
                        events[nxt], MappingStartEvent
                    ):
                        return _mapping_keys(events, nxt)
                    return []
                expecting_key = False
            else:
                expecting_key = True
        i += 1
    return []


def _mapping_keys(events: list[Any], start: int) -> list[str]:
    """Keys of the mapping that starts at ``events[start]``."""
    keys: list[str] = []
    i = start + 1
    depth = 1
    expecting_key = True
    while i < len(events) and depth > 0:
        ev = events[i]
        if isinstance(ev, (MappingStartEvent, SequenceStartEvent)):
            depth += 1
            expecting_key = False
        elif isinstance(ev, (MappingEndEvent, SequenceEndEvent)):
            depth -= 1
            if depth == 1:
                expecting_key = True
        elif depth == 1 and isinstance(ev, (ScalarEvent, AliasEvent)):
            if expecting_key:
                if isinstance(ev, ScalarEvent):
                    keys.append(str(ev.value))
                expecting_key = False
            else:
                expecting_key = True
        i += 1
    return keys


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
