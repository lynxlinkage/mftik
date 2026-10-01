"""strategy.yml — deployment document for TD + MD + STS.

Where a strategy runs, how it is configured, and what the platform does with the
run. Not *which* strategy — that is chosen at deploy time (see below).

    td:                            # account name -> per-account settings
      paper trader:
    md:                            # instance name -> what that MD serves
      md-jp:
        - ticker.Deribit_Perp_BTCUSD          # a feed
        - feed: trade.Deribit_Perp_BTCUSD     # a feed, delivered its own way
          delivery: latest                    # latest | kline | all
        - select: btc_chain                   # a set MD derives and re-derives
          kind: option_chain                  # option_chain | rolling_future
          ...
    restart: on_failure            # never (default) | on_failure
    max_restarts: 5                # inside restart_window_s, then it fails
    restart_window_s: 600
    start_timeout_s: 60            # on_start alone, at most 3600
    ready_timeout_s: 30            # the readiness conditions, after on_start
    limits:                        # RLIMIT_DATA and the offload pool sizes
      memory_mb: 2048
    sts:                           # the strategy's own parameters
      gap_bps: 10

Everything but ``td``, ``md`` and ``sts`` has a default, so a document that says
none of it still describes a complete deployment. The defaults are the plan's
(F11, F12, F33, §4.7, §5.5, §6.4) and are named as constants below.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
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

from mftik.exchange import venues
from mftik.exchange.errors import ExchangeError
from mftik.protocol.topics import Topics

#: The document carries no strategy type. Which strategy runs is chosen at
#: deploy time (``POST /sts/deploy/{type}``), because the type decides what
#: ``sts:`` may contain — a config written for one strategy is meaningless to
#: another, so pairing them in one editable blob invites documents that parse
#: but cannot run. See :mod:`mftik.protocol.strategy_catalog` for per-type
#: templates.


#: What becomes of this run after the process under it ends.
#:
#: The default, and what a document that says nothing gets: the run is over.
RESTART_NEVER = "never"
#: Hang the strategy up again **from scratch** after a crash it could be
#: hung up again from — an A-class crash, where the strategy's own code raised
#: and ``on_stop`` still ran (F11, §5.2). A B-class crash (a hook that blocked
#: the strategy loop past its hard limit) and a C-class one (the process died)
#: are failed instead: the cause is usually the same data and the same code, so
#: a restart tends to replay it, and the strategy never got to shut itself down.
#:
#: The new run starts at ``on_start`` with nothing carried over. It will see
#: positions the failed run left — the platform cancels that session's resting
#: orders before restarting, but a position cannot be cancelled away (R3).
RESTART_ON_FAILURE = "on_failure"
RESTART_MODES = frozenset({RESTART_NEVER, RESTART_ON_FAILURE})

#: How many restarts inside :data:`DEFAULT_RESTART_WINDOW_S` before the session
#: is failed instead of hung up again (F11).
DEFAULT_MAX_RESTARTS = 5
#: The window those restarts are counted in, in seconds (F11).
DEFAULT_RESTART_WINDOW_S = 600
#: Wall clock allowed for ``on_start`` alone, in seconds (F12). It is the one
#: hook with no deadline of its own: loading a model or warming up on a tape is
#: what it is for, so the document is what says how long that may take.
DEFAULT_START_TIMEOUT_S = 60
#: And the most a document may ask for. An hour is already long enough that a
#: deploy stuck in ``on_start`` looks like a hang; past it, the number is a
#: typo more often than an intention.
MAX_START_TIMEOUT_S = 3600
#: Wall clock allowed for the readiness conditions, counted from the moment
#: ``on_start`` returns (F12). TD is a hard condition and MD a soft one: an
#: account that has not reconciled by here fails the session, while feeds that
#: have not arrived are listed in ``ready.missing_feeds`` and ``on_ready`` is
#: called anyway (§5.2).
DEFAULT_READY_TIMEOUT_S = 30

#: Parallelism for ``self.offload`` in thread mode (§5.5).
DEFAULT_OFFLOAD_THREADS = 2
#: And in process mode (``isolate=True`` / ``offload_pool``).
DEFAULT_OFFLOAD_PROCESSES = 1

#: What ``mftik check`` prints for a document that still says
#: ``restart: always``. The parser prefixes the field name, so the line reads
#: back as the value the author wrote.
_OLD_RESTART_HINT = (
    "always is gone. It meant rebuild — resuming a run that was cut short "
    "with the state it had, which nothing does any more. The nearest thing "
    f"left is {RESTART_ON_FAILURE!r}, which starts a fresh run from on_start "
    "and inherits none of it; delete the line for the default, never."
)

#: How an event reaches the strategy when they arrive faster than its hooks
#: read them (§5.3). Each feed has a default by topic and may override it.
#:
#: ``latest`` keeps one event per feed and conflates before decoding: the next
#: push is the whole state again, so an older one is nothing to anybody.
DELIVERY_LATEST = "latest"
#: ``kline`` keeps the latest per ``(feed, bar open time)``, which is ``latest``
#: that cannot lose a closed bar.
DELIVERY_KLINE = "kline"
#: ``all`` queues in order and drops the oldest on overflow, counting what it
#: dropped. For trades: every print is its own fact, and a strategy spots what
#: it missed by the gap in ``event.seq`` (F25).
DELIVERY_ALL = "all"
DELIVERY_MODES = frozenset({DELIVERY_LATEST, DELIVERY_KLINE, DELIVERY_ALL})

#: ``select:`` kinds — a set of instruments MD derives and keeps derived,
#: rather than a list the document has to maintain (F33, §6.4).
SELECT_OPTION_CHAIN = "option_chain"
SELECT_ROLLING_FUTURE = "rolling_future"
SELECT_KINDS = frozenset({SELECT_OPTION_CHAIN, SELECT_ROLLING_FUTURE})

#: Which series a ``rolling_future`` follows, by how the venue spaces expiries.
TENOR_WEEKLY = "weekly"
TENOR_MONTHLY = "monthly"
TENOR_QUARTERLY = "quarterly"
TENORS = frozenset({TENOR_WEEKLY, TENOR_MONTHLY, TENOR_QUARTERLY})

#: Option sides, and the default: a chain with one side is a position in the
#: underlying with extra steps, so both is what ``sides:`` is usually left at.
OPTION_SIDES = ("C", "P")

#: ``field: (low, high)`` for :class:`StrategySpec`'s scalar policy fields.
#: Here rather than only on the fields so the refusal can be a sentence — the
#: cap on ``start_timeout_s`` is a decision, and a range error does not say so.
_SCALAR_BOUNDS: dict[str, tuple[int, int | None]] = {
    "max_restarts": (0, None),
    "restart_window_s": (1, None),
    "start_timeout_s": (1, MAX_START_TIMEOUT_S),
    "ready_timeout_s": (1, None),
}

#: ``2h``, ``30m``, ``90s``, ``3d`` — what a duration may be spelled as.
#:
#: Seconds everywhere would make ``min_tte: 7200`` and ``roll_before: 259200``
#: of the two numbers a reader most needs to recognise at a glance, which is why
#: the plan's own example writes them with units (§6.4). A bare number is still
#: accepted and read as seconds.
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DURATION_RE = re.compile(r"\A(\d+)([smhd])\Z")

#: A ``select:`` name, and what a strategy passes to ``self.md.universe``.
#: Narrow because it also ends up in log lines and status payloads keyed by it.
_SELECT_NAME_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]*\Z")

#: The keys that make an entry under ``md:`` something other than a feed key.
_SELECT_KEY = "select"
_FEED_KEY = "feed"
_DELIVERY_KEY = "delivery"
_FEED_ENTRY_KEYS = frozenset({_FEED_KEY, _DELIVERY_KEY})

#: What each ``select:`` kind takes. Spelled out so an unknown key is refused
#: by name with the alternatives — a selector that silently ignored
#: ``expiry: 2`` would derive a universe nobody asked for.
_OPTION_CHAIN_KEYS = frozenset(
    {
        _SELECT_KEY,
        "kind",
        "venue",
        "underlying",
        "ref",
        "expiries",
        "strikes",
        "sides",
        "topics",
        "recenter",
    }
)
_ROLLING_FUTURE_KEYS = frozenset(
    {_SELECT_KEY, "kind", "venue", "underlying", "tenor", "roll_before", "topics"}
)
_EXPIRIES_KEYS = frozenset({"nearest", "min_tte"})
_STRIKES_KEYS = frozenset({"atm"})
_RECENTER_KEYS = frozenset({"strikes", "min_dwell"})

#: The two fields :func:`parse_strategy_yml` lifts out of ``md:``. Named here
#: because the lift also has to refuse them as document keys.
_MD_SELECT_FIELD = "md_select"
_MD_DELIVERY_FIELD = "md_delivery"

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


class Limits(BaseModel):
    """``limits:`` — the ceilings one session's worker runs under.

    None of these are performance tuning. ``memory_mb`` is what turns an
    out-of-memory session from a SIGKILL into a ``MemoryError`` the strategy can
    log before it fails (§4.7), and the two offload counts are what decide how
    much work can be outside the strategy loop at once (§5.5). Both memory
    limits are unset by default: a ceiling nobody chose is a session killed at a
    number nobody chose.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    #: ``RLIMIT_DATA`` for the session worker, applied by the shim before
    #: ``exec``. Not ``RLIMIT_AS``: numpy and torch reserve address space they
    #: never touch, so a virtual-size limit refuses sessions that would have fit.
    memory_mb: int | None = Field(default=None, ge=1)
    #: Threads behind ``await self.offload(...)``.
    offload_threads: int = Field(default=DEFAULT_OFFLOAD_THREADS, ge=1)
    #: Processes behind ``offload(..., isolate=True)`` and ``offload_pool``.
    offload_processes: int = Field(default=DEFAULT_OFFLOAD_PROCESSES, ge=1)
    #: ``RLIMIT_DATA`` for each of those processes. They are already the first
    #: thing the kernel kills under pressure (``oom_score_adj`` +900), and a
    #: strategy survives losing one — it gets ``OffloadWorkerLost``.
    offload_memory_mb: int | None = Field(default=None, ge=1)

    @field_validator(
        "memory_mb", "offload_memory_mb", "offload_threads", "offload_processes",
        mode="before",
    )
    @classmethod
    def _counted(cls, value: Any, info: ValidationInfo) -> Any:
        """A whole number, said in a sentence rather than as a type error.

        Zero is refused rather than read as "off": a pool with no workers is a
        strategy whose first ``offload`` never returns.
        """
        if value is None:
            return cls.model_fields[str(info.field_name)].default
        return _whole(value, low=1)


class ChainExpiries(BaseModel):
    """``expiries:`` — which expiries an option chain covers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    #: How many expiries, counting from the front of the board.
    nearest: int = Field(ge=1)
    #: Skip an expiry closer to settlement than this. A chain that followed the
    #: board to the last hour would spend that hour on instruments nobody can
    #: get out of; with ``min_tte`` it has already moved on to the next expiry.
    min_tte_s: int = Field(default=0, ge=0)


class ChainStrikes(BaseModel):
    """``strikes:`` — how wide around the money, per expiry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    #: Strikes either side of the money, counted in listed strikes rather than
    #: in price: the spacing is the venue's and it is not uniform across a
    #: board. ``0`` is the nearest listed strike alone.
    atm: int = Field(ge=0)


class ChainRecenter(BaseModel):
    """``recenter:`` — when the chain is allowed to follow the reference price.

    Without this a chain re-centres on every tick that crosses a strike
    boundary, and each re-centre subscribes and unsubscribes a row of
    instruments. The defaults are the plan's example (§6.4): the plan does not
    name defaults, and these are the values it chose when it wrote them out.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    #: How far the reference has to leave the current centre, in listed strikes.
    strikes: int = Field(default=1, ge=1)
    #: And the least time between two re-centres.
    min_dwell_s: int = Field(default=60, ge=0)


class OptionChainSelect(BaseModel):
    """``select:`` with ``kind: option_chain`` — an option board, kept centred.

    What MD derives from it is a set of atoms; what the strategy sees is
    ``self.md.universe(name)`` and ``on_universe_change`` (F33). Nothing here is
    resolved at parse time: the listings and the reference price it needs both
    live in MD, and a document parsed on a laptop has neither.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["option_chain"] = SELECT_OPTION_CHAIN
    #: What the strategy passes to ``self.md.universe``.
    name: str
    venue: str
    #: The asset the options are on — ``BTC``, not an instrument.
    underlying: str
    #: Feed key of the price the chain centres on. MD subscribes to it for the
    #: selector, so the document does not list it among its own feeds and the
    #: strategy does not receive it unless it asks for it separately.
    ref: str
    expiries: ChainExpiries
    strikes: ChainStrikes
    sides: tuple[str, ...] = OPTION_SIDES
    #: Which topics to carry for each selected instrument.
    topics: tuple[str, ...]
    recenter: ChainRecenter = Field(default_factory=ChainRecenter)


class RollingFutureSelect(BaseModel):
    """``select:`` with ``kind: rolling_future`` — one series, followed forward.

    The contract that is current rolls before expiry, and the one it rolled off
    stays until it settles: a position in it is still a position, and a roll
    with one side of the market missing is a roll done blind (§6.4).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["rolling_future"] = SELECT_ROLLING_FUTURE
    name: str
    venue: str
    underlying: str
    #: Which series: ``weekly`` | ``monthly`` | ``quarterly``.
    tenor: str
    #: How long before expiry ``current`` moves to the next contract. ``0``
    #: rolls at settlement, which is late enough to be a decision.
    roll_before_s: int = Field(default=0, ge=0)
    topics: tuple[str, ...]


#: One ``select:`` block, discriminated by ``kind`` so the normalized form of a
#: document reads back into the same model it parsed into.
MdSelect = Annotated[
    OptionChainSelect | RollingFutureSelect, Field(discriminator="kind")
]


def md_selects_of(
    md_select: dict[str, list[MdSelect]] | None,
) -> list[MdSelect]:
    """Every selector the document declares, whatever instance holds it.

    The flat counterpart to :func:`md_feeds_of`, and flat for the same reason:
    a selector's name is what a strategy looks it up by, and which MD derives
    it is a deployment's business.
    """
    out: list[MdSelect] = []
    for selects in (md_select or {}).values():
        out.extend(selects)
    return out


class StrategySpec(BaseModel):
    """Parsed strategy.yml document.

    ``td`` is account name → settings; ``md`` is instance name → feed keys,
    with a plain list meaning "any MD". ``sts`` is the strategy's own
    parameters.

    ``md_select`` and ``md_delivery`` are not keys anybody writes. They are the
    two halves of ``md:`` that are not feed keys — a ``select:`` block and a
    feed's ``delivery:`` override — lifted out by :func:`parse_strategy_yml` so
    that ``md`` stays instance → feed keys for everything downstream of it.
    """

    model_config = ConfigDict(extra="forbid")

    td: dict[str, TdSettings] = Field(default_factory=dict)
    md: dict[str, list[str]] = Field(default_factory=dict)
    #: Instance name → the ``select:`` blocks pinned to it (§6.4).
    md_select: dict[str, list[MdSelect]] = Field(default_factory=dict)
    #: Feed key → delivery mode, for the feeds that said one (§5.3). A feed
    #: absent from here takes the default for its topic, which is what almost
    #: every feed should be left at.
    md_delivery: dict[str, str] = Field(default_factory=dict)
    #: What becomes of this run after the process under it ends: ``never``
    #: (default) or ``on_failure`` (F11).
    restart: str = RESTART_NEVER
    #: Restarts allowed inside ``restart_window_s`` before the session is
    #: failed instead. Counted even when ``restart`` is ``never``, where there
    #: is nothing to count — a document may carry the policy it would use.
    max_restarts: int = Field(default=DEFAULT_MAX_RESTARTS, ge=0)
    restart_window_s: int = Field(default=DEFAULT_RESTART_WINDOW_S, ge=1)
    #: Wall clock for ``on_start`` alone, capped at :data:`MAX_START_TIMEOUT_S`.
    start_timeout_s: int = Field(
        default=DEFAULT_START_TIMEOUT_S, ge=1, le=MAX_START_TIMEOUT_S
    )
    #: Wall clock for the readiness conditions, from when ``on_start`` returns.
    ready_timeout_s: int = Field(default=DEFAULT_READY_TIMEOUT_S, ge=1)
    limits: Limits = Field(default_factory=Limits)
    #: Flat config for whichever strategy is being deployed. Its keys are the
    #: strategy's own; validation happens in that class's ``on_initialized``.
    sts: dict[str, Any] = Field(default_factory=dict)

    @field_validator("restart", mode="before")
    @classmethod
    def _restart_mode(cls, value: Any) -> str:
        if value is None:
            return RESTART_NEVER
        mode = str(value).strip().lower()
        if mode == "always":
            raise ValueError(_OLD_RESTART_HINT)
        if mode not in RESTART_MODES:
            raise ValueError(
                f"restart must be one of {sorted(RESTART_MODES)}, got {value!r}"
            )
        return mode

    @field_validator(
        "max_restarts", "restart_window_s", "start_timeout_s", "ready_timeout_s",
        mode="before",
    )
    @classmethod
    def _counted(cls, value: Any, info: ValidationInfo) -> Any:
        """A whole number in range, said in a sentence.

        Rather than left to pydantic, whose integer errors read as type
        failures. The ceiling on ``start_timeout_s`` is the one that has to
        explain itself, and it cannot do that from a constraint.
        """
        name = str(info.field_name)
        if value is None:
            return cls.model_fields[name].default
        low, high = _SCALAR_BOUNDS[name]
        return _whole(value, low=low, high=high)

    @field_validator("limits", mode="before")
    @classmethod
    def _limits_mapping(cls, value: Any) -> Any:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError(
                "limits must be a mapping of limit name to value, got "
                f"{value!r}"
            )
        return value

    @field_validator("md_delivery", mode="before")
    @classmethod
    def _delivery_modes(cls, value: Any) -> dict[str, str]:
        """Checked again here because this is also the normalized form.

        :func:`parse_strategy_yml` has already refused a bad mode written under
        ``md:``; what this catches is the same document arriving as the mapping
        it parsed into.
        """
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("md_delivery must be a mapping of feed to mode")
        out: dict[str, str] = {}
        for feed, mode in value.items():
            out[str(feed)] = _delivery_mode(mode, where=str(feed))
        return out

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
            try:
                out.append(_normalize_feed(item))
            except StrategyYamlError as exc:
                raise ValueError(f"md entry {exc}") from exc
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
    raw = _lift_md_entries(raw)
    try:
        return StrategySpec.model_validate(raw)
    except ValidationError as exc:
        raise StrategyYamlError(_readable(exc)) from exc
    except Exception as exc:
        raise StrategyYamlError(str(exc)) from exc


def _lift_md_entries(raw: dict[str, Any]) -> dict[str, Any]:
    """Split ``md:`` into feed keys, ``select:`` blocks and delivery overrides.

    Here rather than in a validator because the three come out of one list and
    land in three fields, and because a refusal about a ``select:`` block reads
    better written out than assembled from a field path.

    ``md:`` holds three kinds of entry::

        md:
          md-jp:
            - ticker.Deribit_Perp_BTCUSD          # a feed
            - feed: trade.Deribit_Perp_BTCUSD     # a feed, delivered its way
              delivery: latest
            - select: btc_chain                   # a set MD derives
              kind: option_chain
              ...

    What the model then sees under ``md`` is only the feed keys, which is what
    every reader of it already expects.
    """
    if _MD_SELECT_FIELD in raw or _MD_DELIVERY_FIELD in raw:
        raise StrategyYamlError(
            f"{_MD_SELECT_FIELD} / {_MD_DELIVERY_FIELD} are not document keys: "
            f"a derived set of instruments is a 'select:' entry under md:, and "
            f"a feed's delivery is 'delivery:' on that feed's entry"
        )
    value = raw.get("md")
    if isinstance(value, list):
        lists = {ANY_INSTANCE: value}
    elif isinstance(value, dict):
        lists = {
            name: feeds
            for name, feeds in value.items()
            if isinstance(name, str) and isinstance(feeds, list)
        }
    else:
        # Not a shape this can read; ``_md_feeds`` is what says so.
        return raw

    feeds: dict[str, list[Any]] = {}
    selects: dict[str, list[Any]] = {}
    delivery: dict[str, str] = {}
    named: dict[str, str] = {}
    for instance, entries in lists.items():
        # A plain list named no instance, so neither does a refusal about it.
        held = "md" if isinstance(value, list) else f"md[{instance.strip()!r}]"
        kept: list[Any] = []
        found: list[Any] = []
        for position, entry in enumerate(entries, start=1):
            where = f"{held} entry {position}"
            if not isinstance(entry, dict):
                kept.append(entry)
                continue
            if _SELECT_KEY in entry and _FEED_KEY in entry:
                raise StrategyYamlError(
                    f"{where} names both a feed and a select; one entry is one "
                    f"or the other"
                )
            if _SELECT_KEY in entry:
                select = _parse_select(entry, where)
                if select.name in named:
                    raise StrategyYamlError(
                        f"{where}: select {select.name!r} is already declared "
                        f"under {named[select.name]}; a name is what the "
                        f"strategy looks one up by, so it names one set"
                    )
                named[select.name] = held
                found.append(select)
                continue
            if _FEED_KEY not in entry:
                raise StrategyYamlError(
                    f"{where} is a mapping with no {_FEED_KEY!r} and no "
                    f"{_SELECT_KEY!r}; it takes one of them"
                )
            feed, mode = _parse_feed_entry(entry, where)
            kept.append(feed)
            if mode is not None:
                delivery[feed] = mode
        feeds[instance] = kept
        if found:
            selects[instance] = found

    out = dict(raw)
    if isinstance(value, list):
        out["md"] = feeds[ANY_INSTANCE]
    else:
        out["md"] = {**value, **feeds}
    if selects:
        out[_MD_SELECT_FIELD] = selects
    if delivery:
        out[_MD_DELIVERY_FIELD] = delivery
    return out


def _parse_feed_entry(entry: dict[Any, Any], where: str) -> tuple[str, str | None]:
    """``{feed: ..., delivery: ...}`` → the canonical feed key and its mode."""
    unknown = sorted(str(k) for k in entry if k not in _FEED_ENTRY_KEYS)
    if unknown:
        raise StrategyYamlError(
            f"{where} does not take {unknown[0]!r}; a feed entry takes "
            f"{sorted(_FEED_ENTRY_KEYS)}"
        )
    raw_feed = entry.get(_FEED_KEY)
    if not isinstance(raw_feed, str) or not raw_feed.strip():
        raise StrategyYamlError(
            f"{where}: {_FEED_KEY} must be a non-empty string, got {raw_feed!r}"
        )
    try:
        feed = _normalize_feed(raw_feed)
    except StrategyYamlError as exc:
        raise StrategyYamlError(f"{where}: {_FEED_KEY} {exc}") from exc
    if _DELIVERY_KEY not in entry or entry[_DELIVERY_KEY] is None:
        # Written out as a mapping for the sake of one key it did not then
        # write. Harmless, and the same feed either way.
        return feed, None
    return feed, _delivery_mode(entry[_DELIVERY_KEY], where=where)


def _parse_select(entry: dict[Any, Any], where: str) -> MdSelect:
    """One ``select:`` block → the model for its ``kind``."""
    name = _select_name(entry.get(_SELECT_KEY), where)
    at = f"{where} (select {name!r})"
    kind = entry.get("kind")
    if not isinstance(kind, str) or kind.strip() not in SELECT_KINDS:
        raise StrategyYamlError(
            f"{at}: kind must be one of {sorted(SELECT_KINDS)}, got {kind!r}"
        )
    kind = kind.strip()
    allowed = (
        _OPTION_CHAIN_KEYS if kind == SELECT_OPTION_CHAIN else _ROLLING_FUTURE_KEYS
    )
    unknown = sorted(str(k) for k in entry if k not in allowed)
    if unknown:
        raise StrategyYamlError(
            f"{at}: {kind} does not take {unknown[0]!r}; it takes "
            f"{sorted(allowed - {_SELECT_KEY, 'kind'})}"
        )
    common = {
        "name": name,
        "venue": _select_venue(_required(entry, "venue", at), at),
        "underlying": _underlying(_required(entry, "underlying", at), at),
        "topics": _topics(_required(entry, "topics", at), at),
    }
    if kind == SELECT_ROLLING_FUTURE:
        tenor = _required(entry, "tenor", at)
        if not isinstance(tenor, str) or tenor.strip().lower() not in TENORS:
            raise StrategyYamlError(
                f"{at}: tenor must be one of {sorted(TENORS)}, got {tenor!r}"
            )
        return RollingFutureSelect(
            tenor=tenor.strip().lower(),
            roll_before_s=_seconds(
                entry.get("roll_before", 0), where=f"{at}: roll_before"
            ),
            **common,
        )

    expiries = _sub_mapping(entry, "expiries", at, _EXPIRIES_KEYS, required=True)
    strikes = _sub_mapping(entry, "strikes", at, _STRIKES_KEYS, required=True)
    recenter = _sub_mapping(entry, "recenter", at, _RECENTER_KEYS, required=False)
    return OptionChainSelect(
        ref=_select_ref(_required(entry, "ref", at), at),
        expiries=ChainExpiries(
            nearest=_whole(
                _required(expiries, "nearest", f"{at}: expiries"),
                low=1,
                where=f"{at}: expiries.nearest",
            ),
            min_tte_s=_seconds(
                expiries.get("min_tte", 0), where=f"{at}: expiries.min_tte"
            ),
        ),
        strikes=ChainStrikes(
            atm=_whole(
                _required(strikes, "atm", f"{at}: strikes"),
                low=0,
                where=f"{at}: strikes.atm",
            )
        ),
        sides=_sides(entry.get("sides"), at),
        recenter=ChainRecenter(
            strikes=_whole(
                recenter.get("strikes", 1), low=1, where=f"{at}: recenter.strikes"
            ),
            min_dwell_s=_seconds(
                recenter.get("min_dwell", 60), where=f"{at}: recenter.min_dwell"
            ),
        ),
        **common,
    )


def _required(entry: dict[Any, Any], key: str, at: str) -> Any:
    value = entry.get(key)
    if value is None:
        raise StrategyYamlError(f"{at}: {key} is required")
    return value


def _sub_mapping(
    entry: dict[Any, Any], key: str, at: str, allowed: frozenset[str], *, required: bool
) -> dict[Any, Any]:
    value = entry.get(key)
    if value is None:
        if required:
            raise StrategyYamlError(f"{at}: {key} is required")
        return {}
    if not isinstance(value, dict):
        raise StrategyYamlError(
            f"{at}: {key} must be a mapping of {sorted(allowed)}, got {value!r}"
        )
    unknown = sorted(str(k) for k in value if k not in allowed)
    if unknown:
        raise StrategyYamlError(
            f"{at}: {key} does not take {unknown[0]!r}; it takes {sorted(allowed)}"
        )
    return value


def _select_name(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _SELECT_NAME_RE.match(value.strip()):
        raise StrategyYamlError(
            f"{where}: select must be a name of letters, digits, '_' and '-' "
            f"(e.g. 'btc_chain'), got {value!r}"
        )
    return value.strip()


def _select_venue(value: Any, at: str) -> str:
    """The venue the listings come from, checked against the venue registry.

    A typo here would otherwise surface as an empty universe after the deploy
    had already started — a selector that derives nothing looks exactly like a
    board with nothing on it.
    """
    try:
        return venues.require(str(value)).name
    except ExchangeError as exc:
        raise StrategyYamlError(f"{at}: venue {exc}") from exc


def _underlying(value: Any, at: str) -> str:
    text = str(value).strip().upper()
    if not text or any(c in text for c in " \t_.-"):
        raise StrategyYamlError(
            f"{at}: underlying must be an asset code (e.g. 'BTC'), got {value!r}"
        )
    return text


def _select_ref(value: Any, at: str) -> str:
    try:
        return _normalize_feed(value)
    except StrategyYamlError as exc:
        raise StrategyYamlError(f"{at}: ref {exc}") from exc


def _topics(value: Any, at: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise StrategyYamlError(
            f"{at}: topics must be a non-empty list of feed topics "
            f"(e.g. [ticker, greeks]), got {value!r}"
        )
    out: list[str] = []
    for item in value:
        topic = str(item).strip() if isinstance(item, str) else ""
        if not topic or any(c in topic for c in " \t."):
            raise StrategyYamlError(
                f"{at}: topic must be the topic half of a feed key "
                f"(e.g. 'ticker', 'kline_1m'), got {item!r}"
            )
        if topic in out:
            raise StrategyYamlError(f"{at}: topic {topic!r} is listed twice")
        out.append(topic)
    return tuple(out)


def _sides(value: Any, at: str) -> tuple[str, ...]:
    if value is None:
        return OPTION_SIDES
    if not isinstance(value, list) or not value:
        raise StrategyYamlError(
            f"{at}: sides must be a non-empty list of {list(OPTION_SIDES)}, "
            f"got {value!r}"
        )
    out: list[str] = []
    for item in value:
        side = str(item).strip().upper()
        if side not in OPTION_SIDES:
            raise StrategyYamlError(
                f"{at}: side must be one of {list(OPTION_SIDES)} "
                f"(call, put), got {item!r}"
            )
        if side in out:
            raise StrategyYamlError(f"{at}: side {side!r} is listed twice")
        out.append(side)
    return tuple(out)


def _normalize_feed(value: Any) -> str:
    """A feed key as a person typed it → the canonical spelling.

    Normalized, not just checked: this is YAML a person typed, and what comes
    out is what MD keys on. A ticker typed in lower case and one typed
    canonically have to end up as one feed, or a single instrument runs two
    subscriptions.

    The message starts mid-sentence so a caller can say where it came from.
    """
    try:
        return Topics.normalize_md_feed(str(value).strip())
    except Exception as exc:
        raise StrategyYamlError(
            f"must be topic.UniversalTicker (e.g. "
            f"bestquote.Gate_Spot_BTCUSDT), got {value!r}: {exc}"
        ) from exc


def _delivery_mode(value: Any, where: str) -> str:
    mode = str(value).strip().lower() if value is not None else ""
    if mode not in DELIVERY_MODES:
        raise StrategyYamlError(
            f"{where}: delivery must be one of {sorted(DELIVERY_MODES)}, "
            f"got {value!r}"
        )
    return mode


def _whole(value: Any, *, low: int, high: int | None = None, where: str = "") -> int:
    """A plain whole number in range. ``where`` names it when nothing else will.

    ``bool`` is refused by name: it is an ``int`` to Python, so ``true`` under a
    count would otherwise quietly mean 1.
    """
    prefix = f"{where} " if where else ""
    if isinstance(value, bool) or not isinstance(value, int):
        raise StrategyYamlError(f"{prefix}must be a whole number, got {value!r}")
    if value < low or (high is not None and value > high):
        bound = f"between {low} and {high}" if high is not None else f"at least {low}"
        raise StrategyYamlError(f"{prefix}must be {bound}, got {value}")
    return value


def _seconds(value: Any, where: str) -> int:
    """``2h`` / ``30m`` / ``90s`` / ``3d``, or a bare number of seconds."""
    if isinstance(value, str):
        found = _DURATION_RE.match(value.strip())
        if found is None:
            raise StrategyYamlError(
                f"{where} must be a duration like '2h', '30m', '90s', '3d', or "
                f"a whole number of seconds, got {value!r}"
            )
        return int(found.group(1)) * _DURATION_UNITS[found.group(2)]
    return _whole(value, low=0, where=where)


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
