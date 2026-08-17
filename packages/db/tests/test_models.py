from mftik_db.models import (
    Account,
    Api,
    ApiType,
    Audit,
    Base,
    MdSessionRow,
    SessionDomain,
    SessionStatus,
    StrategyRow,
    StsSessionRow,
    TdSessionRow,
    User,
)


def test_metadata_includes_split_session_tables() -> None:
    tables = set(Base.metadata.tables)
    assert {
        "users",
        "apis",
        "accounts",
        "audits",
        "sts_sessions",
        "td_sessions",
        "md_sessions",
        "strategies",
    } <= tables
    assert "sessions" not in tables


def test_api_owner_fk_and_type() -> None:
    assert Api.__tablename__ == "apis"
    assert ApiType.HMAC.value == "HMAC"
    assert ApiType.ED25519.value == "ED25519"
    assert "venue" in Api.__table__.c
    assert not Api.__table__.c.venue.nullable
    owner_col = Api.__table__.c.owner_id
    assert owner_col.foreign_keys
    assert "users.id" in {str(fk.column) for fk in owner_col.foreign_keys}


def test_audit_user_fk() -> None:
    assert Audit.__tablename__ == "audits"
    user_col = Audit.__table__.c.user_id
    assert "users.id" in {str(fk.column) for fk in user_col.foreign_keys}


def test_user_relationships() -> None:
    assert "apis" in User.__mapper__.relationships
    assert "audits" in User.__mapper__.relationships
    assert "sts_sessions" in User.__mapper__.relationships
    assert "td_sessions" in User.__mapper__.relationships
    assert "md_sessions" in User.__mapper__.relationships
    assert "strategies" in User.__mapper__.relationships
    assert "accounts" in User.__mapper__.relationships


def test_account_api_one_to_one() -> None:
    assert Account.__tablename__ == "accounts"
    cols = set(Account.__table__.c.keys())
    assert {"id", "name", "api_id", "created_by", "created_at"} <= cols
    api_fk = Account.__table__.c.api_id
    assert "apis.id" in {str(fk.column) for fk in api_fk.foreign_keys}
    assert api_fk.unique
    assert Account.__table__.c.name.unique
    assert "account" in Api.__mapper__.relationships
    assert "api" in Account.__mapper__.relationships


def test_strategy_row_columns() -> None:
    assert StrategyRow.__tablename__ == "strategies"
    cols = set(StrategyRow.__table__.c.keys())
    assert {
        "id",
        "type",
        "config",
        "created_by",
        "created_at",
        "sts_session",
    } <= cols
    sts_fk = StrategyRow.__table__.c.sts_session
    assert "sts_sessions.session_id" in {
        str(fk.column) for fk in sts_fk.foreign_keys
    }
    assert "strategy_row" in StsSessionRow.__mapper__.relationships


def test_session_row_columns() -> None:
    assert StsSessionRow.__tablename__ == "sts_sessions"
    assert TdSessionRow.__tablename__ == "td_sessions"
    assert MdSessionRow.__tablename__ == "md_sessions"

    sts_cols = set(StsSessionRow.__table__.c.keys())
    assert {
        "session_id",
        "created_by",
        "created_at",
        "finished_at",
        "status",
        "strategy",
        "td_api_ids",
        "md_ids",
        "st_paras",
    } <= sts_cols

    td_cols = set(TdSessionRow.__table__.c.keys())
    assert {
        "id",
        "session_id",
        "created_by",
        "created_at",
        "finished_at",
        "status",
        "api_id",
    } <= td_cols
    assert "sts_session_id" not in td_cols
    assert TdSessionRow.__table__.c.id.primary_key
    assert not TdSessionRow.__table__.c.session_id.primary_key

    md_cols = set(MdSessionRow.__table__.c.keys())
    assert {
        "id",
        "venue",
        "session_id",
        "created_by",
        "created_at",
        "finished_at",
        "status",
    } <= md_cols
    assert MdSessionRow.__table__.c.id.primary_key
    assert not MdSessionRow.__table__.c.session_id.primary_key

    assert SessionDomain.TD.value == "td"
    assert SessionStatus.LIVE.value == "live"


def test_symbol_category_matches_the_ticker_vocabulary() -> None:
    """``mftik_db`` does not depend on ``mftik-common``, so nothing imports one
    into the other — but these values are the middle part of a stored universal
    ticker, and a drift would write rows nothing can parse."""
    from mftik.exchange.tickers import Category
    from mftik_db.models.symbol import SymbolCategory

    assert {c.value for c in SymbolCategory} == {c.value for c in Category}

