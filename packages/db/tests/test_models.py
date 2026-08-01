from mft_db.models import (
    Api,
    ApiType,
    Audit,
    Base,
    MdSessionRow,
    SessionDomain,
    SessionStatus,
    StsSessionRow,
    TdSessionRow,
    User,
)


def test_metadata_includes_split_session_tables() -> None:
    tables = set(Base.metadata.tables)
    assert {
        "users",
        "apis",
        "audits",
        "sts_sessions",
        "td_sessions",
        "md_sessions",
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
