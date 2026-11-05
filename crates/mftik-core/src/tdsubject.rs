//! The C6 subjects of one TD account (PROTOCOL §7, E21-D006, E21-D007,
//! E21-D042–E21-D044). This is the Go package `contract/wire/tdsubject`.
//!
//! A subject is `td.{order,view,evt}.{td_instance}.{api_id}`. The caller
//! supplies `td_instance`. This module does not guess one and does not fall
//! back to `td.{order,view,evt}.{api_id}`. `td_instance` is one NATS token:
//! empty, `.`, `*`, `>`, and whitespace are refused and not escaped (E21-D043).

use std::fmt;

/// Why a subject could not be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// `td_instance` is empty or contains `.`, `*`, `>`, or whitespace.
    Instance(String),
    /// `api_id` is not greater than 0.
    ApiId(i64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instance(inst) => write!(
                f,
                "tdsubject: td_instance {inst:?}: tdsubject: td_instance is empty or contains '.', '*', '>' or whitespace"
            ),
            Self::ApiId(api_id) => write!(
                f,
                "tdsubject: api_id {api_id}: tdsubject: api_id must be positive"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Rejects a `td_instance` that cannot be one NATS subject token (E21-D043).
pub fn token(inst: &str) -> Result<(), Error> {
    // char::is_whitespace is the Unicode White_Space property, as Go's
    // unicode.IsSpace is.
    if inst.is_empty()
        || inst
            .chars()
            .any(|c| matches!(c, '.' | '*' | '>') || c.is_whitespace())
    {
        return Err(Error::Instance(inst.to_owned()));
    }
    Ok(())
}

/// `td.order.{td_instance}.{api_id}`.
pub fn order(inst: &str, api_id: i64) -> Result<String, Error> {
    subject("order", inst, api_id)
}

/// `td.view.{td_instance}.{api_id}`.
pub fn view(inst: &str, api_id: i64) -> Result<String, Error> {
    subject("view", inst, api_id)
}

/// `td.evt.{td_instance}.{api_id}`.
pub fn evt(inst: &str, api_id: i64) -> Result<String, Error> {
    subject("evt", inst, api_id)
}

fn subject(kind: &str, inst: &str, api_id: i64) -> Result<String, Error> {
    token(inst)?;
    if api_id <= 0 {
        return Err(Error::ApiId(api_id));
    }
    Ok(format!("td.{kind}.{inst}.{api_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    type Build = fn(&str, i64) -> Result<String, Error>;

    #[test]
    fn subjects() {
        assert_eq!(order("td-a", 7).unwrap(), "td.order.td-a.7");
        assert_eq!(view("td-a", 7).unwrap(), "td.view.td-a.7");
        assert_eq!(evt("td-a", 7).unwrap(), "td.evt.td-a.7");
    }

    #[test]
    fn token_refuses() {
        let builds: [Build; 3] = [order, view, evt];
        for inst in [
            "",
            "td.a",
            "td*a",
            "td>a",
            "td a",
            "td\ta",
            "td\u{a0}a",
            "td\u{85}a",
            "td\u{3000}a",
        ] {
            for build in builds {
                assert_eq!(
                    build(inst, 7),
                    Err(Error::Instance(inst.to_owned())),
                    "{inst:?}"
                );
            }
            assert_eq!(
                token(inst),
                Err(Error::Instance(inst.to_owned())),
                "{inst:?}"
            );
        }
        assert_eq!(order("td-a", 0), Err(Error::ApiId(0)));
        assert_eq!(order("td-a", -1), Err(Error::ApiId(-1)));
    }

    #[test]
    fn instance_is_checked_before_api_id() {
        assert_eq!(order("", 0), Err(Error::Instance(String::new())));
    }

    #[test]
    fn error_text_matches_go() {
        assert_eq!(
            Error::Instance("td.a".into()).to_string(),
            "tdsubject: td_instance \"td.a\": tdsubject: td_instance is empty or contains '.', '*', '>' or whitespace"
        );
        assert_eq!(
            Error::ApiId(0).to_string(),
            "tdsubject: api_id 0: tdsubject: api_id must be positive"
        );
    }
}
