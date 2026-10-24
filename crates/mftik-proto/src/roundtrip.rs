//! Encode and decode `worker.v1` and `stsctl.v1`.
//!
//! Fixtures are short fixed values. Each one sets a field that protobuf keeps
//! on the wire, then checks that decode returns the same message.

use prost::Message;

use crate::wire::mftiko::common::v1::{Outcome, Version, WorkerRef};
use crate::wire::mftiko::session::v1::{AccountDecl, FeedDecl, StrategyRef};
use crate::wire::mftiko::stsctl::v1::{
    AccountBinding, BindingState, Bindings, Command, Fail, FeedBinding, InitPayload,
    NatsCredentials, QueueStat, Report, SelectorBinding, StartPayload, Universe, command,
};
use crate::wire::mftiko::worker::v1::{
    Applied, Beat, Controller, Done, Exiting, Hello, Init, Live, Relay, Report as WorkerReport,
    Stage, Start, Stop, Stopped, Undeliverable, Verb, WorkerDown, WorkerUp, worker_down, worker_up,
};

fn round_trip<M>(message: &M)
where
    M: Message + Default + PartialEq,
{
    let encoded = message.encode_to_vec();
    assert!(
        !encoded.is_empty(),
        "fixture must set a field that is present on the wire"
    );
    let decoded = M::decode(encoded.as_slice()).expect("decode");
    if decoded != *message {
        panic!(
            "round trip changed the message\nencoded: {encoded:?}\ndecoded: {:?}",
            decoded.encode_to_vec()
        );
    }
}

fn up(msg: worker_up::Msg) -> WorkerUp {
    WorkerUp { msg: Some(msg) }
}

fn down(msg: worker_down::Msg) -> WorkerDown {
    WorkerDown { msg: Some(msg) }
}

#[test]
fn worker_up_hello_round_trip() {
    round_trip(&up(worker_up::Msg::Hello(Hello {
        kind: "session".into(),
        release: "v1".into(),
        contracts: vec!["c1".into(), "c2".into()],
        pid: 7,
    })));
}

#[test]
fn worker_up_done_round_trip() {
    round_trip(&up(worker_up::Msg::Done(Done {
        verb: Verb::Init as i32,
        ok: true,
        error: "e".into(),
    })));
}

#[test]
fn worker_up_stage_round_trip() {
    round_trip(&up(worker_up::Msg::Stage(Stage {
        name: "load".into(),
    })));
}

#[test]
fn worker_up_beat_round_trip() {
    round_trip(&up(worker_up::Msg::Beat(Beat {
        seq: 3,
        progress: vec![1, 2],
    })));
}

#[test]
fn worker_up_report_round_trip() {
    round_trip(&up(worker_up::Msg::Report(WorkerReport {
        contract: "c1".into(),
        payload: vec![3],
    })));
}

#[test]
fn worker_up_relay_round_trip() {
    round_trip(&up(worker_up::Msg::Relay(Relay {
        contract: "c1".into(),
        corr_id: vec![4],
        payload: vec![5, 6],
    })));
}

#[test]
fn worker_up_exiting_round_trip() {
    round_trip(&up(worker_up::Msg::Exiting(Exiting {
        outcome: Outcome::Crashed as i32,
        reason: "r".into(),
        detail: "d".into(),
    })));
}

#[test]
fn worker_up_stopped_round_trip() {
    round_trip(&up(worker_up::Msg::Stopped(Stopped {
        on_stop_completed: true,
    })));
}

#[test]
fn worker_up_applied_round_trip() {
    round_trip(&up(worker_up::Msg::Applied(Applied {
        digest: "d1".into(),
    })));
}

#[test]
fn worker_down_init_round_trip() {
    round_trip(&down(worker_down::Msg::Init(Init {
        contract: "c1".into(),
        payload: vec![1],
        deadline_ms: 9,
        live_digest: "d1".into(),
    })));
}

#[test]
fn worker_down_start_round_trip() {
    round_trip(&down(worker_down::Msg::Start(Start {
        contract: "c1".into(),
        payload: vec![2],
        deadline_ms: 8,
    })));
}

#[test]
fn worker_down_stop_round_trip() {
    round_trip(&down(worker_down::Msg::Stop(Stop {
        reason: "stop".into(),
        grace_ms: 10,
    })));
}

#[test]
fn worker_down_relay_round_trip() {
    round_trip(&down(worker_down::Msg::Relay(Relay {
        contract: "c2".into(),
        corr_id: vec![7],
        payload: vec![8],
    })));
}

#[test]
fn worker_down_undeliverable_round_trip() {
    round_trip(&down(worker_down::Msg::Undeliverable(Undeliverable {
        corr_id: vec![9],
    })));
}

#[test]
fn worker_down_controller_round_trip() {
    round_trip(&down(worker_down::Msg::Controller(Controller {
        attached: true,
        epoch: 11,
    })));
}

#[test]
fn worker_down_live_round_trip() {
    round_trip(&down(worker_down::Msg::Live(Live {
        contract: "c1".into(),
        payload: vec![1, 2],
        digest: "d2".into(),
    })));
}

#[test]
fn worker_up_rejects_a_truncated_varint() {
    WorkerUp::decode([0x80].as_slice()).expect_err("truncated varint");
}

fn sample_bindings() -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq: 2 }),
        feeds: vec![FeedBinding {
            feed: "f1".into(),
            md_instance: "md".into(),
            subject: "sub".into(),
            state: BindingState::Ready as i32,
        }],
        selectors: vec![SelectorBinding {
            name: "sel".into(),
            md_instance: "md".into(),
            universe: Some(Universe {
                epoch: 3,
                members: vec!["m1".into(), "m2".into()],
                current: "m1".into(),
                topics: vec!["t".into()],
            }),
            subject_prefix: "p".into(),
        }],
        accounts: vec![AccountBinding {
            name: "a".into(),
            api_id: 4,
            state: BindingState::Pending as i32,
            worker: Some(WorkerRef {
                key: "w".into(),
                incarnation: 5,
            }),
        }],
    }
}

#[test]
fn stsctl_init_payload_round_trip() {
    round_trip(&InitPayload {
        session_id: "s1".into(),
        incarnation: 2,
        sts_instance: "sts".into(),
        name: "n".into(),
        strategy: Some(StrategyRef {
            name: "strat".into(),
            code: "c".into(),
            repository: "repo".into(),
        }),
        params: vec![1, 2],
        feeds: vec![FeedDecl {
            feed: "f1".into(),
            md_instance: "md".into(),
        }],
        accounts: vec![AccountDecl {
            name: "a".into(),
            api_id: 3,
            td_instance: "td".into(),
            algo_ids: vec![1, 4],
        }],
        nats: Some(NatsCredentials {
            user_jwt: "jwt".into(),
            seed: "seed".into(),
            inbox_prefix: "in".into(),
        }),
        recv_window_ms: 5,
        hook_timeout_s: 6,
    });
}

#[test]
fn stsctl_start_payload_round_trip() {
    round_trip(&StartPayload {
        bindings: Some(sample_bindings()),
    });
}

#[test]
fn stsctl_command_bindings_round_trip() {
    round_trip(&Command {
        cmd: Some(command::Cmd::Bindings(sample_bindings())),
    });
}

#[test]
fn stsctl_command_fail_round_trip() {
    round_trip(&Command {
        cmd: Some(command::Cmd::Fail(Fail { reason: "x".into() })),
    });
}

#[test]
fn stsctl_report_round_trip() {
    round_trip(&Report {
        bindings_applied: Some(Version { epoch: 1, seq: 2 }),
        hook: "on".into(),
        hook_blocked_ms: 3,
        hook_slow: 4,
        queues: vec![
            QueueStat {
                name: "md".into(),
                depth: 5,
                dropped: 6,
                max_age_ms: 7,
            },
            QueueStat {
                name: "td".into(),
                depth: 1,
                dropped: 2,
                max_age_ms: 3,
            },
        ],
    });
}

#[test]
fn stsctl_init_payload_rejects_a_truncated_varint() {
    InitPayload::decode([0x80].as_slice()).expect_err("truncated varint");
}
