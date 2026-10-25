//! The Rust standard conformance strategy (PROTOCOL §9.8, E08-D035, E03-D007).
//!
//! The same behavior as mftiko `examples/strategies/conform/main.go`, on
//! [`mftik_rt::run`]. `InitPayload.params` selects it. Buffer limits, the
//! prehook, the recon watermark, and `deadline_ms` aborts belong to the SDK;
//! this process only sleeps, submits, and reads its Context. For the checks
//! that must see events only after the subscriptions are up, OnStart calls
//! `resolve`, which the harness holds, and `priority` asks for a Recon first.
//! The SDK prints `gate` once the subscriptions are up. CI runs this through
//! `mftiko-conformance` on every pull request.

use std::io::Write;
use std::thread;
use std::time::Duration;

use mftik_proto::wire::mftiko::common::v1 as common;
use mftik_rt::{
    Context, Handlers, HookError, Order, OrderEvent, OrderHandler, PositionEvent, PositionHandler,
    ReconEvent, ReconHandler, Strategy, TapeQuery, Trade, TradeHandler, topic,
};
use serde::{Deserialize, Serialize};

/// The JSON object in `InitPayload.params`. The names are the cross-language
/// contract in PROTOCOL §9.8.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Params {
    hook_sleep_ms: u64,
    order_count: u32,
    init_sleep_ms: u64,
    start_sleep_ms: u64,
    on_stop_sleep_ms: u64,
    self_exit: bool,
    probe_reads: bool,
    scenario: String,
    #[allow(dead_code)] // Read by the SDK's harness, not the strategy (§9.8).
    until_seq: u64,
    api_id: i64,
    algo_id: u32,
    uid: u32,
}

/// The fixture instrument (§9.8). It is not a params field. Resolving it
/// lets the harness hold OnStart.
const CONFORM_TICKER: &str = "Binance_Spot_BTCUSDT";

/// One stdout line. Absent fields are left out, as Go's `omitempty`; the
/// booleans are always written when set, `false` included.
#[derive(Debug, Default, Serialize)]
struct Observation {
    #[serde(skip_serializing_if = "Option::is_none")]
    hook: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ctx: Option<bool>,
    #[serde(skip_serializing_if = "is_zero")]
    seq: u64,
    #[serde(skip_serializing_if = "is_zero32")]
    incarnation: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    view: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch: Option<bool>,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

fn is_zero32(n: &u32) -> bool {
    *n == 0
}

fn hook(name: &'static str) -> Observation {
    Observation {
        hook: Some(name),
        ..Default::default()
    }
}

fn write_obs(obs: &Observation) {
    let Ok(mut raw) = serde_json::to_vec(obs) else {
        return;
    };
    raw.push(b'\n');
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(&raw);
    let _ = out.flush();
}

fn sleep_ms(ms: u64) {
    if ms > 0 {
        thread::sleep(Duration::from_millis(ms));
    }
}

/// The scenarios whose events must wait in the SDK's buffers until OnStart
/// returns. The harness holds `resolve` for these, and only these.
fn needs_hold(scenario: &str) -> bool {
    matches!(
        scenario,
        "priority" | "prehook" | "ring" | "overflow" | "ctx"
    )
}

#[derive(Default)]
struct Conform {
    p: Params,
}

impl Strategy for Conform {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        if !ctx.params().is_empty() {
            self.p = serde_json::from_slice(ctx.params())?;
        }
        sleep_ms(self.p.init_sleep_ms);
        write_obs(&hook("OnInit"));
        Ok(())
    }

    fn on_start(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        let beat = self.p.scenario == "beat";
        if beat {
            write_obs(&hook("OnStart"));
            sleep_ms(self.p.hook_sleep_ms);
        } else {
            sleep_ms(self.p.start_sleep_ms);
        }
        if self.p.scenario == "priority" {
            ctx.request_recon(self.p.api_id)?;
        }
        if needs_hold(&self.p.scenario) {
            ctx.resolve(CONFORM_TICKER)?;
        }
        for _ in 0..self.p.order_count {
            ctx.submit(Order {
                api_id: self.p.api_id,
                algo_id: self.p.algo_id,
                uid: self.p.uid,
                side: common::Side::Buy,
                order_type: common::OrderType::Limit,
                tif: common::TimeInForce::Gtc,
                qty: Some(1.0),
                quote_qty: None,
                price: Some(1.0),
                recv_window_ms: 0,
                reduce_only: false,
                params: Default::default(),
            })?;
        }
        if self.p.probe_reads {
            let query = TapeQuery {
                limit: 1,
                ..Default::default()
            };
            let _ = ctx.tape(self.p.uid, topic::TRADE, query, |_, _, _| {
                write_obs(&Observation {
                    view: Some(true),
                    ..Default::default()
                });
            });
            let _ = ctx.fetch_klines(self.p.uid, "1m", 1, |_, _, _| {
                write_obs(&Observation {
                    fetch: Some(true),
                    ..Default::default()
                });
            });
        }
        if !beat {
            write_obs(&hook("OnStart"));
            sleep_ms(self.p.hook_sleep_ms);
        }
        if self.p.self_exit {
            ctx.exit();
        }
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &Context<Self>) {
        sleep_ms(self.p.on_stop_sleep_ms);
        write_obs(&hook("OnStop"));
    }

    fn handlers(&self, h: &mut Handlers<Self>) {
        h.trade().order().position().recon();
    }
}

impl TradeHandler for Conform {
    fn on_trade(&mut self, _ctx: &Context<Self>, ev: Trade) {
        write_obs(&Observation {
            seq: ev.md.seq,
            ..hook("OnTrade")
        });
    }
}

impl OrderHandler for Conform {
    fn on_order(&mut self, ctx: &Context<Self>, ev: OrderEvent) {
        let ok = ctx.order(ev.td.api_id, &ev.order.client_order_id).is_some();
        write_obs(&Observation {
            ctx: Some(ok),
            seq: ev.td.seq,
            incarnation: ev.td.incarnation,
            ..hook("OnOrder")
        });
        if self.p.scenario == "ctx" && !ok {
            ctx.fail("ctx");
        }
    }
}

impl PositionHandler for Conform {
    fn on_position(&mut self, ctx: &Context<Self>, ev: PositionEvent) {
        let uid = ev.positions.first().map_or(0, |p| p.uid);
        let ok = ctx.position(ev.td.api_id, uid).is_some();
        write_obs(&Observation {
            ctx: Some(ok),
            seq: ev.td.seq,
            incarnation: ev.td.incarnation,
            ..hook("OnPosition")
        });
        if self.p.scenario == "ctx" && !ok {
            ctx.fail("ctx");
        }
    }
}

impl ReconHandler for Conform {
    fn on_recon(&mut self, _ctx: &Context<Self>, _ev: ReconEvent) {
        write_obs(&hook("OnRecon"));
    }
}

fn main() {
    mftik_rt::run(Conform::default(), None)
}
