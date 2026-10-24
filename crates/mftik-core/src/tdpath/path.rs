//! OMS, ledger, and recon watermark.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::time::Duration;

use mftik_proto::wire::mftiko::{common::v1 as common, td::v1 as td};

use super::cloid::algo_of;

/// Held `td.evt` messages across every account. One more fails the session
/// (PROTOCOL §9.6, E08-D041). The dispatcher FIFO uses the same cap.
pub const HOLD_CAP: usize = 65536;

/// `Exiting` reason when the hold cannot grow. Same words as the TD FIFO.
pub const REASON_HOLD_FULL: &str = "td buffer full";

/// Local reject reason when `td.order` has no responder (PROTOCOL §9.4).
pub const REASON_OFFLINE: &str = "account offline";

/// First recon retry. It doubles until [`RECON_BACKOFF_MAX`] (E08-D042).
pub const RECON_BACKOFF_MIN: Duration = Duration::from_secs(1);

/// Cap on the recon retry. 16s doubled is 32s, which lands on this cap.
pub const RECON_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// What [`Path::open`] needs. There is no NATS connection and no clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub session_id: String,
    /// `InitPayload.recv_window_ms`. One attempt of recon, submit, cancel, and
    /// batch cancel waits this long. Zero is rejected.
    pub recv_window: Duration,
    pub accounts: Vec<AccountDecl>,
}

/// One account this session may trade. `algo_ids` are the algos declared for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountDecl {
    pub api_id: i64,
    pub algo_ids: Vec<u32>,
}

/// Why [`Path::open`] refused the config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenError {
    EmptySessionId,
    RecvWindow,
    ApiIdNotPositive { api_id: i64 },
    DuplicateApiId { api_id: i64 },
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySessionId => f.write_str("tdpath: empty session id"),
            Self::RecvWindow => f.write_str("tdpath: recv window must be positive"),
            Self::ApiIdNotPositive { api_id } => {
                write!(f, "tdpath: api_id {api_id} is not positive")
            }
            Self::DuplicateApiId { api_id } => write!(f, "tdpath: duplicate api_id {api_id}"),
        }
    }
}

impl std::error::Error for OpenError {}

/// A mistake in the call. TD's answers arrive later as events, not as errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    UnknownAccount,
    AlgoNotDeclared,
    NotListening,
    NoCancelBatch,
    Stopping,
    ListenInProgress,
    ListenNotInProgress,
    ClientOrderId(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownAccount => f.write_str("tdpath: the account is not bound to this session"),
            Self::AlgoNotDeclared => {
                f.write_str("tdpath: the algo_id is not declared for this account")
            }
            Self::NotListening => f.write_str("tdpath: td.evt is not subscribed yet"),
            Self::NoCancelBatch => {
                f.write_str("tdpath: the account's cancel_batch is not known yet")
            }
            Self::Stopping => f.write_str("tdpath: the session is stopping"),
            Self::ListenInProgress => f.write_str("tdpath: listen is already in progress"),
            Self::ListenNotInProgress => f.write_str("tdpath: listen is not in progress"),
            Self::ClientOrderId(message) => write!(f, "tdpath: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// Mints the next `client_order_id`. One counter per process lives in
/// `cloidgen` (E02-05). [`Path`] does not keep that counter.
///
/// `algo_ids` is that account's declared set, sorted. Return
/// [`ClientOrderIdError::AlgoNotDeclared`] without advancing the counter when
/// `algo_id` is not in it. [`Path::submit`] also checks, and does not call
/// `next` when the algo is missing.
pub trait ClientOrderIds {
    fn next(
        &mut self,
        algo_ids: &[u32],
        algo_id: u32,
        now_ms: i64,
    ) -> Result<String, ClientOrderIdError>;
}

/// [`ClientOrderIds::next`] could not mint an id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientOrderIdError {
    AlgoNotDeclared,
    Other(String),
}

impl fmt::Display for ClientOrderIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlgoNotDeclared => {
                f.write_str("tdpath: the algo_id is not declared for this account")
            }
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ClientOrderIdError {}

/// A new order, in the units of `td.Submit`. `None` leaves the optional number
/// unset. `Some(NaN)` is the same as `None` (`decimal.Has` in the Go SDK).
#[derive(Clone, Debug, PartialEq)]
pub struct NewOrder {
    pub api_id: i64,
    pub algo_id: u32,
    pub uid: u32,
    pub side: common::Side,
    pub order_type: common::OrderType,
    pub tif: common::TimeInForce,
    pub qty: Option<f64>,
    pub quote_qty: Option<f64>,
    pub price: Option<f64>,
    pub recv_window_ms: i64,
    pub reduce_only: bool,
    pub params: HashMap<String, String>,
}

/// `client_order_id` is already in the OMS when this is returned.
#[derive(Clone, Debug, PartialEq)]
pub struct Submitted {
    pub client_order_id: String,
    pub effects: Vec<Effect>,
}

/// Ticket on a [`Effect::SendOrder`]. Pass it back to [`Path::on_order_round`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RoundTicket(u64);

/// Where an order change came from. The numeric values match the Go SDK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    FromTd = 1,
    LocalReject = 2,
    Timeout = 3,
}

/// Stamp carried on every account event. Zero on a local reject or a timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Td {
    pub api_id: i64,
    pub incarnation: u32,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderEvent {
    pub td: Td,
    pub origin: Origin,
    pub order: td::Order,
    pub cause: i32,
    pub code: i32,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FillEvent {
    pub td: Td,
    pub fill: td::Fill,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PositionEvent {
    pub td: Td,
    pub positions: Vec<td::Position>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BalanceEvent {
    pub td: Td,
    pub balances: Vec<td::Balance>,
}

/// `open_orders` are this session's algos. Positions and balances are the
/// whole account (E06-D033).
#[derive(Clone, Debug, PartialEq)]
pub struct ReconEvent {
    pub td: Td,
    pub recon: td::Recon,
}

/// Which cancel request a [`RejectEvent`] answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectedRequest {
    Cancel = 1,
    BatchCancel = 2,
}

/// A cancel TD refused locally. The order is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectEvent {
    pub td: Td,
    pub request: RejectedRequest,
    pub client_order_ids: Vec<String>,
    pub code: i32,
    pub reason: String,
}

/// Strategy callback to run after the matching state is already updated.
#[derive(Clone, Debug, PartialEq)]
pub enum Hook {
    Order(OrderEvent),
    Fill(FillEvent),
    Position(PositionEvent),
    Balance(BalanceEvent),
    Recon(ReconEvent),
    Reject(RejectEvent),
}

/// Something E03 must do. A [`Hook`] is delivered before the next [`Path::end_hook`].
#[derive(Clone, PartialEq)]
pub enum Effect {
    /// Subscribe `td.evt.{api_id}` for each id, then flush. Events that arrive
    /// before [`Path::listen_bound`] are held.
    SubscribeEvt {
        api_ids: Vec<i64>,
    },
    UnsubscribeEvt {
        api_ids: Vec<i64>,
    },
    /// Publish `request` on `subject` and wait `wait` for the reply.
    /// Publish order across effects is the order they were returned.
    SendOrder {
        ticket: RoundTicket,
        api_id: i64,
        subject: String,
        request: td::OrderRequest,
        wait: Duration,
    },
    /// `td.view` recon. `generation` comes back to [`Path::on_recon_round`].
    SendRecon {
        api_id: i64,
        generation: u64,
        subject: String,
        request: td::ViewRequest,
        wait: Duration,
    },
    /// Recon backoff (E08-D042). Another ask does not cancel it.
    /// Call [`Path::on_backoff`] when it fires.
    ArmBackoff {
        api_id: i64,
        after: Duration,
    },
    Hook(Box<Hook>),
    /// Session `FAILED`. Outstanding waits and backoff ticks are dropped.
    Fail {
        reason: String,
    },
}

impl fmt::Debug for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubscribeEvt { api_ids } => f.debug_tuple("SubscribeEvt").field(api_ids).finish(),
            Self::UnsubscribeEvt { api_ids } => {
                f.debug_tuple("UnsubscribeEvt").field(api_ids).finish()
            }
            Self::SendOrder {
                ticket,
                api_id,
                subject,
                ..
            } => f
                .debug_struct("SendOrder")
                .field("ticket", ticket)
                .field("api_id", api_id)
                .field("subject", subject)
                .finish_non_exhaustive(),
            Self::SendRecon {
                api_id,
                generation,
                subject,
                ..
            } => f
                .debug_struct("SendRecon")
                .field("api_id", api_id)
                .field("generation", generation)
                .field("subject", subject)
                .finish_non_exhaustive(),
            Self::ArmBackoff { api_id, after } => f
                .debug_struct("ArmBackoff")
                .field("api_id", api_id)
                .field("after", after)
                .finish(),
            Self::Hook(hook) => f.debug_tuple("Hook").field(&hook_name(hook)).finish(),
            Self::Fail { reason } => f.debug_struct("Fail").field("reason", reason).finish(),
        }
    }
}

/// How a `td.order` round ended.
#[derive(Clone, PartialEq)]
pub enum RoundOutcome {
    Reply(td::OrderReply),
    /// The subject had no responder. Submit and cancel become a 101.
    NoResponder,
    /// The wait ended, or the body was not an `OrderReply`.
    NoAnswer,
}

impl fmt::Debug for RoundOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reply(_) => f.write_str("Reply"),
            Self::NoResponder => f.write_str("NoResponder"),
            Self::NoAnswer => f.write_str("NoAnswer"),
        }
    }
}

/// How a recon round ended. Timeout, no responder, and a reply that is not a
/// recon all use [`ReconOutcome::Failed`].
#[derive(Clone, PartialEq)]
pub enum ReconOutcome {
    Reply(td::ViewReply),
    Failed,
}

impl fmt::Debug for ReconOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reply(_) => f.write_str("Reply"),
            Self::Failed => f.write_str("Failed"),
        }
    }
}

/// [`Path::listen_bound`] after the flush.
#[derive(Clone, Debug, PartialEq)]
pub enum Bound {
    Listening,
    /// The session failed while the subscribe was in flight. Drop the subs.
    Stopping(Vec<Effect>),
}

pub fn order_subject(api_id: i64) -> String {
    format!("td.order.{api_id}")
}

pub fn view_subject(api_id: i64) -> String {
    format!("td.view.{api_id}")
}

pub fn evt_subject(api_id: i64) -> String {
    format!("td.evt.{api_id}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderKind {
    Submit,
    Cancel,
    Batch,
}

struct Inflight {
    api_id: i64,
    kind: OrderKind,
    id: String,
    ids: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Drop,
    Gap,
    Newer,
    Deliver,
}

enum Cont {
    Release(i64),
}

struct Account {
    algos: BTreeSet<u32>,
    orders: BTreeMap<String, td::Order>,
    submit_gen: HashMap<String, u64>,
    sent_gen: u64,
    positions: BTreeMap<u32, td::Position>,
    balances: BTreeMap<String, td::Balance>,
    hold: VecDeque<td::AccountEvent>,
    holding: bool,
    pending: bool,
    asking: bool,
    reply_open: bool,
    have: bool,
    have_batch: bool,
    inc: u32,
    seq: u64,
    cancel_batch: u32,
    backoff: Duration,
    waiting_backoff: bool,
    prev_inc: u32,
    prev_seq: u64,
    prev_have: bool,
    in_flight_gen: u64,
}

impl Account {
    fn new(algo_ids: Vec<u32>) -> Self {
        Self {
            algos: algo_ids.into_iter().collect(),
            orders: BTreeMap::new(),
            submit_gen: HashMap::new(),
            sent_gen: 0,
            positions: BTreeMap::new(),
            balances: BTreeMap::new(),
            hold: VecDeque::new(),
            holding: false,
            pending: false,
            asking: false,
            reply_open: false,
            have: false,
            have_batch: false,
            inc: 0,
            seq: 0,
            cancel_batch: 0,
            backoff: RECON_BACKOFF_MIN,
            waiting_backoff: false,
            prev_inc: 0,
            prev_seq: 0,
            prev_have: false,
            in_flight_gen: 0,
        }
    }

    /// Snapshot rows replace the book. A local `PENDING_NEW` submitted at or
    /// after `request_gen`, and missing from this view reply, stays (E08-D054).
    /// A pushed recon is not that reply.
    fn merge(&mut self, filtered: &[td::Order], request_gen: u64, from_request: bool) {
        let mut next = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for order in filtered {
            if order.client_order_id.is_empty() {
                continue;
            }
            seen.insert(order.client_order_id.clone());
            if is_terminal(order.status) {
                continue;
            }
            next.insert(order.client_order_id.clone(), order.clone());
        }
        if from_request {
            for (id, order) in &self.orders {
                if seen.contains(id) {
                    continue;
                }
                let Some(generation) = self.submit_gen.get(id).copied() else {
                    continue;
                };
                if order.status != i32::from(td::OrderStatus::PendingNew)
                    || generation < request_gen
                {
                    continue;
                }
                next.insert(id.clone(), order.clone());
            }
        }
        self.submit_gen.retain(|id, _| next.contains_key(id));
        self.orders = next;
    }

    fn replace_ledger(&mut self, positions: &[td::Position], balances: &[td::Balance]) {
        let mut next_positions = BTreeMap::new();
        for position in positions {
            next_positions.insert(position.uid, *position);
        }
        let mut next_balances = BTreeMap::new();
        for balance in balances {
            next_balances.insert(balance.asset.clone(), balance.clone());
        }
        self.positions = next_positions;
        self.balances = next_balances;
    }
}

/// One session's OMS and ledger.
pub struct Path {
    session_id: String,
    recv_window: Duration,
    api_ids: Vec<i64>,
    accounts: HashMap<i64, Account>,
    listening: bool,
    subscribing: bool,
    failed: bool,
    fail_sent: bool,
    next_ticket: u64,
    rounds: HashMap<RoundTicket, Inflight>,
    pending_drops: Vec<(i64, String)>,
    cont: VecDeque<Cont>,
}

impl Path {
    pub fn open(config: Config) -> Result<Self, OpenError> {
        if config.session_id.is_empty() {
            return Err(OpenError::EmptySessionId);
        }
        if config.recv_window.is_zero() {
            return Err(OpenError::RecvWindow);
        }
        let mut accounts = HashMap::with_capacity(config.accounts.len());
        let mut api_ids = Vec::with_capacity(config.accounts.len());
        for decl in config.accounts {
            if decl.api_id <= 0 {
                return Err(OpenError::ApiIdNotPositive {
                    api_id: decl.api_id,
                });
            }
            if accounts.contains_key(&decl.api_id) {
                return Err(OpenError::DuplicateApiId {
                    api_id: decl.api_id,
                });
            }
            api_ids.push(decl.api_id);
            accounts.insert(decl.api_id, Account::new(decl.algo_ids));
        }
        Ok(Self {
            session_id: config.session_id,
            recv_window: config.recv_window,
            api_ids,
            accounts,
            listening: false,
            subscribing: false,
            failed: false,
            fail_sent: false,
            next_ticket: 1,
            rounds: HashMap::new(),
            pending_drops: Vec::new(),
            cont: VecDeque::new(),
        })
    }

    /// Start listening. The returned subscribe must flush before
    /// [`Self::listen_bound`]. A second call after that is a no-op.
    #[must_use = "subscribe td.evt before listen_bound"]
    pub fn listen(&mut self) -> Result<Vec<Effect>, Error> {
        if self.failed {
            return Err(Error::Stopping);
        }
        if self.listening {
            return Ok(Vec::new());
        }
        if self.subscribing {
            return Err(Error::ListenInProgress);
        }
        self.subscribing = true;
        for account in self.accounts.values_mut() {
            account.holding = true;
        }
        Ok(vec![Effect::SubscribeEvt {
            api_ids: self.api_ids.clone(),
        }])
    }

    /// The subscribe flushed. Events received since [`Self::listen`] stay held.
    pub fn listen_bound(&mut self) -> Result<Bound, Error> {
        if !self.subscribing {
            return Err(Error::ListenNotInProgress);
        }
        self.subscribing = false;
        if self.failed {
            return Ok(Bound::Stopping(vec![Effect::UnsubscribeEvt {
                api_ids: self.api_ids.clone(),
            }]));
        }
        self.listening = true;
        Ok(Bound::Listening)
    }

    /// The subscribe failed. Held events from that attempt are dropped.
    pub fn listen_aborted(&mut self) {
        if !self.subscribing {
            return;
        }
        self.subscribing = false;
        for account in self.accounts.values_mut() {
            account.holding = false;
            account.hold.clear();
        }
    }

    /// Ask every account for a snapshot. Events stay held until that account's
    /// reply has been given to the recon hook.
    pub fn recon_all(&mut self) -> Result<Vec<Effect>, Error> {
        if self.failed {
            return Err(Error::Stopping);
        }
        if !self.listening {
            return Err(Error::NotListening);
        }
        let api_ids = self.api_ids.clone();
        let mut effects = Vec::new();
        for api_id in api_ids {
            self.arm_account(api_id);
            if let Some(effect) = self.pump(api_id) {
                effects.push(effect);
            }
        }
        Ok(effects)
    }

    /// Ask one account. An ask already in flight keeps holding and sends again
    /// when it finishes.
    pub fn request_recon(&mut self, api_id: i64) -> Result<Vec<Effect>, Error> {
        self.ready(api_id)?;
        self.arm_account(api_id);
        Ok(self.pump(api_id).into_iter().collect())
    }

    /// Record `PENDING_NEW`, then return the `td.order` send. `now_ms` is the
    /// caller's clock in unix milliseconds. The id is in [`Self::open_orders`]
    /// before this returns.
    pub fn submit(
        &mut self,
        order: NewOrder,
        now_ms: i64,
        ids: &mut dyn ClientOrderIds,
    ) -> Result<Submitted, Error> {
        self.ready(order.api_id)?;
        let algos = {
            let account = self
                .accounts
                .get(&order.api_id)
                .ok_or(Error::UnknownAccount)?;
            if !account.algos.contains(&order.algo_id) {
                return Err(Error::AlgoNotDeclared);
            }
            account.algos.iter().copied().collect::<Vec<_>>()
        };
        let id = ids
            .next(&algos, order.algo_id, now_ms)
            .map_err(|error| match error {
                ClientOrderIdError::AlgoNotDeclared => Error::AlgoNotDeclared,
                ClientOrderIdError::Other(message) => Error::ClientOrderId(message),
            })?;
        if self.failed {
            return Err(Error::Stopping);
        }
        let row = td::Order {
            uid: order.uid,
            client_order_id: id.clone(),
            side: i32::from(order.side),
            order_type: i32::from(order.order_type),
            status: i32::from(td::OrderStatus::PendingNew),
            qty: wire_qty(order.qty),
            quote_qty: finite_opt(order.quote_qty),
            price: finite_opt(order.price),
            ..td::Order::default()
        };
        let request = td::OrderRequest {
            req: Some(td::order_request::Req::Submit(td::Submit {
                session_id: self.session_id.clone(),
                uid: order.uid,
                side: i32::from(order.side),
                order_type: i32::from(order.order_type),
                qty: finite_opt(order.qty),
                quote_qty: finite_opt(order.quote_qty),
                price: finite_opt(order.price),
                tif: i32::from(order.tif),
                reduce_only: order.reduce_only,
                client_order_id: id.clone(),
                params: order.params,
                recv_window_ms: order.recv_window_ms,
            })),
        };
        {
            let account = self
                .accounts
                .get_mut(&order.api_id)
                .ok_or(Error::UnknownAccount)?;
            let generation = account.sent_gen;
            account.orders.insert(id.clone(), row);
            account.submit_gen.insert(id.clone(), generation);
        }
        let effect = self.start_round(
            order.api_id,
            OrderKind::Submit,
            id.clone(),
            Vec::new(),
            request,
        );
        Ok(Submitted {
            client_order_id: id,
            effects: vec![effect],
        })
    }

    /// Ask TD to cancel one of this session's orders. A local reject is a
    /// [`Hook::Reject`] and does not change the order. A timeout changes
    /// nothing and is not reported (E08-D045).
    pub fn cancel(&mut self, api_id: i64, client_order_id: &str) -> Result<Vec<Effect>, Error> {
        self.ready(api_id)?;
        self.own_algo(api_id, client_order_id)?;
        let uid = self
            .accounts
            .get(&api_id)
            .and_then(|account| account.orders.get(client_order_id))
            .map(|order| order.uid)
            .unwrap_or(0);
        let request = td::OrderRequest {
            req: Some(td::order_request::Req::Cancel(td::Cancel {
                session_id: self.session_id.clone(),
                uid,
                client_order_id: client_order_id.to_owned(),
                recv_window_ms: 0,
            })),
        };
        let id = client_order_id.to_owned();
        Ok(vec![self.start_round(
            api_id,
            OrderKind::Cancel,
            id.clone(),
            vec![id],
            request,
        )])
    }

    /// Cancel live orders of `algo_id` on the account, limited to `uids` when
    /// that slice is not empty. `cancel_batch == 1` sends one `Cancel` per
    /// order (E06-D031). Ids are sorted by `client_order_id`.
    pub fn batch_cancel(
        &mut self,
        api_id: i64,
        algo_id: u32,
        uids: &[u32],
    ) -> Result<Vec<Effect>, Error> {
        self.ready(api_id)?;
        let account = self.accounts.get(&api_id).ok_or(Error::UnknownAccount)?;
        if !account.algos.contains(&algo_id) {
            return Err(Error::AlgoNotDeclared);
        }
        if !account.have_batch || account.cancel_batch == 0 {
            return Err(Error::NoCancelBatch);
        }
        let batch = account.cancel_batch;
        let ids = live_ids(account, algo_id, uids);
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.send_batches(api_id, batch, &ids))
    }

    /// One decoded `td.evt`. Malformed bytes never reach here.
    #[must_use = "deliver hooks and sends, then end_hook"]
    pub fn on_account_event(&mut self, api_id: i64, event: td::AccountEvent) -> Vec<Effect> {
        self.flush_drops();
        if self.failed || !self.accounts.contains_key(&api_id) {
            return Vec::new();
        }
        let decision = {
            let account = &self.accounts[&api_id];
            if account.holding || !account.have {
                Live::Hold
            } else {
                let inc = source_inc(&event);
                if inc != account.inc || seq_past_next(account.seq, event.seq) {
                    Live::Resync
                } else if event.seq <= account.seq {
                    Live::Drop
                } else {
                    Live::Apply
                }
            }
        };
        match decision {
            Live::Hold => self.hold_event(api_id, event),
            Live::Drop => Vec::new(),
            Live::Resync => {
                if let Some(account) = self.accounts.get_mut(&api_id) {
                    account.holding = true;
                    account.pending = true;
                }
                let mut effects = self.hold_event(api_id, event);
                if self.failed {
                    return effects;
                }
                if let Some(effect) = self.pump(api_id) {
                    effects.push(effect);
                }
                effects
            }
            Live::Apply => {
                if let Some(account) = self.accounts.get_mut(&api_id) {
                    account.seq = event.seq;
                }
                self.apply(api_id, event).into_iter().collect()
            }
        }
    }

    /// The `td.order` round for `ticket` finished.
    #[must_use = "deliver a reject or timeout hook, then end_hook"]
    pub fn on_order_round(&mut self, ticket: RoundTicket, outcome: RoundOutcome) -> Vec<Effect> {
        let Some(round) = self.rounds.remove(&ticket) else {
            return Vec::new();
        };
        if self.failed {
            return Vec::new();
        }
        self.flush_drops();
        match outcome {
            RoundOutcome::Reply(reply) if reply.accepted => Vec::new(),
            RoundOutcome::Reply(reply) => self.refuse(round, reply.code, reply.reason),
            RoundOutcome::NoResponder => self.refuse(
                round,
                i32::from(td::RejectCode::AccountOffline),
                REASON_OFFLINE.to_owned(),
            ),
            RoundOutcome::NoAnswer => {
                if round.kind != OrderKind::Submit {
                    return Vec::new();
                }
                self.mark_unknown(round)
            }
        }
    }

    /// The recon round for `generation` finished. On success the OMS and ledger
    /// already include the snapshot when the [`Hook::Recon`] is returned.
    /// [`Self::end_hook`] releases held events after that hook.
    #[must_use = "deliver the recon hook, then end_hook"]
    pub fn on_recon_round(
        &mut self,
        api_id: i64,
        generation: u64,
        outcome: ReconOutcome,
    ) -> Vec<Effect> {
        if self.failed || !self.take_ask(api_id, generation) {
            return Vec::new();
        }
        self.flush_drops();
        match outcome {
            ReconOutcome::Failed => self.recon_failed(api_id),
            ReconOutcome::Reply(reply) => self.accept_reply(api_id, generation, reply),
        }
    }

    /// The backoff armed for `api_id` fired.
    #[must_use = "a fired backoff may send another recon"]
    pub fn on_backoff(&mut self, api_id: i64) -> Vec<Effect> {
        if self.failed {
            return Vec::new();
        }
        let waiting = self
            .accounts
            .get(&api_id)
            .is_some_and(|account| account.waiting_backoff);
        if !waiting {
            return Vec::new();
        }
        if let Some(account) = self.accounts.get_mut(&api_id) {
            account.waiting_backoff = false;
        }
        self.pump(api_id).into_iter().collect()
    }

    /// The strategy hook returned. Drops a terminal order from that hook, then
    /// continues recon release. Call this only after the hook has run.
    #[must_use = "release may return the next hook, send, or backoff"]
    pub fn end_hook(&mut self) -> Option<Effect> {
        self.flush_drops();
        self.drive()
    }

    /// Stop the path. In-flight replies are ignored after this.
    #[must_use = "unsubscribe td.evt when the effect says so"]
    pub fn close(&mut self) -> Vec<Effect> {
        self.failed = true;
        self.rounds.clear();
        self.cont.clear();
        let unsubscribe = self.listening || self.subscribing;
        self.listening = false;
        self.subscribing = false;
        if unsubscribe {
            vec![Effect::UnsubscribeEvt {
                api_ids: self.api_ids.clone(),
            }]
        } else {
            Vec::new()
        }
    }

    pub fn order(&self, api_id: i64, client_order_id: &str) -> Option<td::Order> {
        self.accounts
            .get(&api_id)?
            .orders
            .get(client_order_id)
            .cloned()
    }

    /// Live orders of `algo_id`. Zero lists every algo declared for the account.
    /// Terminal orders are omitted. The vec is sorted by `client_order_id`.
    pub fn open_orders(&self, api_id: i64, algo_id: u32) -> Vec<td::Order> {
        let Some(account) = self.accounts.get(&api_id) else {
            return Vec::new();
        };
        account
            .orders
            .values()
            .filter(|order| !is_terminal(order.status))
            .filter(|order| {
                algo_of(&order.client_order_id).is_some_and(|algo| {
                    (algo_id == 0 || algo == algo_id) && account.algos.contains(&algo)
                })
            })
            .cloned()
            .collect()
    }

    pub fn position(&self, api_id: i64, uid: u32) -> Option<td::Position> {
        self.accounts.get(&api_id)?.positions.get(&uid).copied()
    }

    pub fn balance(&self, api_id: i64, asset: &str) -> Option<td::Balance> {
        self.accounts.get(&api_id)?.balances.get(asset).cloned()
    }

    /// `td.evt` messages waiting on a recon watermark, across every account.
    #[cfg(test)]
    pub fn held(&self) -> usize {
        self.held_count()
    }

    fn ready(&self, api_id: i64) -> Result<(), Error> {
        if self.failed {
            return Err(Error::Stopping);
        }
        if !self.listening {
            return Err(Error::NotListening);
        }
        if !self.accounts.contains_key(&api_id) {
            return Err(Error::UnknownAccount);
        }
        Ok(())
    }

    fn own_algo(&self, api_id: i64, client_order_id: &str) -> Result<u32, Error> {
        let account = self.accounts.get(&api_id).ok_or(Error::UnknownAccount)?;
        let algo = algo_of(client_order_id).ok_or(Error::AlgoNotDeclared)?;
        if !account.algos.contains(&algo) {
            return Err(Error::AlgoNotDeclared);
        }
        Ok(algo)
    }

    fn arm_account(&mut self, api_id: i64) {
        if let Some(account) = self.accounts.get_mut(&api_id) {
            account.holding = true;
            account.pending = true;
        }
    }

    fn pump(&mut self, api_id: i64) -> Option<Effect> {
        if self.failed || !self.listening {
            return None;
        }
        let wait = self.recv_window;
        let generation = {
            let account = self.accounts.get_mut(&api_id)?;
            if !account.pending || account.asking || account.waiting_backoff {
                return None;
            }
            account.pending = false;
            account.asking = true;
            account.reply_open = false;
            account.sent_gen = account.sent_gen.wrapping_add(1);
            account.in_flight_gen = account.sent_gen;
            account.prev_inc = account.inc;
            account.prev_seq = account.seq;
            account.prev_have = account.have;
            account.in_flight_gen
        };
        Some(Effect::SendRecon {
            api_id,
            generation,
            subject: view_subject(api_id),
            request: recon_request(),
            wait,
        })
    }

    fn take_ask(&mut self, api_id: i64, generation: u64) -> bool {
        let Some(account) = self.accounts.get_mut(&api_id) else {
            return false;
        };
        if !account.asking || account.reply_open || account.in_flight_gen != generation {
            return false;
        }
        account.reply_open = true;
        true
    }

    fn accept_reply(&mut self, api_id: i64, generation: u64, reply: td::ViewReply) -> Vec<Effect> {
        let incarnation = reply.incarnation;
        let seq = reply.seq;
        let cancel_batch = reply.cancel_batch;
        let Some(td::view_reply::Reply::Recon(mut body)) = reply.reply else {
            return self.recon_failed(api_id);
        };
        body.reason = i32::from(td::ReconReason::Requested);
        let effect = self.deliver_recon(
            api_id,
            Td {
                api_id,
                incarnation,
                seq,
            },
            body,
            true,
            cancel_batch,
            generation,
        );
        self.cont.push_back(Cont::Release(api_id));
        vec![effect]
    }

    fn recon_failed(&mut self, api_id: i64) -> Vec<Effect> {
        let Some(after) = self.arm_backoff(api_id) else {
            return Vec::new();
        };
        vec![Effect::ArmBackoff { api_id, after }]
    }

    fn arm_backoff(&mut self, api_id: i64) -> Option<Duration> {
        let account = self.accounts.get_mut(&api_id)?;
        account.asking = false;
        account.pending = true;
        account.holding = true;
        account.waiting_backoff = true;
        let after = account.backoff;
        account.backoff = grow_backoff(account.backoff);
        Some(after)
    }

    fn finish_ask(&mut self, api_id: i64) -> Option<Effect> {
        let decision = {
            let account = self.accounts.get_mut(&api_id)?;
            account.asking = false;
            account.reply_open = false;
            let same = account.prev_have
                && account.have
                && account.inc == account.prev_inc
                && account.seq == account.prev_seq;
            if account.pending && same {
                account.waiting_backoff = true;
                let after = account.backoff;
                account.backoff = grow_backoff(account.backoff);
                AskEnd::Backoff(after)
            } else {
                account.backoff = RECON_BACKOFF_MIN;
                AskEnd::Pump
            }
        };
        match decision {
            AskEnd::Backoff(after) => Some(Effect::ArmBackoff { api_id, after }),
            AskEnd::Pump => self.pump(api_id),
        }
    }

    fn hold_event(&mut self, api_id: i64, event: td::AccountEvent) -> Vec<Effect> {
        if self.held_count() >= HOLD_CAP {
            return self.fail_full();
        }
        if let Some(account) = self.accounts.get_mut(&api_id) {
            account.hold.push_back(event);
        }
        Vec::new()
    }

    fn fail_full(&mut self) -> Vec<Effect> {
        self.failed = true;
        self.rounds.clear();
        self.cont.clear();
        if self.fail_sent {
            return Vec::new();
        }
        self.fail_sent = true;
        vec![Effect::Fail {
            reason: REASON_HOLD_FULL.to_owned(),
        }]
    }

    fn held_count(&self) -> usize {
        self.accounts
            .values()
            .map(|account| account.hold.len())
            .sum()
    }

    fn drive(&mut self) -> Option<Effect> {
        while let Some(cont) = self.cont.pop_front() {
            match cont {
                Cont::Release(api_id) => {
                    if let Some(effect) = self.release_step(api_id) {
                        return Some(effect);
                    }
                }
            }
        }
        None
    }

    fn release_step(&mut self, api_id: i64) -> Option<Effect> {
        loop {
            if self.failed {
                if let Some(account) = self.accounts.get_mut(&api_id) {
                    account.asking = false;
                    account.reply_open = false;
                }
                return None;
            }
            let step = {
                let account = self.accounts.get_mut(&api_id)?;
                if account.hold.is_empty() {
                    if !account.pending {
                        account.holding = false;
                    }
                    ReleaseStep::Finish
                } else {
                    let event_inc = account.hold.front().map(source_inc).unwrap_or(0);
                    let event_seq = account.hold.front().map(|event| event.seq).unwrap_or(0);
                    match classify(account.inc, account.seq, event_inc, event_seq) {
                        Class::Drop => {
                            account.hold.pop_front();
                            ReleaseStep::Drop
                        }
                        Class::Gap | Class::Newer => {
                            account.pending = true;
                            account.holding = true;
                            ReleaseStep::Block
                        }
                        Class::Deliver => {
                            let event = account
                                .hold
                                .pop_front()
                                .unwrap_or_else(|| unreachable!("the hold was not empty"));
                            account.seq = event.seq;
                            ReleaseStep::Deliver(Box::new(event))
                        }
                    }
                }
            };
            match step {
                ReleaseStep::Drop => {}
                ReleaseStep::Finish | ReleaseStep::Block => return self.finish_ask(api_id),
                ReleaseStep::Deliver(event) => {
                    if let Some(effect) = self.apply(api_id, *event) {
                        self.cont.push_front(Cont::Release(api_id));
                        return Some(effect);
                    }
                }
            }
        }
    }

    fn apply(&mut self, api_id: i64, event: td::AccountEvent) -> Option<Effect> {
        let td_meta = event_td(api_id, &event);
        match event.event {
            Some(td::account_event::Event::OrderUpdate(update)) => {
                self.apply_update(api_id, td_meta, update)
            }
            Some(td::account_event::Event::Fill(fill)) => self.apply_fill(api_id, td_meta, fill),
            Some(td::account_event::Event::PositionUpdate(update)) => {
                Some(self.apply_position(api_id, td_meta, update))
            }
            Some(td::account_event::Event::BalanceUpdate(update)) => {
                Some(self.apply_balance(api_id, td_meta, update))
            }
            Some(td::account_event::Event::Recon(body)) => {
                Some(self.deliver_recon(api_id, td_meta, body, false, 0, 0))
            }
            None => None,
        }
    }

    fn deliver_recon(
        &mut self,
        api_id: i64,
        td_meta: Td,
        mut body: td::Recon,
        from_view: bool,
        cancel_batch: u32,
        request_gen: u64,
    ) -> Effect {
        let filtered = self.own_orders(api_id, &body.open_orders);
        if let Some(account) = self.accounts.get_mut(&api_id) {
            account.merge(&filtered, request_gen, from_view);
            account.replace_ledger(&body.positions, &body.balances);
            if from_view {
                account.have = true;
                account.inc = td_meta.incarnation;
                account.seq = td_meta.seq;
                if cancel_batch > 0 {
                    account.cancel_batch = cancel_batch;
                    account.have_batch = true;
                }
            }
        }
        body.open_orders = filtered;
        hook(Hook::Recon(ReconEvent {
            td: td_meta,
            recon: body,
        }))
    }

    fn own_orders(&self, api_id: i64, orders: &[td::Order]) -> Vec<td::Order> {
        let Some(account) = self.accounts.get(&api_id) else {
            return Vec::new();
        };
        orders
            .iter()
            .filter(|order| self.order_kept(account, &order.client_order_id))
            .cloned()
            .collect()
    }

    fn order_kept(&self, account: &Account, client_order_id: &str) -> bool {
        algo_of(client_order_id).is_some_and(|algo| account.algos.contains(&algo))
    }

    fn keeps(&self, api_id: i64, client_order_id: &str) -> bool {
        self.accounts
            .get(&api_id)
            .is_some_and(|account| self.order_kept(account, client_order_id))
    }

    fn apply_update(
        &mut self,
        api_id: i64,
        td_meta: Td,
        update: td::OrderUpdate,
    ) -> Option<Effect> {
        let order = update.order?;
        if !self.keeps(api_id, &order.client_order_id) {
            return None;
        }
        let id = order.client_order_id.clone();
        let terminal = is_terminal(order.status);
        if let Some(account) = self.accounts.get_mut(&api_id) {
            account.orders.insert(id.clone(), order.clone());
        }
        if terminal {
            self.pending_drops.push((api_id, id));
        }
        Some(hook(Hook::Order(OrderEvent {
            td: td_meta,
            origin: Origin::FromTd,
            order,
            cause: update.cause,
            code: update.code,
            reason: update.reason,
        })))
    }

    fn apply_fill(&mut self, api_id: i64, td_meta: Td, fill: td::Fill) -> Option<Effect> {
        if !self.keeps(api_id, &fill.client_order_id) {
            return None;
        }
        Some(hook(Hook::Fill(FillEvent { td: td_meta, fill })))
    }

    fn apply_position(&mut self, api_id: i64, td_meta: Td, update: td::PositionUpdate) -> Effect {
        if let Some(account) = self.accounts.get_mut(&api_id) {
            for position in &update.positions {
                account.positions.insert(position.uid, *position);
            }
        }
        hook(Hook::Position(PositionEvent {
            td: td_meta,
            positions: update.positions,
        }))
    }

    fn apply_balance(&mut self, api_id: i64, td_meta: Td, update: td::BalanceUpdate) -> Effect {
        if let Some(account) = self.accounts.get_mut(&api_id) {
            for balance in &update.balances {
                account
                    .balances
                    .insert(balance.asset.clone(), balance.clone());
            }
        }
        hook(Hook::Balance(BalanceEvent {
            td: td_meta,
            balances: update.balances,
        }))
    }

    fn send_batches(&mut self, api_id: i64, batch: u32, ids: &[String]) -> Vec<Effect> {
        if batch == 1 {
            return ids
                .iter()
                .map(|id| self.cancel_effect(api_id, id))
                .collect();
        }
        let width = usize::try_from(batch).unwrap_or(usize::MAX).max(1);
        ids.chunks(width)
            .map(|chunk| self.batch_effect(api_id, chunk))
            .collect()
    }

    fn cancel_effect(&mut self, api_id: i64, client_order_id: &str) -> Effect {
        let uid = self
            .accounts
            .get(&api_id)
            .and_then(|account| account.orders.get(client_order_id))
            .map(|order| order.uid)
            .unwrap_or(0);
        let request = td::OrderRequest {
            req: Some(td::order_request::Req::Cancel(td::Cancel {
                session_id: self.session_id.clone(),
                uid,
                client_order_id: client_order_id.to_owned(),
                recv_window_ms: 0,
            })),
        };
        let id = client_order_id.to_owned();
        self.start_round(api_id, OrderKind::Cancel, id.clone(), vec![id], request)
    }

    fn batch_effect(&mut self, api_id: i64, ids: &[String]) -> Effect {
        let orders = ids
            .iter()
            .map(|id| td::BatchCancelOrder {
                client_order_id: id.clone(),
            })
            .collect();
        let request = td::OrderRequest {
            req: Some(td::order_request::Req::BatchCancel(td::BatchCancel {
                orders,
            })),
        };
        self.start_round(
            api_id,
            OrderKind::Batch,
            String::new(),
            ids.to_vec(),
            request,
        )
    }

    fn start_round(
        &mut self,
        api_id: i64,
        kind: OrderKind,
        id: String,
        ids: Vec<String>,
        request: td::OrderRequest,
    ) -> Effect {
        let ticket = RoundTicket(self.next_ticket);
        self.next_ticket = self.next_ticket.wrapping_add(1);
        self.rounds.insert(
            ticket,
            Inflight {
                api_id,
                kind,
                id,
                ids,
            },
        );
        Effect::SendOrder {
            ticket,
            api_id,
            subject: order_subject(api_id),
            request,
            wait: self.recv_window,
        }
    }

    fn refuse(&mut self, round: Inflight, code: i32, reason: String) -> Vec<Effect> {
        if round.kind == OrderKind::Submit {
            return self.refuse_submit(round, code, reason);
        }
        let request = if round.kind == OrderKind::Batch {
            RejectedRequest::BatchCancel
        } else {
            RejectedRequest::Cancel
        };
        vec![hook(Hook::Reject(RejectEvent {
            td: Td {
                api_id: round.api_id,
                incarnation: 0,
                seq: 0,
            },
            request,
            client_order_ids: round.ids,
            code,
            reason,
        }))]
    }

    fn refuse_submit(&mut self, round: Inflight, code: i32, reason: String) -> Vec<Effect> {
        let shown = self.mark_pending(round.api_id, &round.id, td::OrderStatus::Rejected);
        let Some(order) = shown else {
            return Vec::new();
        };
        self.pending_drops.push((round.api_id, round.id));
        vec![hook(Hook::Order(OrderEvent {
            td: Td {
                api_id: round.api_id,
                incarnation: 0,
                seq: 0,
            },
            origin: Origin::LocalReject,
            order,
            cause: i32::from(td::OrderCause::Unspecified),
            code,
            reason,
        }))]
    }

    fn mark_unknown(&mut self, round: Inflight) -> Vec<Effect> {
        let Some(order) = self.mark_pending(round.api_id, &round.id, td::OrderStatus::Unknown)
        else {
            return Vec::new();
        };
        vec![hook(Hook::Order(OrderEvent {
            td: Td {
                api_id: round.api_id,
                incarnation: 0,
                seq: 0,
            },
            origin: Origin::Timeout,
            order,
            cause: i32::from(td::OrderCause::Unspecified),
            code: 0,
            reason: String::new(),
        }))]
    }

    /// Move a still-`PENDING_NEW` row to `status` and return the stored copy.
    fn mark_pending(
        &mut self,
        api_id: i64,
        id: &str,
        status: td::OrderStatus,
    ) -> Option<td::Order> {
        let account = self.accounts.get_mut(&api_id)?;
        let order = account.orders.get_mut(id)?;
        if order.status != i32::from(td::OrderStatus::PendingNew) {
            return None;
        }
        order.status = i32::from(status);
        Some(order.clone())
    }

    fn flush_drops(&mut self) {
        let drops = std::mem::take(&mut self.pending_drops);
        for (api_id, id) in drops {
            let Some(account) = self.accounts.get_mut(&api_id) else {
                continue;
            };
            let terminal = account
                .orders
                .get(&id)
                .is_some_and(|order| is_terminal(order.status));
            if terminal {
                account.orders.remove(&id);
                account.submit_gen.remove(&id);
            }
        }
    }
}

enum Live {
    Hold,
    Resync,
    Drop,
    Apply,
}

enum ReleaseStep {
    Drop,
    Finish,
    Block,
    Deliver(Box<td::AccountEvent>),
}

enum AskEnd {
    Backoff(Duration),
    Pump,
}

fn hook(hook: Hook) -> Effect {
    Effect::Hook(Box::new(hook))
}

fn hook_name(hook: &Hook) -> &'static str {
    match hook {
        Hook::Order(_) => "Order",
        Hook::Fill(_) => "Fill",
        Hook::Position(_) => "Position",
        Hook::Balance(_) => "Balance",
        Hook::Recon(_) => "Recon",
        Hook::Reject(_) => "Reject",
    }
}

fn recon_request() -> td::ViewRequest {
    td::ViewRequest {
        req: Some(td::view_request::Req::Recon(td::ReconRequest {})),
    }
}

fn event_td(api_id: i64, event: &td::AccountEvent) -> Td {
    Td {
        api_id,
        incarnation: source_inc(event),
        seq: event.seq,
    }
}

fn source_inc(event: &td::AccountEvent) -> u32 {
    event
        .source
        .as_ref()
        .map(|source| source.incarnation)
        .unwrap_or(0)
}

fn classify(inc: u32, seq: u64, event_inc: u32, event_seq: u64) -> Class {
    if event_inc < inc {
        Class::Drop
    } else if event_inc > inc {
        Class::Newer
    } else if event_seq <= seq {
        Class::Drop
    } else if next_seq(seq) == Some(event_seq) {
        Class::Deliver
    } else {
        Class::Gap
    }
}

fn seq_past_next(cursor: u64, seq: u64) -> bool {
    match next_seq(cursor) {
        Some(next) => seq > next,
        None => false,
    }
}

fn next_seq(cursor: u64) -> Option<u64> {
    cursor.checked_add(1)
}

fn is_terminal(status: i32) -> bool {
    matches!(
        td::OrderStatus::try_from(status),
        Ok(td::OrderStatus::Filled
            | td::OrderStatus::Cancelled
            | td::OrderStatus::Rejected
            | td::OrderStatus::Expired)
    )
}

fn live_ids(account: &Account, algo_id: u32, uids: &[u32]) -> Vec<String> {
    let filter: BTreeSet<u32> = uids.iter().copied().collect();
    let mut ids = Vec::new();
    for (id, order) in &account.orders {
        if is_terminal(order.status) {
            continue;
        }
        if algo_of(id) != Some(algo_id) {
            continue;
        }
        if !filter.is_empty() && !filter.contains(&order.uid) {
            continue;
        }
        ids.push(id.clone());
    }
    ids.sort();
    ids
}

fn finite_opt(value: Option<f64>) -> Option<f64> {
    value.filter(|number| !number.is_nan())
}

fn wire_qty(value: Option<f64>) -> f64 {
    finite_opt(value).unwrap_or(0.0)
}

fn grow_backoff(current: Duration) -> Duration {
    match current.checked_mul(2) {
        Some(doubled) if doubled <= RECON_BACKOFF_MAX => doubled,
        _ => RECON_BACKOFF_MAX,
    }
}
