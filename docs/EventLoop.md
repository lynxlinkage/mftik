# uvloop — can this node change event loops, and would it matter?

Short answer: it can, cheaply and with no new dependency, and one of the six
processes has been running on uvloop in production for as long as the API has
existed. What it would buy is roughly a tenth of the CPU each plane burns —
not the latency this question is usually asked in hope of.

Nothing here is built yet. The measurements are, and `just loop-bench`
reproduces every number below.

## What is already true

Facts this assessment rests on, all of them checkable in the tree today.

**The API already runs uvloop.** `run()` calls `uvicorn.run(...)` with no
`loop` argument (`apps/api/src/mftik_api/main.py:156`), which leaves Uvicorn on
its default `loop="auto"` — and `auto` means uvloop when uvloop imports. Asked
from inside a request handler started that way, the answer is `uvloop.Loop`.
So the node is *already* mixed: the control plane runs one loop and the five
domains run another, and nobody chose either.

**uvloop is already installed in every container.** It arrives as an extra of
`uvicorn[standard]` (`apps/api/pyproject.toml:10`) and is pinned in the
lockfile at 0.22.1 (`uv.lock:1263`), with prebuilt manylinux and musllinux
wheels for x86-64 and aarch64 on 3.12, 3.13 and 3.14 — nothing compiles. The
Dockerfile installs the whole workspace in one shot with
`uv sync --all-packages` (`Dockerfile:32`) and every service shares that image,
so `import uvloop` already succeeds inside TD, MD, STS, SYM and Paper. Adopting
it there adds no dependency, no image weight and no build step.

**The five domains each own exactly one loop, created in one line.** `main()`
calls `asyncio.run(amain())` and nothing else touches loop construction:
`apps/td/src/mftik_td/app.py:184`, `apps/md/src/mftik_md/app.py:232`,
`apps/sts/src/mftik_sts/app.py:229`, `apps/sym/src/mftik_sym/app.py:144`,
`apps/paper/src/mftik_paper/app.py:317`. There is no `new_event_loop`, no
`set_event_loop`, no `get_event_loop`, no `set_event_loop_policy` and no
`run_forever` anywhere in the repository. The change is five lines, in five
files, each replacing `asyncio.run` with `uvloop.run`.

**Sessions are tasks, not processes.** STS, TD and MD all multiplex sessions
onto one loop with `asyncio.create_task`. That means the loop is genuinely
shared infrastructure — one slow callback is felt by every session in the
process — so loop efficiency is a real lever on how many sessions fit in a
plane. It also means there is exactly one loop per container to convert.

**Nothing in the tree uses the parts of asyncio uvloop does not implement.**
No `add_reader` or `add_writer`, no child watchers, no `subprocess_exec` or
`create_subprocess_*`, no Unix-socket servers, no `SelectorEventLoop`
references, and no `EventLoopPolicy` subclass. The four loop APIs that *are*
used — `loop.add_signal_handler` for SIGINT/SIGTERM in all five domains,
`asyncio.to_thread` for eventlog disk writes
(`packages/common/src/mftik/strategy/eventlog.py:293`),
`loop.run_in_executor` against a dedicated pool for alert matching
(`apps/api/src/mftik_api/alert_eval.py:20`), and `loop.time()` — are all
supported. Two of them behave differently, which is the next section.

## The two behaviours that actually differ

`just loop-bench --probe` runs both loops through the behaviours this tree
depends on. Everything is identical except the `loop.time()` rows and the
sub-millisecond sleep, and neither of those turns out to reach a call site here.

| Probe | asyncio | uvloop |
|---|---|---|
| `loop.time()` steps per 2000 yields | 2000 | 2 |
| `loop.time()` smallest step | 1.4 µs | 1000 µs |
| `await asyncio.sleep(0.0001)` | 1121 µs | 1.7 µs |
| `await asyncio.sleep(0.01)` | 10135 µs | 10127 µs |
| `add_signal_handler` | delivered | delivered |
| `to_thread` / custom executor | ok | ok |
| refused connect raises | `ConnectionRefusedError` | `ConnectionRefusedError` |
| named tasks, cancellation, exception handler | same | same |

**`loop.time()` is a millisecond clock under uvloop.** libuv keeps its own
cached clock and reports whole milliseconds, so a loop that yields two thousand
times sees the clock move twice. Every deadline in this tree built on that clock
is seconds: `LEASE_GRACE_S` is 3.0 in MD and 5.0 in TD,
`PENDING_NEW_TIMEOUT_S` 5.0, `UNKNOWN_FORCE_RECON_S` 10.0,
`UNKNOWN_FORCE_RECON_INTERVAL_S` 60.0, `CANCEL_RETRY_S` 1.0. A millisecond is
0.1% of the tightest of them, so nothing in the tree can tell the difference
today.

It is still worth writing down, because it is a ceiling rather than a bug. The
venue sockets stamp `stats.last_frame_at` off this clock, and any future work
that wants to say how long a print took to reach a strategy cannot get that from
`loop.time()` on uvloop. `time.perf_counter()` is the clock for that, and it is
unaffected: every duration in the probe and the benchmark is measured with it,
and the two loops agree on the sleeps they both honour to within 8 µs.

**A sub-millisecond sleep stops being a sleep.** asyncio floors
`sleep(0.0001)` at about a millisecond; uvloop returns from it in under two
microseconds. A poll loop written as `await asyncio.sleep(0.0001)` would go from
a thousand laps a second to a spin. No production or test code does this: the
shortest sleep anywhere in the tree is `0.01`, which both loops honour to within
8 µs. It is a rule for later, not a migration blocker.

## The suite says the same thing on either loop

pytest-asyncio builds every test's loop from its `event_loop_policy` fixture,
so running the suite both ways is one fixture override, and `asyncio_mode` is
already `auto` (`pyproject.toml:33`). All 2,926 tests — every venue adapter
against its stub server, the OMS, the broker, the TD/MD/STS session machinery —
were run on each loop: 2,921 passed on asyncio and 2,922 on uvloop, in 209
seconds either way, the suite being bound by fakeredis and sqlite rather than
by the loop.

The five that failed deserve the honest version rather than a rounded one. They
were the same five on both loops except that one passed under uvloop, and all of
them are about the on-disk strategy registry and an environment overlay rather
than about async anything. Run in isolation the set changes again — three fail
on asyncio, one on uvloop — which is what order-dependence looks like, and they
were reproduced on asyncio before uvloop was introduced. No test failed under
uvloop that passed under asyncio.

That is what makes this cheap to try rather than a leap: the switch is testable
on both loops from one branch, and the suite is a real check on it.

## What it would buy

Measured with `just loop-bench` against a real Redis, driving the actual
`Broker` and the actual pydantic envelopes. Medians of five reps.

| Case | What it is | asyncio | uvloop | uvloop gain |
|---|---|---|---|---|
| `tcp_echo` | bare stream round trip — **the control** | 69,196/s | 104,570/s | **1.51x** |
| `ws_ingest` | a venue read loop: recv + `json.loads` | 89,685/s | 97,274/s | 1.08x |
| `subscribe` | STS feed pump: `get_message` + envelope parse | 9,943/s | 10,103/s | 1.02x |
| `rpc` | `Broker.request` p50 latency (lower is better) | 332 µs | 309 µs | 1.07x |
| `fanout` | `Dispatcher.publish` as written today | 1,747/s | 1,731/s | 0.99x |

Read the control first. On a raw transport with nothing above it, uvloop is
half again as fast, so the harness can plainly see what uvloop is good at. The
rest of the table is that advantage being diluted, and the order is not a
coincidence — it is the amount of Python between the socket and the payload.
Add `websockets` framing and a `json.loads` and 1.51x becomes 1.08x. Add
redis-py's protocol handling and pydantic validation and it becomes 1.02x. On
MD's fan-out it disappears into the noise, because that path is not spending its
time in the loop at all: it is spending it on `sessions + 1` serialised Redis
round trips per print (`apps/md/src/mftik_md/session/dispatcher.py:103`), and no
loop makes a round trip that still has to happen any cheaper.

**CPU is the exception, and it is consistent.** uvloop costs 10–12% less CPU
for identical work on every Redis case — 63.4 → 56.5 µs per fan-out round trip,
100.4 → 91.8 µs per message received, 0.50 → 0.45 s for 1,500 RPCs. That is
real: sessions are tasks sharing one process, so a tenth of the CPU back is a
tenth more headroom before a plane has to be split. It is just a different claim
from the one a loop swap is usually proposed to make.

## The lever that is bigger than the loop

The same harness runs the fan-out a second way, with the N publishes and the
tape append batched into one round trip instead of `N+1` awaited ones:

| Fan-out variant | asyncio | uvloop |
|---|---|---|
| as written today | 1,747/s | 1,731/s |
| pipelined | 5,663/s | 5,794/s |

**3.24x, on the same loop.** If the reason to want uvloop is MD throughput,
uvloop is the wrong change — it is worth 0.99x on that path and pipelining is
worth 3.24x. The two do not compete for the same work and could both be done,
but they should not be confused for each other, and only one of them is on the
critical path of a busy feed.

The same shape shows up elsewhere and is worth naming while it is in view:
`Broker.subscribe` sleeps 10 ms between empty polls and `Broker.request` polls
BLPOP in one-second laps; `envapply` runs a blocking `subprocess.run` on the
loop that serves it. None of those are loop problems, and a faster loop hides
none of them.

## Recommendation

Adopt it, for CPU headroom and for consistency, and do not sell it as latency.

The consistency argument is the stronger one. Right now the API runs uvloop
because a transitive extra happened to be installed, and the five domains run
the stdlib loop because nobody said otherwise. That is one node running two
loops by accident, which means a loop-sensitive bug reproduces differently in
the API than in STS and nothing in the repository would explain why. Whichever
loop is chosen, it should be chosen out loud.

What that takes:

1. **Declare it.** Add `uvloop` to the `[project.dependencies]` of the five
   apps, not to `packages/common`. `mftik` is the distribution a strategy
   author installs with `pip install mftik`, and uvloop publishes no Windows
   wheel — the lockfile already carries `sys_platform != 'win32'` on it for
   that reason. The SDK's own entrypoints (the `mftik` CLI) are synchronous, so
   it needs no loop on a laptop.
2. **Swap five lines.** `asyncio.run(amain())` → `uvloop.run(amain())` in the
   five `main()` functions listed above. `uvloop.run` is the drop-in: it
   installs the loop for that call and nothing else, which keeps loop choice
   visible at the entrypoint instead of hidden in a policy set at import time.
3. **Pin the API's loop explicitly.** Pass `loop="uvloop"` to `uvicorn.run`, so
   the API is on uvloop because someone decided it rather than because an extra
   is installed. This changes no behaviour today and stops a dependency bump
   from silently changing it tomorrow.
4. **Run the suite both ways in CI.** One `event_loop_policy` fixture override
   behind a flag. It is what makes the third-party surface — a strategy author
   can install packages into a session's environment overlay — checkable rather
   than assumed.

What to watch afterwards, in this order: per-plane CPU at a fixed message rate,
which is where the 10% should show up and the only place the case rests;
reconnect behaviour on the venue sockets, which is the largest body of
OS-error handling in the tree and the least exercised by tests; and eventlog
write latency, since `to_thread` is the one place the loop hands work to a
thread on a path a strategy can feel.

If none of that is worth the churn, the defensible alternative is not "leave
it" — it is to pin the API to the stdlib loop with `loop="asyncio"` for the
same one-node-one-loop reason, and revisit uvloop when a profile actually shows
a plane CPU-bound in its loop.
