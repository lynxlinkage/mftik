# Runtime notes

These are the constraints from PROTOCOL §9.9 that a strategy has to follow. The SDK already runs the control connection, the heartbeat, and the network reads on threads of its own. The strategy's job is not to fight that arrangement.

## Processes

The runtime has background threads before the first hook. Starting a child with `fork` after those threads exist can copy a lock another thread is holding. Use `multiprocessing` with the `spawn` start method or the `forkserver` start method. Set the start method before creating a process.

A child has to exit with the worker. The platform signals the worker's process group when the session stops. A child that leaves the group and keeps running violates the session contract. Do not daemonize a helper so that it outlives the strategy.

## SIGTERM

The runtime installs its SIGTERM handler before it sends Hello, on a thread of its own. SIGTERM is a stop, with no grace deadline of its own, the same role as the Go SDK's signal notification. Do not install a SIGTERM handler. `signal.signal` replaces the handler the runtime is waiting on, and the session then does not see the platform's stop.

SIGINT is not the platform's stop signal. A `KeyboardInterrupt` inside a hook is an ordinary hook exception. See [Errors and exit codes](errors.md).

## Memory

Go workers receive `GOMEMLIMIT` from the platform. This process does not. `Limits.memory_mb` on the session spec is not read by `mftik`, and the SDK does not watch the process size. Stay inside the host's memory limit, or watch the resident set in the strategy and stop itself with `fail` when it should. There is no second mechanism inside the SDK.

`Limits.volume_mb` is different. The platform measures the volume directory and stops the session when the directory grows past that limit. See [Volume and inputs](volume.md). `hook_timeout_s` is the hard hook budget in [Threads](threads.md).

## What else the process must leave alone

The platform injects `MFTIKO_*` variables: the worker identity, the control file descriptor, the NATS address, and the volume and input directories. Do not overwrite them. Credentials for the session are not in the environment. They arrive in the init message, and strategy code does not handle that message.

The entrypoint sets stdout and stderr to line buffering before the strategy is constructed. Print a line and it is flushed on the newline, including on the way out of the process. The runtime also flushes both streams before it exits, except when a hook is still on the stack or the outcome is `HUNG`. In those cases the exiting thread must not wait for the GIL.

Do not put a secret in a log line, in `params` that you then print, or in an exception message. Those strings are stored with the session.
