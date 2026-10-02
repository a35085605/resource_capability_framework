# Synchronous resource scopes: server and tracker

The public API has three layers:

| Layer | Responsibility |
| --- | --- |
| `Resource[T]` | A reusable, lazy description of acquisition and cleanup. `make`, `pure`, `map`, and `flat_map` do not acquire anything until a scope runs them. |
| `ResourceScope` | Own values' cleanup responsibilities, child scopes, rollback, and closure. |
| `CapabilityLifecycleCoordinator` | Manage requests, generations, capability publication, and operation results within a caller-supplied scope. |

Applications supply the root scope. Each coordinator activation gets a new child scope,
so releasing a coordinator leaves its owning scope available for another activation.

## Root scope, server, and tracker

The adapter functions and capability types below belong to the application. Each close
adapter owns any finite retry policy; the framework remains synchronous.

```python
from lifecycle.capability import CapabilityLifecycleCoordinator
from lifecycle.resource import Resource, ResourceRecoveryPool, ResourceScope


def server_resource(port: int) -> Resource[ServerCapability]:
    return Resource.make(
        acquire=lambda: open_server(port),
        release=lambda server: close_server_with_finite_retry(server),
    ).map(lambda server: ServerCapability(server))


def tracker_resource(
    server: ServerCapability,
    request: TrackerRequest,
) -> Resource[TrackerCapability]:
    return Resource.make(
        acquire=lambda: start_tracker(server, request),
        release=lambda tracker: stop_tracker_with_finite_retry(tracker),
    ).map(lambda tracker: TrackerCapability(tracker))


pool = ResourceRecoveryPool()
with ResourceScope(pool) as app_scope:
    server = CapabilityLifecycleCoordinator(server_resource, scope=app_scope)
    server_result = server.acquire(0, 5037)

    if server_result.snapshot.capability is not None:
        tracker = server.create_child(0, tracker_resource)
        tracker_result = tracker.acquire(0, request)

        # A tracker can stop without stopping the server.
        tracker.release(tracker_result.snapshot.generation, request)

        # Start another activation while this server remains alive.
        current = tracker.read()
        tracker.acquire(current.generation, next_request)

    # Exiting app_scope closes the remaining tracker before the server.

report = app_scope.close()  # The cached report; no repeated cleanup.
```

A child coordinator captures the exact parent capability and activation scope that were
active at `create_child()`. Reacquiring the server never retargets an old tracker.
Create a new tracker coordinator from the new ACTIVE server generation instead.
An acquire on an old tracker fails with `ScopeClosedError` in its diagnostics before
invoking its factory.

`Resource.map` changes the exposed value. The finalizer still owns and releases the
original server or tracker object.

## Direct scope use

```python
pool = ResourceRecoveryPool()
with ResourceScope(pool) as app_scope:
    config = app_scope.acquire(Resource.pure({"port": 5037}))
    server_scope = app_scope.child()  # Immediately registered; inherits the pool.
    server = server_scope.acquire(server_resource(config["port"]))

    tracker_scope = server_scope.child()  # May be created after server acquisition.
    tracker = tracker_scope.acquire(tracker_resource(server, request))
    tracker_scope.close()  # Independently stops this tracker.
    # app_scope still owns server_scope and its server.
```

A scope can acquire repeatedly. Every call uses a separate
[`contextlib.ExitStack`](https://docs.python.org/3/library/contextlib.html#contextlib.ExitStack)
batch. A failed call rolls back only its own successful acquisition steps. Earlier
committed batches stay owned and usable until the scope closes. Values requiring
independent stopping belong in separate child scopes.

The API deliberately removes `Resource.allocate()` and `ResourceHandle`, without
compatibility aliases. Replace `resource.allocate(pool)` with
`scope.acquire(resource)`, which returns the value directly. Replace handle release
with `scope.close()`, and construct coordinators with the required keyword
`scope=app_scope` instead of a recovery-pool argument.

## Shutdown and concurrency

A tree and all its coordinators share one internal `Condition(RLock())`. Admission,
registration, state commits, and notifications hold this lock. Factories, acquisition
callbacks, projections, and finalizers execute outside it.

As soon as any ancestor begins closing, descendants reject new acquisition and child
creation. `scope.acquire()` wraps this `ScopeClosedError` in
`ResourceAllocationError`; `child()` and context-manager entry raise it directly.
Acquisitions already running must finish their commit decision or rollback before
their scope can finish closing. A coordinator factory is part of that acquisition.

Each scope closes in this order:

1. Wait for its own in-flight acquisitions to finish rollback.
2. Close child scopes in reverse registration order.
3. Close its committed batches in reverse commit order, with LIFO cleanup within
   each batch.

A closed child detaches from its parent. Concurrent close calls share one cleanup
round. All successful and repeated `close()` calls return the same immutable
`ReleaseReport` object. `closed` becomes true only after cleanup or responsibility
handoff is complete. A scope cannot reopen, and its context manager cannot be reentered.

Cleanup failures from acquisitions still in flight when shutdown begins are included
in that shutdown's report, including when an ancestor started closing first. Reports
from child closures that finish during this round also flow into the ancestor report.
Earlier failed acquisitions and child scopes already closed before shutdown are not
retroactively included.

## Publication and generations

A coordinator publishes ACTIVE only after checking its activation and ancestors under
the same control lock used for shutdown. If publication wins, acquire succeeds; its
returned snapshot describes that moment. If shutdown wins, acquire cleans up and
returns `ACQUIRE_FAILED`, including any rollback or release report.

Reading an activation whose scope or ancestor is closing returns RELEASING with no
capability. Once the activation scope is closed, reading returns IDLE. Completion
writes match both generation and activation identity, so reading an externally closed
activation cannot advance generation twice, and a late operation cannot overwrite the
next activation.

| Operation | Generation rule |
| --- | --- |
| Successful acquire | Stays unchanged. |
| Started acquire that fails | Advances once after cleanup. |
| Completed release, including externally driven closure | Advances once. |
| Busy state, mismatched request, or stale generation | `NOT_EXECUTED`; no operation-driven advance. |

Generation starts at zero for each coordinator. Invalid `create_child()` requests
require a matching ACTIVE generation and raise `ValueError`. Factory errors, invalid
factory return values, and a capability value of `None` produce failed acquisition
diagnostics. A valid acquire against a closed owning scope also fails and advances its
generation; it does not run the factory.

Snapshots are observations, not a borrow guaranteeing continued capability use.
Another thread can close the activation after a snapshot is returned. Adapters remain
responsible for terminating blocking callbacks and implementing finite retries.
Callbacks must not synchronously wait for closure of a scope that is waiting for that
same callback to finish.

## Reports, interruptions, and recovery

Ordinary finalizer errors go into `ReleaseReport.errors` and transfer that finalizer's
responsibility once to `ResourceRecoveryPool`. Other finalizers keep running.
`pooled_count` counts these transfers. Pool snapshots expose immutable
`RecoveryEntry` values; the pool performs no retries or background scheduling.

A control-flow interruption, such as `KeyboardInterrupt` or another `BaseException`
outside `Exception`, also allows remaining cleanup to finish. Internally cleanup
returns both its report and its interruption. Scope closure, report caching, and
notifications complete before the initiating caller receives the interruption;
concurrent or later close callers obtain the cached report without replaying it.
An in-flight acquire propagates its own interruption to its acquiring caller, while
its rollback failures are still included in any shutdown waiting for it.

An original acquisition or body control-flow interruption takes precedence over a
cleanup interruption. A cleanup interruption takes precedence over an ordinary
acquisition or body exception and retains that exception as its cause. Ordinary
cleanup failures never replace the acquisition error or suppress a `with` body
exception. After leaving `with`, including after an exception, call `close()` to
retrieve its cached report.

A completed scope close means every owned cleanup responsibility was finalized or
transferred; it does not guarantee that all physical resources disappeared.

### Recovery dependency rule

A finalizer eligible for transfer must represent a detached recovery responsibility.
Its recovery action must remain meaningful after ancestor scopes have closed. If
cleanup needs an ancestor dependency, the transferred recovery token must retain
what it needs explicitly, or the adapter must resolve that dependency before handoff.
Grouping dependent acquisition steps in one scope determines cleanup order, but does
not by itself preserve ancestors for a later recovery action. The pool never keeps
ancestor scopes open.

### Multi-step acquisition rule

`Resource.make` can register a finalizer only after its acquire callback returns.
Compose independently owned acquisitions so each successful step registers immediately:

```python
server_socket.flat_map(
    lambda socket: server_process(socket).flat_map(
        lambda process: Resource.pure(ServerCapability(socket, process))
    )
)
```

If a callback creates internal state and raises before returning, that callback must
clean up the state that was never transferred to the framework.

## Verification

The migrated baseline and deterministic concurrency cases run with:

```text
python -B -m unittest discover -s tests -v
```

Concurrency tests coordinate with events and condition notifications, including the
gap between acquisition commit and capability publication; they do not use sleeps
to choose operation order.
