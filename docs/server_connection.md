# Server / Connection resource example

The resource layer separates a reusable description (`Resource`) from one runtime
allocation (`ResourceHandle`). A server handle may own dynamic connection handles while
capabilities expose only the values consumers need.

```python
from lifecycle.capability import CapabilityLifecycleCoordinator
from lifecycle.resource import Resource, ResourceRecoveryPool

pool = ResourceRecoveryPool()


def server_resource(port: int) -> Resource[ServerCapability]:
    return Resource.make(
        acquire=lambda: open_server(port),
        release=lambda server: close_server_with_finite_retry(server),
    ).map(lambda server: ServerCapability(server))


server = CapabilityLifecycleCoordinator(server_resource, pool)
server.acquire(0, 5037)


def connection_resource(
    server_capability: ServerCapability,
    serial: str,
) -> Resource[ConnectionCapability]:
    return Resource.make(
        acquire=lambda: open_connection(server_capability, serial),
        release=lambda connection: close_connection_with_finite_retry(connection),
    ).map(lambda connection: ConnectionCapability(connection))


connections = server.create_child(0, connection_resource)
connections.acquire(0, "device-1")
```

`Resource.map` changes only the exposed capability. The registered finalizer still owns
and releases the original server or connection object.

A child coordinator created with `create_child` is bound to the exact parent handle that
was active at creation time. Reacquiring the server does not retarget an existing child
coordinator to the new server instance.

Releasing the server closes still-live connection handles in reverse registration order
before finalizing the server itself. Releasing one connection earlier detaches it from
that parent scope and does not close the server or its siblings.

If a finalizer exhausts its own finite retry policy and raises, the failed responsibility
is transferred once to `ResourceRecoveryPool`; remaining sibling and parent finalizers
still run. `release()` therefore means the scope completed finalization or responsibility
handoff, not that every physical object was necessarily destroyed.

## Recovery-pool dependency rule

A finalizer that may be transferred to the recovery pool must represent a detached
recovery responsibility. Its recovery action must remain meaningful after ancestor
handles are finalized. If cleanup fundamentally requires a live ancestor, model that
lifetime as a single resource scope or make the transferred recovery token retain the
necessary dependency explicitly. The pool does not keep parent handles alive and does
not perform retries or background cleanup.

## Multi-step acquisition rule

`Resource.make` can register a finalizer only after its acquire function returns. Do not
hide several independently owned physical acquisitions inside one acquire callback when
partial failure can occur. Express them compositionally so each successful step is
registered immediately:

```python
server_socket.flat_map(
    lambda socket: server_process(socket).flat_map(
        lambda process: Resource.pure(ServerCapability(socket, process))
    )
)
```

If a single acquire callback creates internal state and raises before returning, that
callback remains responsible for cleaning the state that was never transferred to the
framework.
