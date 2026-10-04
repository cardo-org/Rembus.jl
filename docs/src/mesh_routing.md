# Mesh Routing and Forwarding

```@meta
CurrentModule = Rembus
```

This page documents how a Rembus [Mesh Network](@ref) keeps every
broker's routing tables consistent, and how Pub/Sub and RPC messages are
actually forwarded between brokers once those tables are populated. The logic
described here is mostly implemented in `src/admin.jl`, with the data-plane
counterpart (actual message delivery) in `src/broker.jl` and `src/twin.jl`.

## Routing state

Each `Router` keeps the following tables (see `src/types.jl`):

- `topic_impls::Dict{topic => Set{twin}}`: which connected twins expose an
  RPC method named `topic`.
- `topic_interests::Dict{topic => Set{twin}}`: which connected twins are
  subscribed to `topic` (or to a glob pattern).
- `topic_auth::Dict{topic => Dict{rid => true}}`: the set of components
  authorized to use a *private* topic.
- `glob_topics`: the subset of `topic_interests` keys that are "match
  everything" patterns (`**`, `/**/`), kept separate so that broadcasting a
  message does not need to evaluate a regex per subscriber.
- `id_twin::Dict{rid => twin}`: every named twin currently connected to this
  router, i.e. its direct neighbors (downstream components/brokers *and* the
  upstream broker it connects to, if any).
- `eid::UInt64`: an ephemeral id generated for this router instance, used to
  stamp messages as they travel through the mesh (see below).

Because a broker is always also a component (`component(url, ws=port)`), the
twin that represents the *upstream connection* is stored in `id_twin` and
`topic_impls`/`topic_interests` exactly like any other twin: from the point
of view of a router, there is no structural difference between "a local
client" and "another broker in the mesh". This is what allows the same
tables and the same forwarding code to work regardless of how many hops away
the real producer/consumer of a message is.

## Bootstrapping a link: `SETUP_CMD`

When a component connects to a broker (and, symmetrically, when a broker
connects upstream), it sends a `setup` `AdminReqMsg` built by
`twin_setup`/`twin_configuration` (`src/helpers.jl`) that lists the topics it
already exposes/subscribes to:

```julia
Dict("exposers" => [...], "subscribers" => [...], COMMAND => SETUP_CMD)
```

`admin_command` handles `SETUP_CMD` in `src/admin.jl` by registering the twin
in `topic_interests`/`topic_impls` for each announced topic and replying with
`EnableReactiveMsg`. This seeds the router's tables with the state the peer
already had *before* the link existed, so reconnects and newly-joined mesh
nodes immediately become reachable without replaying history.

## Propagating changes: subscribe/expose commands

After the initial setup, every subsequent `subscribe`/`expose`/
`unsubscribe`/`unexpose` admin command issued by a client must be propagated
to *every other broker in the mesh*, not just applied locally, so that a
publisher connected to broker A can reach a subscriber connected to broker C
through broker B. This is done by `admin_command` for `SUBSCRIBE_CMD`,
`EXPOSE_CMD`, `UNSUBSCRIBE_CMD` and `UNEXPOSE_CMD`:

1. Check authorization (`isauthorized`) for private topics.
2. Call `mark_and_broadcast(router, twin, msg)`, which both floods the
   command to this router's neighbors and returns whether the *local* table
   update should actually happen (see loop prevention below).
3. Only if step 2 returns `true`, update `router.topic_interests` /
   `router.topic_impls` for the local twin.

### Loop prevention with `rmark` and `touch`

A mesh is a general graph of brokers, so a naive flood would broadcast
forever on any cycle. Two cooperating markers, both carried inside
`msg.data`, prevent this:

- **`rmark`** (route mark): the list of router `eid`s that have already
  *processed* this command. `mark_and_broadcast` checks whether
  `router.eid` is already in `msg.data["rmark"]`; if so the command has
  already looped back to this router and is dropped (returns `false`,
  so the table is not touched twice and the flood stops). Otherwise the
  router appends its own `eid` and proceeds to broadcast.
- **`touch`** (visited twins): the list of component ids (`rid`) that have
  already *received* this message. `admin_broadcast` iterates over every
  named, open twin connected to this router (`router.id_twin`) and skips:
  the twin that is the origin of the command (`rid(tw) != rid(twin)`), and
  any twin whose id is already in `touch`. For every twin it does send to,
  `color_admin` appends that twin's `rid` to `touch` before calling
  `transport_send`, so the next broker down the line knows not to bounce the
  command straight back.

Together, `rmark` stops re-processing at the router level (idempotency of
the state change) while `touch` stops redundant retransmission at the link
level (every edge of the mesh carries the command at most once in each
direction). The combination means a command issued by any component
eventually reaches every broker/component in the mesh exactly once, no
matter the topology (tree, ring, or arbitrary graph).

```mermaid
sequenceDiagram
    participant Sub as Subscriber (on C)
    participant A as Broker A
    participant B as Broker B
    participant C as Broker C
    Sub->>C: subscribe("temperature")
    C->>C: topic_interests["temperature"] += Sub
    C->>B: admin broadcast (rmark=[C.eid], touch=[Sub])
    B->>B: topic_interests["temperature"] += (twin toward C)
    B->>A: admin broadcast (rmark=[C.eid,B.eid], touch=[Sub,B])
    A->>A: topic_interests["temperature"] += (twin toward B)
    Note over A,C: A does not relay back to B, B does not relay back to C<br/>(already in "touch"); if the link closed a cycle, the<br/>router whose eid is in "rmark" simply stops.
```

After this propagation, every broker on the path from A to C has a
`topic_interests["temperature"]` entry pointing towards the neighbor that is
closer to the real subscriber, which is exactly what the data plane needs
(next section).

`ismultipath(router)` gates whether a router bothers to track routes at all;
it currently always returns `true`; a hook for a future
optimization that would only apply to real brokers rather than plain
request/response pools.

### `admin_broadcast` and the future/response path

`admin_broadcast` also resolves the original command's future if the issuing
twin registered one for `msg.id` (`twin.socket.direct`), so the client that
issued the `subscribe`/`expose` command gets its `STS_SUCCESS` response
immediately, independent of how far the flood still has to travel.

## Data-plane forwarding (Pub/Sub)

Once `topic_interests` is populated mesh-wide, publishing a message
(`pubsub_msg` in `src/twin.jl`) does two things on every broker it passes
through:

1. `local_subscribers(router, twin, msg)`: invokes any locally-registered
   Julia callback bound to the topic (exact match or glob pattern).
2. `broadcast_msg(router, msg)` (`src/broker.jl`): computes the set of
   authorized twins interested in `msg.topic` — the union of
   `topic_interests[topic]` and every twin subscribed through a `glob_topics`
   pattern, filtered by `topic_auth` for private topics and by matching
   `domain(twin)` (multi-tenancy) — and `put!`s the message on each twin's
   inbox, except back onto the twin that published it and non-reactive
   twins.

Because one of those "twins" on each hop is the twin object representing the
*neighbor broker*, publishing the message onto that twin's process inbox is
precisely what makes the message continue its hop-by-hop walk towards the
real subscriber: the neighbor broker receives it like any other inbound
message and repeats the same two steps. No special "forwarding" message type
is required — the router graph built by the admin commands above *is* the
forwarding table.

## Data-plane forwarding (RPC)

RPC requests are routed instead of broadcast, since only one exposer should
execute a given call. `find_implementor` (`src/broker.jl`) looks up
`router.topic_impls[topic]` (populated mesh-wide the same way as
`topic_interests`, via `EXPOSE_CMD`/`UNEXPOSE_CMD` propagation) and:

1. Removes the requestor itself from the candidate list (avoids a method
   calling itself through a loopback route) and reports
   `STS_METHOD_LOOPBACK` if that was the only candidate.
2. Calls `select_twin`, which applies the router's configured policy
   (`:first_up`, `:round_robin`, or `:less_busy`, see `src/broker.jl`) to
   pick one implementor among those connected with a matching tenant
   (`domain(tw) == tenant`).
3. `put!`s the request message onto the chosen twin's inbox.

If the chosen implementor is a local component, the request completes there.
If it is the twin representing a neighbor broker, that broker receives the
request as an ordinary `RpcReqMsg` and repeats `find_implementor` using its
own `topic_impls`, hopping towards the real exposer exactly like a Pub/Sub
message hops towards subscribers. The response travels back the same way,
via `respond`/`rpc_response`, following the chain of requests stored on each
hop rather than needing full-path information.

## Summary

| Concern | Mechanism |
|---|---|
| Discover existing remote state when a link is established | `SETUP_CMD` / `twin_setup` |
| Propagate new subscriptions/exposures mesh-wide | `mark_and_broadcast` + `admin_broadcast` |
| Avoid infinite loops on router state updates | `rmark` (per-router `eid`) |
| Avoid redundant retransmission on each link | `touch` (per-twin `rid`) |
| Forward Pub/Sub messages hop by hop | `broadcast_msg` using `topic_interests` |
| Forward RPC requests hop by hop | `find_implementor`/`select_twin` using `topic_impls` |
| Enforce private topics while forwarding | `topic_auth` / `isauthorized` checked at every hop |

## Function reference

Every function below participates directly in building or using the mesh
routing tables described above. Connection/handshake and reconnection
functions are from `src/twin.jl`; command handling and flooding functions
are from `src/admin.jl`.

### Connection handshake and reconnection (`src/twin.jl`)

```@docs
do_connect
authenticate
await_challenge
attestation
update_tables
get_topics
topic_impls
topic_interests
_topics
setidentity
reconnect
handle_connection
remove_twin
destroy_twin
end_receiver
```

### Message handling and forwarding (`src/twin.jl`)

```@docs
command_permitted
pubsub_msg
admin_msg
rpc_request
manage_target
sendto_origin
```

### Administration commands and mesh flooding (`src/admin.jl`)

```@docs
isadmin
isauthorized
private_topic
public_topic
authorize
unauthorize
shutdown_broker
color_admin
admin_broadcast
mark_and_broadcast
ismultipath
admin_command
```

