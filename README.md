# StatsHouse Go client

The package aggregates metrics and sends batches to a StatsHouse agent using TCP,
UDP or a Unix datagram socket. Existing `NewClientEx`, `ConfigureEx` and recording
functions retain their default unlimited aggregation behaviour.

## Optional shared memory budget

`MemoryBudget` limits **accounted SDK-owned retained storage**, not process RSS.
Pass the same budget to several clients to bound their combined storage, or use
separate budgets for independent limits. The application chooses the size; the
SDK does not impose a new default.

```go
func newTelemetryClient(limitBytes int64) (*statshouse.Client, error) {
    budget, err := statshouse.NewMemoryBudget(limitBytes)
    if err != nil {
        return nil, err
    }
    return statshouse.NewClientWithBudget(statshouse.ConfigureArgs{
        Network:        "tcp",
        StatsHouseAddr: statshouse.DefaultAddr,
    }, budget)
}
```

`NewClientWithBudget` returns an error without starting a client if mandatory
storage does not fit. Always handle the error, and close a successfully created
client when it is no longer needed. Reuse `budget` in another
`NewClientWithBudget` call to share the limit.

To configure the global client, call `ConfigureWithBudget(args, budget)` **before
creating any metric references, recording observations or registering callbacks**.
This includes cached references created by package initializers. Otherwise it
returns `ErrClientStarted` and preserves the old configuration. Applications with
existing initialization-time references can instead create a separate bounded
client. Ignoring a configuration error does not enable a budget.

A bounded client can be reconfigured with the same budget. Replacing its budget
is rejected because existing references may retain allocations. Configuration is
transactional on memory-admission failure. Calling the existing `ConfigureEx` on
a bounded client preserves its budget and logs a rejected configuration. Configure
clients before concurrent recording; recording and snapshots support concurrent
use once configured.

### Accounting and overflow

The accounting includes:

- SDK object metadata, metric indexes and callback registry storage;
- owned metric names, numeric/named labels and StringTop strings;
- active and send-side numeric, unique and StringTop backing capacities;
- producer packets, both TCP queues and in-flight packet buffers;
- owned configuration, handshakes, target lists and temporary SDK address lists.

Charges use architecture-specific SDK structure sizes, backing-array capacities
and owned string byte lengths. Growth reserves the replacement allocation while
the old storage is still charged. Returned statistics include the budget's own
metadata. Sharing a budget shares all of these charges and rejection statistics.

The limit **does not** include Go allocator size-class rounding, fragmentation,
not-yet-collected garbage, goroutine stacks, runtime channel/timer/finalizer
bookkeeping, resolver/socket internals, kernel buffers or application-owned
callback closures or caller-owned reference wrappers/containers. The buckets
retained through those references are still charged. Provision runtime and allocator headroom separately. Neither
this accounting nor `GOMEMLIMIT` guarantees a hard whole-process RSS limit.

Storage held by detached but still reachable `MetricRef` objects remains charged.
Finalization releases remaining object/key charges after references become
unreachable; the library never forces garbage collection. Conservative charges
can therefore delay admission until collection. `Close` releases payloads and
registries, but fixed metadata and live references can remain charged afterward.

Exhausting the budget rejects new storage immediately, without waiting for memory,
GC or telemetry delivery. A rejected reference has `IsValid() == false` and its
write methods safely do nothing. Batch observations can be partially admitted;
local drop counters report rejected observations. Existing buckets can continue
recording within their available storage. Normal mutex synchronization still
applies; historical writes and configuration can also perform bounded transport
initialization. The API is not a lock-free or wait-free interface.

`StartRegularMeasurement` returns zero when its registry cannot grow; zero is not
a registered ID and can safely be passed to `StopRegularMeasurement`.

## TCP deadlines and shutdown

Handshake, data and loss-report writes receive a write deadline. Deadline refresh
is safe for the first write, reconnect and a long idle period. Reconnects and DNS
refresh are cancelled on shutdown. `Close` attempts a best-effort flush and gives
TCP transport work one shared shutdown cutoff (15 seconds by default), rather
than waiting a full write timeout for each queued packet. Remaining packets are
discarded and their storage is released. Repeated/concurrent `Close` calls wait
for the same completion.

User-provided regular measurement callbacks and loggers must return for shutdown
to complete. A measurement callback must not call `Close` on its own client.

## Local diagnostics

`client.Snapshot()`, package-level `Snapshot()` and `budget.Snapshot()` perform no
network requests, bucket walks or allocations. They remain usable while the
telemetry destination is unavailable. Snapshot fields are individually atomic;
a multi-field snapshot is weakly consistent during concurrent updates.

Client statistics expose indexed aggregation cardinality, queued/in-flight packet
counts and payload bytes, bounded-aggregation admission/closed-client observation drops, transport
packet/byte drops, dial
attempts/errors, successful reconnects and write errors. Memory statistics expose
the shared limit, used/peak charge and rejected reservations. Queue byte gauges
measure payload length, while the memory budget charges backing capacity.

Cumulative counters saturate at `math.MaxUint64`. Aggregation drops count
observations, transport drops count discarded packets and their payload bytes;
a counter update counts as one aggregation observation, while a batch counts its
individual samples. Rejected reference creation alone is not an observation.
These are different units. Transport counters describe local discards, not an
end-to-end measurement of delivery loss. They survive reconnect/reconfiguration
and are independent of the best-effort in-band loss report. The report itself
can be lost, and its floating-point wire value cannot exactly represent all
64-bit integers. Snapshots never expose metric keys, labels or configuration.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem ./...
```

Tests use fake connections, `net.Pipe` and controlled timeouts to cover healthy,
refused, stalled, reconnect/recovery and shutdown paths. Budget tests cover shared
clients, high cardinality, StringTop, historical writes, retained references,
concurrent admission and failures during storage growth.
