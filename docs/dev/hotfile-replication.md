# Hot File Replication

Hot file replication automatically creates additional replicas of files that are being read
frequently (i.e. "hot" files), distributing load across the pool fleet.

## How It Works

### Protocol-Agnostic Design

All dCache read protocols (DCap, NFS/pNFS, WebDAV, xrootd, FTP, HTTP, pool-to-pool) send a
`PoolIoFileMessage` to the pool when a client requests a file. The pool dispatches that message
to `PoolV4.ioFile()`, which is therefore a single, protocol-independent entry point for every
read request. Hot file monitoring is implemented at that point:

```
Any Door (DCap / NFS / WebDAV / xrootd / FTP / HTTP / …)
  → sends PoolIoFileMessage
    → Pool.messageArrived(PoolIoFileMessage)
      → PoolV4.ioFile()          ← monitoring happens here
        → queues mover (protocol-specific)
        → FileRequestMonitor.reportFileRequest(pnfsId, currentCount, protocolInfo)
```

Because the counting and triggering happen in `PoolV4.ioFile()`, no protocol-specific code
changes are required to benefit from this feature.

### Request Counting

`PoolV4.ioFile()` reads the concurrent mover count for the file from `IoQueueManager`:

```java
long requestCount = _ioQueue.numberOfRequestsFor(message.getPnfsId());
_fileRequestMonitor.reportFileRequest(message.getPnfsId(), requestCount,
      message.getProtocolInfo());
```

When `requestCount` reaches or exceeds the configured threshold,
`MigrationModule.reportFileRequest()` creates a migration job named
`hotfile-<pnfsId>` that replicates the file to additional pools.

### Pool Selection

The migration job selects target pools by querying PoolManager via `PoolMgrQueryPoolsMsg`,
deriving `protocolUnit` from the triggering request's `ProtocolInfo` (e.g., `"DCap/3"`) and
`netUnitName` from the client's IP address when available (e.g., `"192.168.1.10"`). When the
client IP is not available (non-IP protocol or unknown), an empty string is used for `netUnitName`,
which causes PoolManager to match any network unit. When `ProtocolInfo` is null (e.g., for
internal pool-to-pool transfers), `protocolUnit` falls back to `"*/*"` and `netUnitName` to `""`
so that selection is based solely on the file's storage group and pool-group read preferences.

`PoolMgrQueryPoolsMsg.getPools()` returns a `List<String>[]` where index 0 is the highest
read-preference level. `PoolListByPoolMgrQuery` selects **only** the first non-empty
preference level, so the file is always replicated to the best available pools:

```java
// Only take the first non-empty preference level (highest read preference)
for (int i = 0; i < poolLists.length; i++) {
    List<String> poolList = poolLists[i];
    if (poolList != null && !poolList.isEmpty()) {
        selectedPools = poolList;
        break;
    }
}
```

Prior to this, all preference levels were flattened into a union, causing files to be
replicated to pools from lower-preference groups (e.g. flush pools) instead of the intended
read-only pools.

### Job Housekeeping

To prevent unbounded memory growth, `MigrationModule` keeps at most 50 hotfile jobs. When a
new job would exceed that limit, the oldest jobs that have reached a terminal state
(`FINISHED`, `CANCELLED`, `FAILED`) are pruned first.

## Configuration

| Property | Default | Description |
|---|---|---|
| `pool.hotfile.replication.enable` | `false` | Enable/disable hot file monitoring. **Must be `true` to activate.** |
| `pool.migration.hotfile.threshold` | `50` | Number of concurrent read movers required to trigger replication |
| `pool.migration.hotfile.replicas` | `1` | Number of additional replicas to create |
| `pool.migration.concurrency.default` | `1` | Number of files the migration job migrates concurrently |

Example (`dcache.conf` or pool layout file):

```ini
pool.hotfile.replication.enable = true
pool.migration.hotfile.threshold = 3
pool.migration.hotfile.replicas = 3
pool.migration.concurrency.default = 1
```

> **Note:** The feature is disabled by default. A pool restart is required after any
> configuration change.

## Key Source Files

| File | Role |
|---|---|
| `modules/dcache/src/main/java/org/dcache/pool/classic/PoolV4.java` | Entry point; checks enable flag, calls `FileRequestMonitor` |
| `modules/dcache/src/main/java/org/dcache/pool/migration/MigrationModule.java` | Implements `FileRequestMonitor`; counts requests, creates and manages migration jobs |
| `modules/dcache/src/main/java/org/dcache/pool/migration/PoolListByPoolMgrQuery.java` | Queries PoolManager for eligible target pools; selects highest-preference level only |
| `modules/dcache/src/test/java/org/dcache/pool/classic/HotfileMonitoringTest.java` | Spring-context integration test for enable/disable behaviour |
| `modules/dcache/src/test/java/org/dcache/pool/migration/MigrationModuleTest.java` | Unit tests for `reportFileRequest`, threshold, housekeeping |
| `modules/dcache/src/test/java/org/dcache/pool/migration/PoolListByPoolMgrQueryTest.java` | Unit tests for pool selection, preference-level handling, unknown net unit, and wildcard protocol |
| `skel/share/defaults/pool.properties` | Canonical defaults for all `pool.hotfile.*` and `pool.migration.hotfile.*` properties |

## Diagnostics

### Log Messages

With the default log level the following INFO messages are emitted by `MigrationModule`:

```
Hot file monitoring: pnfsId=<id>, requests=<n>, threshold=<t>
Hot file detected! Triggering replication for pnfsId=<id>
Created migration job with id hotfile-<id> for pnfsId <id> with <n> replicas and concurrency <c>
Starting migration job hotfile-<id> for pnfsId <id>
Successfully started migration job hotfile-<id> for pnfsId <id>
Job hotfile-<id> already exists with state <STATE>
```

`PoolV4` emits at INFO:

```
PoolV4.ioFile: Received IO request for pnfsId=<id>, hotFileEnabled=<bool>, monitorSet=<bool>
PoolV4.ioFile: Calling reportFileRequest for pnfsId=<id>, count=<n>
```

And at ERROR if the monitor is not wired:

```
PoolV4.ioFile: Hot file replication enabled but FileRequestMonitor is NULL!
```

`PoolListByPoolMgrQuery` emits at DEBUG when a preference level is selected:

```
Selected preference level <i> with <n> pools for <pnfsId>: [pool1, pool2, …]
```

### Runtime Log Level Adjustment

```
# In the pool's admin shell
log set org.dcache.pool.classic.PoolV4 DEBUG
log set org.dcache.pool.migration.MigrationModule DEBUG
```

### Checking Job and Replica Status

```bash
# List active migration jobs on all pools
ssh -p <admin-port> admin@<host> '\s <pool-pattern> migration ls'

# Inspect a specific job
ssh -p <admin-port> admin@<host> '\s <pool-name> migration info hotfile-<pnfsId>'

# List replicas of a file
ssh -p <admin-port> admin@<host> '\sl <pnfsId> rep ls <pnfsId>'
```

### Interpreting Absence of Log Messages

| Symptom | Likely Cause |
|---|---|
| No `PoolV4.ioFile` messages | IO requests are not reaching the pool, or the feature is disabled (`hotFileEnabled=false`) |
| `monitorSet=false` in PoolV4 log | `FileRequestMonitor` not wired — check Spring context startup errors |
| `requests` stays at 1 | IoQueue not counting movers correctly |
| "Hot file detected" but no job created | Exception during job creation — check ERROR lines for a stack trace |
| Job created but not started | `MigrationModule` not started — run `migration start` in the admin interface |
| "Job already exists" repeating | Previous job is stuck in a non-terminal state — inspect job state |
