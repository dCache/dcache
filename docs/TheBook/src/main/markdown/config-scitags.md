SciTags Flow Marking (Firefly) in dCache
========================================

This document explains how SciTags (Firefly flow markers) are implemented in dCache.
It is written for two audiences:

- Site operators and advanced users who want to understand configuration and behavior.
- Developers who want to debug, validate, or extend the implementation.

## Scope and terminology

Within dCache, SciTags classification data is carried as a transfer tag string (`transferTag`) in
door-to-pool protocol info objects. Pool-side code then derives `experiment-id` and `activity-id`
and emits a Firefly marker over UDP.

In this document:

- "SciTag value" means the integer encoded by clients.
- "transferTag" means the dCache protocol field that carries that value as text.
- "Firefly marker" means the JSON payload emitted via syslog-formatted UDP packet.

## Fast operator summary

The behavior is:

1. The door extracts SciTags data from protocol-specific request metadata.
2. The pool validates and decodes the value (or falls back to VO-to-experiment mapping via FQAN).
3. dCache sends Firefly markers to:
   - the data-flow destination host on UDP 10514 (default), and
   - optionally, an additional configured collector.

Supported marker protocols in current pool logic:

- `xrootd`
- `http`
- `https`
- `remotehttpdatatransfer`
- `remotehttpsdatatransfer`

SciTag encoding follows:

$$
scitag = (experiment\_id << 6) \;|\; activity\_id
$$

$$
experiment\_id = scitag >> 6
$$

$$
activity\_id = scitag \& 0x3F
$$

Accepted SciTag value range in current implementation is `64..65535` (inclusive).

## End-user and operator configuration

Primary settings are in pool configuration defaults:

- `pool.enable.firefly`
- `pool.firefly.destination`
- `pool.firefly.excludes`
- `pool.firefly.vo-mapping`
- `pool.firefly.storage-statistics`

Reference defaults: `skel/share/defaults/pool.properties`.

Typical site configuration in `/etc/dcache/dcache.conf`:

```ini
pool.enable.firefly=true

# Optional additional collector (host or host:port).
pool.firefly.destination=us.scitags.org:10333

# Optional list of subnets to exclude from marker emission when BOTH endpoints are local.
pool.firefly.excludes=10.0.0.0/8,192.168.0.0/16

# Fallback mapping when transferTag is missing (keys are lower-cased VO names).
pool.firefly.vo-mapping=atlas:2,cms:3,lhcb:4,alice:5

# Include storage statistics in end markers.
pool.firefly.storage-statistics=true
```

Notes:

- If `pool.firefly.destination` is unset, markers are sent only to the peer host at UDP 10514.
- If set, dCache sends an additional copy to that collector (unless it equals the primary destination).
- Exclusion requires both source and destination addresses to match configured local subnets.

## Request-side extraction by protocol

### WebDAV / HTTP door path

For regular WebDAV requests, SciTag extraction is in
`modules/dcache-webdav/src/main/java/org/dcache/webdav/DcacheResourceFactory.java`.

Order of precedence is:

1. `SciTag` header (case-insensitive lookup)
2. `TransferHeaderSciTag` header (case-insensitive lookup)
3. `scitag.flow` query parameter

Blank values are ignored and treated as not present.

The resulting string is copied into `HttpProtocolInfo.setTransferTag(...)` and propagated to the pool.

### WebDAV Third-Party Copy (TPC)

For COPY/TPC, capture and forwarding are in
`modules/dcache-webdav/src/main/java/org/dcache/webdav/transfer/CopyFilter.java` and
`modules/dcache-webdav/src/main/java/org/dcache/webdav/transfer/RemoteTransferHandler.java`.

Behavior:

- `CopyFilter` captures `SciTag` and `TransferHeaderSciTag` request headers.
- Missing or blank header values are normalized to sentinel `-` for request attributes.
- Pool transferTag precedence is:
  1. `SciTag`
  2. `TransferHeaderSciTag`
  3. empty string
- `RemoteTransferHandler` copies this into
  `RemoteHttpDataTransferProtocolInfo` / `RemoteHttpsDataTransferProtocolInfo` via `setTransferTag(...)`.

### XRootD door path

XRootD extraction is in
`modules/dcache-xrootd/src/main/java/org/dcache/xrootd/door/XrootdTransfer.java`.

Behavior:

- transferTag is read from opaque parameter `scitag.flow`.
- Missing parameter yields empty string.
- Value is copied into `XrootdProtocolInfo.setTransferTag(...)`.

Door call-site: `XrootdDoor.createTransfer(...)` constructs `XrootdTransfer`, and
`XrootdTransfer` handles SciTag extraction and propagation into protocol info.

### Important validation boundary

Door code primarily captures and propagates tag values; SciTag numeric validation is pool-side.

## Client examples

### Compute a SciTag value

Example for `experiment-id=2` and `activity-id=1`:

$$
scitag = (2 << 6) | 1 = 129
$$

### WebDAV / HTTP request

```bash
curl -H "SciTag: 129" "https://dcache.example.org/pnfs/example.org/data/file.dat"
```

### WebDAV query fallback

```bash
curl "https://dcache.example.org/pnfs/example.org/data/file.dat?scitag.flow=129"
```

### XRootD request

```bash
xrdcp "root://dcache.example.org//pnfs/example.org/data/file.dat?scitag.flow=129" /tmp/file.dat
```

## Pool-side lifecycle and marker emission

Core implementation:

- `modules/dcache/src/main/java/org/dcache/pool/movers/TransferLifeCycle.java`

The pool emits markers on transfer start/end with guard checks:

1. Firefly enabled.
2. Not excluded by local subnet rules.
3. Protocol supported.
4. Experiment ID resolvable (from transferTag or VO fallback).

If any check fails, marker emission is skipped.

### Start marker invocation path

Current invocation is from:

- `modules/dcache/src/main/java/org/dcache/pool/movers/NettyTransferService.java`

`NettyTransferService.executeMover(...)` calls `transferLifeCycle.onStart(...)` with:

- source = remote endpoint from `IpProtocolInfo.getSocketAddress()`
- destination = local pool endpoint chosen for mover

### End marker invocation path

Current invocation is from:

- `modules/dcache/src/main/java/org/dcache/pool/classic/DefaultPostTransferService.java`

`DefaultPostTransferService.sendFinished(...)`:

- uses `((IpProtocolInfo) mover.getProtocolInfo()).getSocketAddress()` as source,
- uses `mover.getLocalEndpoint()` as destination when available,
- otherwise tries `deriveLocalEndpoint(remoteEndpoint)` via `NetworkUtils.getLocalAddress(...)`,
- calls `transferLifeCycle.onEnd(...)` when either direct or derived local endpoint is available,
- logs structured skip reasons when end-marker invocation cannot proceed.

### Developer note on service paths

The only direct call to `TransferLifeCycle.onStart(...)` is currently in `NettyTransferService`.
`TransferLifeCycle.onEnd(...)` is invoked from `DefaultPostTransferService` when a local
endpoint is available from the mover or can be derived from the remote endpoint.

## Classification logic

Classification is implemented in `TransferLifeCycle` as follows.

### transferTag path (preferred)

- Parse `protocolInfo.getTransferTag()` as integer.
- Reject values outside `64..65535`.
- Decode:
  - experiment-id = `transferTag >> 6`
  - activity-id = `transferTag & 0x3F`

### FQAN fallback path

If transferTag is empty/missing:

- read primary FQAN from subject,
- get group path,
- lowercase,
- split on first `/` boundary,
- map first segment (VO name) via `pool.firefly.vo-mapping`.

Fallback activity-id defaults to `1`.

### Exclusion logic

`pool.firefly.excludes` builds an OR-combined subnet predicate.
Transfer is excluded only when both source and destination addresses match excluded subnets.

## Firefly payload details

Payload builder:

- `modules/dcache/src/main/java/org/dcache/net/FlowMarker.java`

Marker wire format:

- syslog header prefix: `<134>1 ... dCache - firefly-json - `
- followed by JSON body

Main JSON sections:

- `version`
- `flow-lifecycle` with `state`, `start-time`, optional `end-time`, `current-time`
- `flow-id` with AFI, IPs, ports, and protocol
- `context` with `experiment-id`, `activity-id`, and application/protocol name
- `usage` (`received`, `sent`)
- optional `storage-stats` when `pool.firefly.storage-statistics=true`

For end markers, `start-time` is reconstructed from mover submit time and `end-time`
is derived as `start-time + max(connectionTime, 0)`.

Test schema file:

- `modules/dcache/src/test/resources/org/dcache/net/firefly.schema.json`

## Logging and debugging

SciTags debug logging uses dedicated logger `org.dcache.scitags` with structured key/value
messages. This includes request capture at doors, start-abort diagnostics, end-path skip/invoke
decisions, and marker send/skip events.

Relevant class loggers are:

- `org.dcache.scitags` (DEBUG): structured SciTags lifecycle/request/marker diagnostics.
- `org.dcache.pool.movers.TransferLifeCycle` (WARN): invalid transferTag values,
  invalid VO mapping entries, and marker send failures.

### Enabling debug output for SciTags-related classes

Because dCache uses `CellThresholdFilter`, adding a logger alone is insufficient.
You typically need both:

1. logger entry
2. matching threshold entry inside `CellThresholdFilter`

Template example for `/etc/dcache/logback.xml`:

```xml
<logger name="org.dcache.scitags" level="DEBUG"/>

<turboFilter class="dmg.util.logback.CellThresholdFilter">
  ...
  <threshold>
    <logger>org.dcache.scitags</logger>
    <level>debug</level>
  </threshold>
  ...
</turboFilter>
```

Reference template with `CellThresholdFilter`: `skel/etc/logback.xml`.

### Useful structured message patterns

Look for:

- `scitags event=request ... tagSource=... transferTag=...`
- `scitags lifecycle=start abort reason=interrupted ...`
- `scitags lifecycle=start abort reason=execution-failed ...`
- `scitags lifecycle=end skip reason=...`
- `scitags lifecycle=end invoke ...`
- `scitags event=marker-skip reason=...`
- `scitags event=marker state=start ...`
- `scitags event=marker state=end ... bytesRead=... bytesWritten=...`

Marker skip conditions now carry explicit `reason=` codes in structured debug messages.

### Packet-level verification

On the pool host, verify UDP emission (example):

```bash
tcpdump -nn -i any udp port 10514
```

If `pool.firefly.destination` is configured, also verify traffic to the collector host/port.

## Developer extension guide

### Add a new protocol to SciTags

Minimum steps:

1. Door-side: extract and set `transferTag` in that protocol's `ProtocolInfo`.
2. Pool-side: ensure `TransferLifeCycle.needMarker(...)` recognizes the protocol name.
3. Lifecycle hooks: confirm start and end invocation paths are wired for that mover service.
4. Tests: add/adjust unit and integration tests.

### Change SciTag validation or encoding

Edit in `TransferLifeCycle`:

- `MIN_VALID_TRANSFER_TAG`
- `MAX_VALID_TRANSFER_TAG`
- bit decode constants (`EXPERIMENT_ID_BIT_SHIFT`, `ACTIVITY_ID_MASK`)

Then update/extend tests in:

- `modules/dcache/src/test/java/org/dcache/pool/movers/TransferLifeCycleTest.java`

### Change header precedence or WebDAV behavior

Edit:

- `DcacheResourceFactory.HttpTransfer.readTransferTag(...)`
- `CopyFilter.transferTagForPool(...)`

And update:

- tests under `modules/dcache-webdav/src/test/java/org/dcache/webdav/`
- tests under `modules/dcache-webdav/src/test/java/org/dcache/webdav/transfer/`

### Extend payload content

Edit:

- `FlowMarker.FlowMarkerBuilder`

Then update schema and tests accordingly.

## Existing tests relevant to SciTags

- `TransferLifeCycleTest`
  - accepts minimum valid tag (`64`)
  - rejects below-range values (`63`)
  - verifies slash-prefixed FQAN mapping
  - verifies exclusion behavior

WebDAV unit coverage includes case-insensitive SciTag header lookup in:

- `modules/dcache-webdav/src/test/java/org/dcache/webdav/DcacheResourceFactoryTest.java`

There is no dedicated unit test that exercises full `HttpTransfer.readTransferTag(...)`
precedence end-to-end.

## Source map (quick navigation)

- Pool marker logic: `modules/dcache/src/main/java/org/dcache/pool/movers/TransferLifeCycle.java`
- Marker start hook: `modules/dcache/src/main/java/org/dcache/pool/movers/NettyTransferService.java`
- Marker end hook: `modules/dcache/src/main/java/org/dcache/pool/classic/DefaultPostTransferService.java`
- HTTP/WebDAV extraction: `modules/dcache-webdav/src/main/java/org/dcache/webdav/DcacheResourceFactory.java`
- WebDAV TPC forwarding: `modules/dcache-webdav/src/main/java/org/dcache/webdav/transfer/CopyFilter.java`
- Remote HTTP TPC propagation:
  `modules/dcache-webdav/src/main/java/org/dcache/webdav/transfer/RemoteTransferHandler.java`
- XRootD extraction: `modules/dcache-xrootd/src/main/java/org/dcache/xrootd/door/XrootdTransfer.java`
- XRootD transfer creation call-site: `modules/dcache-xrootd/src/main/java/org/dcache/xrootd/door/XrootdDoor.java`
- Flow marker payload builder: `modules/dcache/src/main/java/org/dcache/net/FlowMarker.java`
- Pool defaults: `skel/share/defaults/pool.properties`
- Lifecycle bean wiring: `modules/dcache/src/main/resources/org/dcache/pool/classic/pool.xml`
- Logback template with threshold filter: `skel/etc/logback.xml`
