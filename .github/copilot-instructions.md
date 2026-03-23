# dCache AI Coding Instructions

You are an expert developer working on **dCache**, a distributed storage system for storing and retrieving huge amounts of data.

## 🏗 Project Architecture & "Big Picture"

- **Cell-Based Architecture:** The core of dCache is the "Cell" concept (`dmg.cells.nucleus.Cell`).
  - Components run as cells and communicate via message passing.
  - **Key Interface:** `dmg.cells.nucleus.CellMessageReceiver` — implement `messageArrived(SomeMessage msg)` methods for automatic dispatch.
  - **Base Class for new cells:** Extend `dmg.cells.nucleus.CellAdapter` directly, or extend `org.dcache.cells.AbstractCell` for the higher-level `@Option`-parsed configuration and automatic message dispatch.
  - **Communication:** Use `org.dcache.cells.CellStub` for sending messages to other cells. Avoid direct Java method calls across cell boundaries.
  - **CellPath:** `dmg.cells.nucleus.CellPath` identifies a destination cell for message routing.
- **Key Cell Nucleus Classes** (`modules/cells/src/main/java/dmg/cells/nucleus/`):
  - `CellAdapter` — base implementation of a cell (most cells extend this directly or indirectly)
  - `AbstractCellComponent` — lightweight component that can be injected into a cell
  - `CellMessage` — the envelope wrapping any inter-cell message
  - `CellMessageReceiver` — interface marking a class as a message handler (uses `messageArrived` dispatch)
  - `CellCommandListener` — interface for `@Command`-annotated admin commands
  - `CellNucleus` — the runtime container for a cell
  - `CellEndpoint` — the sending side of a cell
- **Modules:**
  - `modules/cells` — core communication framework (Nucleus)
  - `modules/dcache` — core dCache logic: Pools, Doors, PoolManager, PnfsManager, namespace
  - `modules/chimera` — namespace/filesystem metadata provider (JDBC-based, uses Spring JdbcTemplate)
  - `modules/dcache-chimera` — integration layer between dCache and Chimera
  - `modules/dcache-frontend` — REST API (JAX-RS via Jersey, package `org.dcache.restful`)
  - `modules/dcache-bulk` — bulk operation service
  - `modules/dcache-qos` — Quality of Service engine (replaces resilience)
  - `modules/dcache-resilience` — older resilience/replication manager
  - `modules/dcache-spacemanager` — space reservation (SRM)
  - `modules/dcache-nfs` — NFS door (NFSv4.1)
  - `modules/dcache-ftp` — FTP/GridFTP door
  - `modules/dcache-webdav` — WebDAV/HTTP door
  - `modules/dcache-xrootd` — XRootD door
  - `modules/dcache-dcap` — dCap protocol door
  - `modules/dcache-gplazma` — authentication/authorization (gPlazma)
  - `modules/gplazma2*` — individual gPlazma plugin modules (LDAP, Kerberos, OIDC, VOMS, etc.)
  - `modules/dcache-info` — info provider (monitoring)
  - `modules/dcache-history` — historical statistics
  - `modules/dcache-srm` — SRM (Storage Resource Manager) implementation (**skipped in standard builds** — see `src: skip SRM when building and testing dCache`)
  - `modules/dcache-nearline-spi` — SPI for nearline storage (HSM tape) plugins
  - `modules/common` — shared utilities (e.g., `diskCacheV111.util.FsPath`, `diskCacheV111.util.CacheException`)
  - `modules/common-security` — shared security utilities
  - `modules/acl` / `modules/acl-vehicles` — ACL support
  - `modules/srm-client` / `modules/srm-common` / `modules/srm-server` — SRM protocol
- **Key Legacy Packages** (still heavily used):
  - `diskCacheV111.vehicles.*` — message/vehicle classes (extend `diskCacheV111.vehicles.Message`)
  - `diskCacheV111.poolManager.*` — PoolManager logic
  - `diskCacheV111.util.*` — utilities including `CacheException`, `PnfsId`, `FsPath`
  - `diskCacheV111.namespace.*` — namespace provider interfaces
- **Data Flow:**
  - Clients → Doors (Protocol Endpoints) → PoolManager → Pools (Storage Nodes)
  - Pools handle direct data transfer (movers, Netty-based) with clients
  - PnfsManager (`diskCacheV111.namespace.PnfsManagerV3`) handles namespace operations
- **Domain Model:**
  - Cells run inside **domains** (each domain = one JVM process).
  - Cell address format: `cellName@domainName` — use this when constructing `CellPath`.
  - `CellPath` can route through multiple hops: `new CellPath("cellA@domain1", "cellB@domain2")`
- **Key Domain Types:** `dCacheDomain` (core services), `poolDomain` (storage), `door` domains (protocol endpoints).
- **Critical Common Classes:**
  - `diskCacheV111.util.PnfsId` — the primary identifier for every file in dCache. Used everywhere.
  - `diskCacheV111.util.FsPath` — type-safe representation of a namespace path.
  - `diskCacheV111.util.CacheException` — base for all dCache-specific errors; has an error code (`int rc`).
  - `javax.security.auth.Subject` — security context passed through operations; populated by gPlazma during login.

## 🛠 Development Workflow

- **Build System:** Maven
  - **Preferred Build Command:** `mvn -U -am package -pl <module-path>`
    - Use `-U` to force update of snapshots.
    - Use `-am` (also make dependents) to ensure consistency.
  - **Module Paths for Targeted Builds:**
    - Core module: `modules/dcache` (artifact: `dcache-core`)
    - Cells module: `modules/cells` (artifact: `dcache-cells`)
    - To build dcache-core: `mvn -U -am package -pl modules/dcache`
    - To build with tests: `mvn -U -am package -pl modules/dcache` (tests run by default)
    - To skip tests: add `-DskipTests`
    - To compile tests only: `mvn test-compile -pl modules/dcache`
  - **Clean:** `mvn clean`
- **Java Version:** Java 17 is required.
- **Key Libraries (qualitative):**
  - **Spring Framework** — XML-based configuration (NOT Spring Boot). Use `@Required`, `@PostConstruct`, `@PreDestroy`.
  - **Guava** — used extensively for `ListenableFuture`, `Futures`, collections utilities.
  - **Netty** — used for pool movers and protocol doors.
  - **JUnit 4** is the primary test framework; some newer tests use JUnit 5.
  - **Mockito** — used for mocking in unit tests.
- **Testing:**
  - **Unit Tests:** JUnit 4 is primary. Run with `mvn test`.
  - **Running Individual Tests:** `mvn test -Dtest=ClassName#methodName -pl modules/dcache`
  - **Running All Tests in a Class:** `mvn test -Dtest=ClassName -pl modules/dcache`
  - **⚠️ Warning:** Building or running individual tests can be flaky due to complex dependencies. Prefer building the full module or project when possible.
  - **Static Analysis:** SpotBugs is used.
  - **Integration:** Robot Framework (system tests).

## 📝 Coding Conventions & Patterns

- **Style:** Follow the **Google Java Style Guide** (modified).
  - Imports are ordered specifically.
  - Strict whitespace and formatting rules.
- **Concurrency:**
  - dCache is highly concurrent.
  - **MUST** use `@GuardedBy` annotations (from `javax.annotation.concurrent`) to document lock usage.
  - Prefer `java.util.concurrent` utilities over raw threads.
- **Configuration (Spring XML + `@Required`):**
  - dCache uses **Spring 5 XML configuration** (not Spring Boot / `@SpringBootApplication`).
  - Bean wiring is done via XML; setter injection is common with `@Required` annotations (611+ usages).
  - `@PostConstruct` / `@PreDestroy` are used for lifecycle hooks.
  - `AbstractCell` supports `@Option`-annotated fields for command-line style option parsing at cell startup.
- **Message Dispatch Pattern:**
  - Cells implement `CellMessageReceiver` and expose `messageArrived(SomeSpecificMessage msg)` methods.
  - The cell framework automatically routes incoming messages to the correct overload by message type.
  - Example: `public void messageArrived(PoolManagerGetPoolListMessage msg) { ... }`
  - For access to the envelope: `public void messageArrived(CellMessage envelope, SomeMessage msg) { ... }`
- **Async & Messaging Patterns:**
  - **CellStub.send()** returns `com.google.common.util.concurrent.ListenableFuture<T>` (Guava) in most overloads.
  - **There is also a `CompletableFuture<T>` overload** — check the specific signature before use.
  - **CellStub.sendAndWait()** is the blocking variant — use sparingly.
  - **CellStub.notify()** is fire-and-forget (no reply expected).
  - **Deferred reply (`Reply`):** A `messageArrived` method can return `dmg.cells.nucleus.Reply` to defer sending the reply asynchronously. This is used when the handler needs to do async work before replying.
  - When writing tests that mock `CellStub.send()`, use `com.google.common.util.concurrent.SettableFuture` (Guava) for creating test futures.
  - Use `com.google.common.util.concurrent.Futures` utility class for transforming and composing futures.
  - Common pattern: `Futures.addCallback(future, callback, MoreExecutors.directExecutor())`
- **Message Vehicle Methods:**
  - All inter-cell messages extend `diskCacheV111.vehicles.Message`.
  - Check actual method signatures in message classes — names vary and do NOT follow a uniform convention.
  - Always use grep or file inspection to verify method names before writing code that calls them.
- **Admin Commands:**
  - Cells expose admin shell commands via `@Command`-annotated inner classes implementing `Callable`.
  - Classes that provide commands implement `CellCommandListener`.
- **Logging:**
  - Use **SLF4J** (`org.slf4j.Logger`, `org.slf4j.LoggerFactory`).
  - Do not use `System.out` or `System.err`.
- **Error Handling:**
  - Propagate exceptions appropriately.
  - Use `CacheException` (`diskCacheV111.util.CacheException`) and its subclasses for dCache-specific errors.

## 🔗 Integration & Dependencies

- **Database (Chimera):** Chimera uses **Spring JDBC** (`JdbcTemplate` / `NamedParameterJdbcTemplate`) — NOT DataNucleus/JDO. The main class is `org.dcache.chimera.JdbcFs` with SQL drivers like `FsSqlDriver`.
- **REST API:** The frontend uses **JAX-RS (Jersey)**, not Spring MVC. Resources live in `org.dcache.restful.resources`. The app is `DcacheRestApplication`.
- **Coordination:** Apache Curator / ZooKeeper is used for distributed coordination.
- **Configuration:** Spring 5 XML beans; `@Required` for mandatory setter injection; `@PostConstruct`/`@PreDestroy` for lifecycle.

## 💡 Tips for AI Agents

- **Context is King:** When modifying a cell, check its `CellStub` usage to understand who it talks to.
- **Thread Safety:** Always verify thread safety when modifying shared state. Look for existing locks and `@GuardedBy`.
- **Diffs:** When generating diffs, include ample context (5+ lines) as the codebase is large and complex.
- **Verify Before Writing:**
  - **Method Signatures:** Use grep or file inspection to verify method names and signatures before writing code.
  - **Return Types:** Check actual return types of framework methods (e.g., `CellStub.send` has both `ListenableFuture` and `CompletableFuture` overloads).
  - **Message Classes:** Look at the actual message class source to confirm method names (they do not follow a uniform convention).
- **After Writing Code:**
  - **Always compile:** After creating or editing files, run `mvn test-compile -pl <module>` to catch errors early.
  - **Fix compilation errors immediately** before moving to other files.
  - **Run tests:** After compilation succeeds, run the relevant test class.
  - **Check test failures carefully:** Test failures often reveal incorrect assumptions about APIs.

## 🔧 File Editing Best Practices

- **Include sufficient context:** When replacing a string, use 3-5 lines BEFORE and AFTER the target text to ensure uniqueness.
- **Match whitespace exactly:** Indentation and line breaks must match precisely.
- **Verify after editing:** Always read the file or compile after editing to confirm changes applied correctly.
- **If edits fail repeatedly:**
  1. Read the file section to see current state.
  2. Check for whitespace/formatting differences.
  3. For multiple changes in the same area, make one comprehensive edit instead of many small ones.
- **Multiple sequential edits to the same section** can cause state confusion — make comprehensive changes in one edit.

## 🧪 Testing Patterns & Pitfalls

### Mockito Verification with Multiple Calls

**CRITICAL:** When testing code that makes multiple `send()` calls, be careful with `ArgumentCaptor`:

```java
// ❌ WRONG - Captures ALL send() calls, causing TooManyActualInvocations
ArgumentCaptor<SomeMessage> captor = ArgumentCaptor.forClass(SomeMessage.class);
verify(cellStub).send(captor.capture());
```

**Problem:** If the code calls `send()` twice (e.g., once in `refresh()`, once in a callback), 
Mockito will capture BOTH calls, but `verify()` without a count expects exactly one.

**Solutions:**

1. **Test behavior, not implementation:**
   ```java
   // ✅ BETTER - Test the final result instead of capturing messages
   // Complete the mocked response directly
   poolInfoFuture.set(expectedResponse);
   Thread.sleep(100);
   
   // Verify the final state
   assertEquals(5, poolList.getPools().size());
   assertTrue(poolList.getPools().contains(expectedPool));
   ```

2. **Verify specific message type with count:**
   ```java
   // ✅ OK - Verify the specific message type was sent once
   verify(cellStub, times(1)).send(any(SpecificMessageType.class));
   ```

3. **Capture all and filter:**
   ```java
   // ✅ OK but complex - Capture all, filter for the type you want
   ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
   verify(cellStub, atLeastOnce()).send(captor.capture());
   
   SpecificMessage msg = captor.getAllValues().stream()
       .filter(m -> m instanceof SpecificMessage)
       .map(m -> (SpecificMessage) m)
       .findFirst()
       .orElseThrow();
   ```

**General Rule:** Prefer testing end-to-end behavior over verifying internal message passing details.

### Test Compilation and Execution

- **Compile tests first:** `mvn test-compile -pl modules/dcache` catches compilation errors faster than a full build.
- **Run specific test class:** `mvn test -Dtest=ClassName -pl modules/dcache`
- **Test isolation:** Tests may interfere with each other; run the full test class rather than individual methods when debugging.

## 🔄 Workflow for Complex Changes

1. **Plan first:** Understand all files that need changes before editing.
2. **Compile after each file:** `mvn test-compile -pl <module>` — fix errors before moving on.
3. **Run tests only after all files compile.**
4. **Fix test failures immediately** before moving forward.
5. **If stuck in a loop of failed edits:** read the current file state, simplify the change, or ask the developer.

## Communication Guidelines

- Interact as a professional colleague: point out mistakes or misunderstandings constructively.
- Avoid sycophancy. If the developer's request contains an error or misunderstanding, explain it clearly.
- If you lack sufficient information, say so — never fabricate answers or hide gaps in knowledge.
- State assumptions explicitly; ask for clarification when a request is ambiguous.
- It is better to acknowledge limitations than to provide incorrect information.
