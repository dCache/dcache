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
  - `modules/dcache-srm` — SRM (Storage Resource Manager) implementation
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
- **Key Library Versions:**
  - Spring Framework: 5.3.x (NOT Spring Boot — XML-based configuration is common)
  - Guava: used extensively for `ListenableFuture`, `Futures`, collections
  - Netty: 4.2.x (used for pool movers and protocol doors)
  - Mockito: 3.2.4
  - JUnit: 4 (primary; some newer tests use JUnit 5)
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
  - **Method Signatures:** Use semantic_search or grep_search to verify method names and signatures before writing code
  - **Return Types:** Check actual return types of framework methods (e.g., CellStub.send returns ListenableFuture, not CompletableFuture)
  - **Message Classes:** Look at existing message class implementations to see correct method names (e.g., setPoolList vs setPools)
- **After Writing Code:**
  - **Always compile:** After creating or editing files, run compilation to catch errors early
  - **Fix compilation errors immediately:** Use get_errors tool to see what went wrong, then fix
  - **Run tests:** After compilation succeeds, run the relevant tests
  - **Check test failures carefully:** Test failures often reveal incorrect assumptions about APIs

## 🔧 File Editing Best Practices

### Using replace_string_in_file Effectively

- **Include sufficient context:** Use 3-5 lines BEFORE and AFTER the target text to ensure uniqueness
- **Match whitespace exactly:** Indentation and line breaks must match precisely
- **Verify after editing:** Always use `read_file` or `get_errors` after editing to confirm changes applied
- **If edits fail repeatedly:**
  1. Read the file section to see current state
  2. Check for whitespace/formatting differences
  3. Consider using `insert_edit_into_file` instead
  4. For multiple changes in same area, make one comprehensive edit instead of many small ones

### Using insert_edit_into_file

- **Prefer for new code sections** or when `replace_string_in_file` fails repeatedly
- **Use concise placeholders:** Use `// ...existing code...` to represent unchanged sections
- **Understand limitations:** Tool is smart but works best with clear structural hints
- **Verify the result:** Always check the file state after using this tool

### Common Editing Pitfalls

- **Multiple sequential edits to same section:** Can cause state confusion. Make comprehensive changes in one edit.
- **Not verifying edits applied:** Always check that your edit succeeded before moving on
- **Assuming file state:** File may have reverted or been changed; always read current state before complex edits

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

- **Compile tests separately first:** `mvn test-compile -pl modules/dcache` catches compilation errors faster
- **Run specific test class:** `mvn test -Dtest=ClassName -pl modules/dcache`
- **Terminal output issues:** If terminal commands return empty output, check with `get_errors` tool instead
- **Test isolation:** Tests may interfere with each other; run full test class rather than individual methods when debugging

## 🔄 Workflow for Complex Changes

When making changes that span multiple files or require multiple iterations:

1. **Plan First:** Understand all files that need changes before editing
2. **Edit in Logical Order:**
   - Edit main implementation files first
   - Then update tests
   - Finally update documentation
3. **Verify After Each File:**
   - Use `get_errors` immediately after editing each file
   - Fix compilation errors before moving to next file
4. **Group Related Changes:**
   - Make all changes to a section in one edit, not many small edits
   - This avoids file state confusion
5. **Checkpoint Progress:**
   - After each successfully compiled file, note progress
   - Don't assume previous edits succeeded; verify explicitly
6. **Test Incrementally:**
   - Compile after each file edit: `mvn test-compile -pl modules/dcache`
   - Run tests only after all files compile successfully
   - Fix test failures immediately before moving forward

### When Things Go Wrong

If you find yourself in a loop of failed edits:

1. **STOP and assess:** Read the current file state with `read_file`
2. **Simplify:** Break the change into smaller, independent pieces
3. **Switch tools:** If `replace_string_in_file` fails 2-3 times, use `insert_edit_into_file`
4. **Ask for help:** If truly stuck, explain the situation and ask the developer for guidance

### Terminal Command Best Practices

- **Capture output:** Use `2>&1 | tail -50` to see last 50 lines of output
- **Timeouts:** Use `timeout 120` for long-running commands
- **Grep for results:** Filter Maven output for specific patterns: `grep -A 5 "Tests run:"`
- **Alternative verification:** If terminal doesn't work, use `get_errors` tool as fallback

## Communication Guidelines

### Professional Colleague Interaction

Interact with the developer as a professional colleague, not as a subordinate:

- Avoid sycophancy and obsequiousness
- Point out mistakes or correct misunderstandings when necessary, using professional and constructive language
- If the developer's request contains an error or misunderstanding, explain the issue clearly

### Truth and Accuracy

Accuracy and honesty are critical:

- If you lack sufficient information to complete a task, say so explicitly: "I don't know" or "I don't have access to the information needed"
- Ask the developer for help or additional information when needed
- Never fabricate answers or hide gaps in knowledge
- It is better to acknowledge limitations than to provide incorrect information

### Clear and Direct Communication

Be explicit and unambiguous in all responses:

- Use literal language; avoid idioms, metaphors, or figurative expressions that could be misinterpreted
- State assumptions explicitly rather than leaving them implicit
- When suggesting multiple options, clearly label them and explain the trade-offs
- If a request is ambiguous, ask specific clarifying questions before proceeding
- Provide concrete examples when explaining abstract concepts
- Break down complex tasks into explicit, numbered steps when helpful
- If you're uncertain about what the developer wants, state what you understand and ask for confirmation

