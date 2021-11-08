/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.qos.services.scanner.admin;

import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.dcache.qos.services.scanner.data.PoolFilter;
import org.dcache.qos.services.scanner.data.PoolOperationMap;
import org.dcache.qos.services.scanner.namespace.NamespaceAccess;
import org.dcache.qos.services.scanner.util.QoSScannerCounters;
import org.dcache.qos.util.ExceptionMessage;
import org.dcache.qos.util.InitializerAwareCommand;
import org.dcache.qos.util.MapInitializer;
import org.dcache.qos.util.MessageGuard;

public final class QoSScannerAdmin implements CellCommandListener {

    static final String CONTAINED_IN = "contained-in-";

    abstract class PoolOpActivateCommand extends InitializerAwareCommand {

        @Option(name = "status",
              valueSpec = "DOWN|READ_ONLY|ENABLED|UNINITIALIZED",
              usage = "Apply only on operations matching this pool status.")
        String status;

        @Argument(required = false,
              usage = "Apply only to pools matching this regular expression.")
        String pools;

        private final boolean activate;

        PoolOpActivateCommand(MapInitializer initialzer, boolean activate) {
            super(initializer);
            this.activate = activate;
        }

        @Override
        protected String doCall() {
            PoolFilter filter = new PoolFilter();
            filter.setPools(pools);
            filter.setPoolStatus(status);
            return String.format("Issued command to %s pool operations.",
                  poolOperationMap.setIncluded(filter, activate));
        }
    }

    class FutureWrapper {

        final Date timestamp;
        final UUID key;
        final String type;
        final String fileName;
        final Future<?> future;

        FutureWrapper(String type, String fileName, Future<?> future) {
            timestamp = new Date(System.currentTimeMillis());
            key = UUID.randomUUID();
            this.type = type;
            this.fileName = fileName;
            this.future = future;
        }

        boolean isRunning() {
            return !future.isCancelled() && !future.isDone();
        }

        public String toString() {
            String state = future.isCancelled() ? "CANCELLED" :
                  future.isDone() ? "DONE" : "RUNNING";
            return String.format("%-36s %32s %20s, file name: %20s (%s)",
                  timestamp, key, type, fileName, state);
        }
    }

    @Command(name = "async cmd cancel",
          hint = "cancel running scans/queries",
          description = "Interrupts the execution of scans or queries launched asynchronously "
                + "(see async cmd ls).")
    class AsyncCmdCancelCommand extends InitializerAwareCommand {

        @Argument(usage = "Comma-delimited list of UUIDs for the jobs to cancel, or '*' for all jobs.")
        String ids;

        AsyncCmdCancelCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            StringBuilder builder = new StringBuilder();

            Set<String> toMatch;

            if (ids.equals("*")) {
                toMatch = futureMap.keySet();
            } else {
                toMatch = Arrays.stream(ids.split(","))
                      .collect(Collectors.toSet());
            }

            toMatch.forEach(id -> {
                FutureWrapper wrapper = futureMap.get(id);
                if (wrapper != null) {
                    if (wrapper.isRunning()) {
                        wrapper.future.cancel(true);
                        builder.append("Cancelled job for ")
                              .append(id).append("\n");
                    } else {
                        builder.append("Job for ")
                              .append(id).append(" already finished")
                              .append("\n");
                    }
                } else {
                    builder.append("No job for ")
                          .append(id).append("\n");
                }
            });

            return builder.toString();
        }
    }

    @Command(name = "async cmd cleanup",
          hint = "remove future entries and/or file",
          description = "If the job has completed, removes the map entry for the future and/or file.")
    class AsyncCmdCleanupCommand extends InitializerAwareCommand {

        @Option(name = "entry", usage = "remove entry from map.")
        boolean entry = true;

        @Option(name = "file", usage = "delete file.")
        boolean file = false;

        @Argument(usage = "Comma-delimited list of UUIDs for the jobs to clean up, or '*' "
              + "for all finished jobs.")
        String ids;

        AsyncCmdCleanupCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            StringBuilder builder = new StringBuilder();

            Set<String> toMatch;

            if (ids.equals("*")) {
                // new set to avoid ConcurrentModificationException
                toMatch = new HashSet<>(futureMap.keySet());
            } else {
                toMatch = Arrays.stream(ids.split(",")).collect(Collectors.toSet());
            }

            toMatch.stream().forEach(id -> {
                FutureWrapper wrapper = futureMap.get(id);
                if (wrapper != null) {
                    if (wrapper.isRunning()) {
                        builder.append("job for ").append(id).append(" is still running\n");

                    } else {
                        if (entry) {
                            futureMap.remove(id);
                            builder.append("entry for ").append(id).append(" removed\n");
                        }

                        if (file) {
                            File file = new File(dataDir, wrapper.fileName);
                            if (!file.exists()) {
                                builder.append("file for ").append(id).append(": ")
                                      .append(wrapper.fileName).append(" NOT FOUND\n");
                            } else {
                                file.delete();
                                builder.append("file for ").append(id).append(": ")
                                      .append(wrapper.fileName).append(" deleted\n");
                            }
                        }
                    }
                }
            });

            return builder.toString();
        }
    }

    @Command(name = "async cmd ls",
          hint = "print out a list of running scans/queries",
          description = "When executing asynchronously either the inaccessible file "
                + "scan or the contained-in query, a future is stored along with identifying "
                + "information and path of the file to which the results will be printed; "
                + "this command lists them all.")
    class AsyncCmdListCommand extends InitializerAwareCommand {

        AsyncCmdListCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            return String.join("\n",
                  futureMap.values()
                        .stream()
                        .map(FutureWrapper::toString)
                        .sorted()
                        .collect(Collectors.toList()));
        }
    }

    @Command(name = "async cmd results",
          hint = "print to screen asynchronous query/scan results",
          description =
                "Reads the file to which the results have been written.  Only 10,000 lines max "
                      + "can be read at one time; if the count exceeds 10,000, use the 'from' option "
                      + "to read the following 10,000.")
    class AsyncCmdResultsCommand extends InitializerAwareCommand {

        AsyncCmdResultsCommand() {
            super(initializer);
        }

        @Option(name = "count",
              usage = "Print only the number of lines in the file.")
        Boolean count = false;

        @Option(name = "from",
              usage = "Starting line index.")
        Integer from = 0;

        @Argument(usage = "Name of file to read and print.")
        String fileName;

        @Override
        protected String doCall() throws Exception {
            File file = new File(dataDir, fileName);
            if (!file.exists()) {
                return fileName + " NOT FOUND.";
            }

            int i = 0;

            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                if (count) {
                    while (reader.readLine() != null) {
                        ++i;
                    }
                    return "" + i + "\n";
                } else {
                    for (; i < from; ++i) {
                        // skip the first 'from' lines
                        if (null == reader.readLine()) {
                            return "No more lines after " + i;
                        }
                    }

                    String line = reader.readLine();
                    if (line == null) {
                        if (from == 0) {
                            return "";
                        }
                        return "No more lines after " + i;
                    }

                    StringBuilder builder = new StringBuilder();
                    builder.append(line).append("\n");
                    ++i;

                    int end = from + 10000;
                    for (; i < end; ++i) {
                        line = reader.readLine();
                        if (line == null) {
                            break;
                        }

                        builder.append(line).append("\n");
                    }

                    return builder.toString();
                }
            } catch (IOException e) {
                return "Error reading file " + fileName + ": " + e.getMessage();
            }
        }
    }

    @Command(name = "contained in",
          hint = "count or list pnfsids which have replicas only on the pools in the list",
          description = "Issues a query to the namespace to determine which files on the "
                + "indicated pools have all their replicas only on those pools. "
                + "Results are written to a file in resilience home named '" + CONTAINED_IN
                + "' + timestamp.  Executed asynchronously. Use 'async cmd ls' to see all running jobs, "
                + "'async cmd cancel' to cancel, 'async cmd results' to read the results "
                + "back from the file, and 'async cmd cleanup' to delete the entry and/or file.")
    class ContainedInCommand extends InitializerAwareCommand {

        @Argument(usage = "A regular expression for pools to be included in the group.")
        String poolExpression;

        ContainedInCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            try {
                StringBuilder builder = new StringBuilder();
                Pattern pattern = Pattern.compile(poolExpression);
                List<String> locations
                      = initializer.getAllPools().stream()
                      .filter((pool) -> pattern.matcher(pool).find())
                      .collect(Collectors.toList());
                execAsync(locations, builder);
                return builder.toString();
            } catch (Exception e) {
                return new ExceptionMessage(e).toString();
            }
        }

        private void execAsync(List<String> locations, StringBuilder builder) {
            String fileName = CONTAINED_IN + System.currentTimeMillis();

            Function<PrintWriter, Void> function = printWriter -> {
                try {
                    namespaceAccess.printContainedInFiles(locations, printWriter);
                } catch (CacheException e) {
                    printWriter.println("error during printContainedInFiles: "
                          + e.toString());
                } catch (InterruptedException e) {
                    printWriter.println("printContainedInFiles was interrupted.");
                }
                return null;
            };

            handleAsync(fileName, "contained-in scan", function, builder);
        }
    }

    @Command(name = "pool reset",
          hint = "control the processing of pool state changes",
          description = "resets the properties for the handling of pool state changes.")
    class PoolControlCommand extends InitializerAwareCommand {

        @Option(name = "max-operations",
              usage = "Maximum number of concurrent pool operations permitted.")
        Integer maxOperations;

        @Option(name = "sweep",
              usage = "(one of sweep|down|restart|idle). "
                    + "How often a sweep of operations is made.")
        Integer sweep;

        @Option(name = "down",
              usage = "(one of sweep|down|restart|idle). "
                    + "Minimum grace period between reception of a pool down update and scan of the given "
                    + "pool.")
        Integer down;

        @Option(name = "restart",
              usage = "(one of sweep|down|restart|idle). "
                    + "Minimum grace period between reception of a pool restart update and scan of "
                    + "the given pool.")
        Integer restart;

        @Option(name = "idle",
              usage = "(one of sweep|down|restart|idle). "
                    + "Maximum time a running pool scan can wait before timing out and being preempted.")
        Integer idle;

        @Option(name = "unit",
              valueSpec = "SECONDS|MINUTES|HOURS|DAYS",
              usage = "For the sweep/down/restart/idle options.")
        TimeUnit unit;

        PoolControlCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() {
            if (maxOperations != null) {
                poolOperationMap.setMaxConcurrentRunning(maxOperations);
            }

            if (sweep != null) {
                poolOperationMap.setTimeout(sweep);
                if (unit != null) {
                    poolOperationMap.setTimeoutUnit(unit);
                }
            } else if (down != null) {
                poolOperationMap.setDownGracePeriod(down);
                if (unit != null) {
                    poolOperationMap.setDownGracePeriodUnit(unit);
                }
            } else if (restart != null) {
                poolOperationMap.setRestartGracePeriod(restart);
                if (unit != null) {
                    poolOperationMap.setRestartGracePeriodUnit(unit);
                }
            } else if (idle != null) {
                poolOperationMap.setMaxRunningIdleTime(idle);
                if (unit != null) {
                    poolOperationMap.setMaxRunningIdleTimeUnit(unit);
                }
            }

            poolOperationMap.reset();
            return poolOperationMap.configSettings();
        }
    }

    @Command(name = "pool cancel",
          hint = "cancel pool operations",
          description =
                "Scans the pool table and cancels operations matching the filter parameters; "
                      + "notifies the verifier.")
    class PoolOpCancelCommand extends InitializerAwareCommand {

        @Option(name = "status",
              valueSpec = "DOWN|READ_ONLY|ENABLED",
              usage = "Cancel only operations on pools matching this pool status.")
        String status;

        @Option(name = "state",
              valueSpec = "WAITING|RUNNING",
              separator = ",",
              usage = "Cancel only operations on pools matching this comma-delimited set of states.")
        String[] state;

        @Option(name = "lastUpdateBefore",
              valueSpec = FORMAT_STRING,
              usage = "Cancel only operations on pools whose last update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
              valueSpec = FORMAT_STRING,
              usage = "Cancel only operations on pools whose last update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
              valueSpec = FORMAT_STRING,
              usage = "Cancel only operations on pools whose scan update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
              valueSpec = FORMAT_STRING,
              usage = "Cancel only operations on pools whose scan update was after this date-time.")
        String lastScanAfter;

        @Argument(required = false,
              valueSpec = "regular expression",
              usage = "Cancel only operations on pools matching this expression.")
        String pools;

        PoolOpCancelCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            if (state != null && state.length == 0) {
                return "Please provide a non-empty string value for state.";
            }

            PoolFilter filter = new PoolFilter();
            filter.setPools(pools);
            filter.setLastScanAfter(getTimestamp(lastScanAfter));
            filter.setLastScanBefore(getTimestamp(lastScanBefore));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setPoolStatus(status);

            if (filter.isUndefined() && state == null) {
                return "Please set at least one option or argument other than 'includeChildren'.";
            }

            if (state == null) {
                state = new String[]{"WAITING", "RUNNING"};
            }

            filter.setState(ImmutableSet.copyOf(state));

            StringBuilder sb = new StringBuilder();

            sb.append("Issued cancel command to ")
                  .append(poolOperationMap.cancel(filter))
                  .append(" pool operations.");

            return sb.toString();
        }
    }

    @Command(name = "pool exclude",
          hint = "exclude pool operations",
          description = "Scans the pool table and excludes operations for the matching pools; "
                + "exclusion will cancel any running operations.  The pool will not be included "
                + "in periodic, forced, or status-change scans; locations on it are still considered "
                + "valid regarding replica count, but cannot be used as copy sources.")
    class PoolOpExcludeCommand extends PoolOpActivateCommand {

        PoolOpExcludeCommand() {
            super(initializer, false);
        }
    }

    @Command(name = "pool include",
          hint = "include pool operations",
          description = "Scans the pool table and includes operations for the matching pools; "
                + "include will only affect pool operations that are currently excluded.")
    class PoolOpIncludeCommand extends PoolOpActivateCommand {

        PoolOpIncludeCommand() {
            super(initializer, true);
        }
    }

    @Command(name = "pool ls", hint = "list entries in the pool operation table",
          description = "Scans the table and returns operations matching the filter parameters.")
    class PoolOpLsCommand extends InitializerAwareCommand {

        @Option(name = "status",
              valueSpec = "DOWN|READ_ONLY|ENABLED|UNINITIALIZED",
              usage = "List only operations on pools matching this pool status.")
        String status;

        @Option(name = "state",
              valueSpec = "IDLE|WAITING|RUNNING|FAILED|CANCELED|EXCLUDED|INACTIVE",
              separator = ",",
              usage = "List only operations on pools matching this comma-delimited set of states "
                    + "(default is everything).")
        String[] state = {"IDLE", "WAITING", "RUNNING", "FAILED", "CANCELED", "EXCLUDED",
              "INACTIVE"};

        @Option(name = "lastUpdateBefore",
              valueSpec = FORMAT_STRING,
              usage = "List only operations on pools whose last update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
              valueSpec = FORMAT_STRING,
              usage = "List only operations on pools whose last update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
              valueSpec = FORMAT_STRING,
              usage = "List only operations on pools whose scan update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
              valueSpec = FORMAT_STRING,
              usage = "List only operations on pools whose scan update was after this date-time.")
        String lastScanAfter;

        @Argument(required = false,
              usage = "List only operations on pools matching this regular expression.")
        String pools;

        PoolOpLsCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            PoolFilter filter = new PoolFilter();
            filter.setPools(pools);
            filter.setLastScanAfter(getTimestamp(lastScanAfter));
            filter.setLastScanBefore(getTimestamp(lastScanBefore));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setPoolStatus(status);
            filter.setState(ImmutableSet.copyOf(state));

            return poolOperationMap.list(filter);
        }
    }

    @Command(name = "pool details",
          hint = "list diagnostic information concerning scan activity by pool",
          description = "Gives statistics on the number of scans, state, files and period.")
    class PoolDetailsCommand extends InitializerAwareCommand {

        PoolDetailsCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            StringBuilder builder = new StringBuilder();
            counters.appendDetails(builder);
            return builder.toString();
        }
    }

    @Command(name = "pool scan",
          hint = "launch a scan of one or more pools",
          description =
                "A check will be initiated to see that the number of replicas on the pool is "
                      + "properly constrained, creating new copies or removing redundant ones "
                      + "as necessary. Note: will not override a currently running operation; matching "
                      + "operations in the waiting state will be guaranteed to run at the next  "
                      + "available slot opening.")
    class PoolScanCommand extends InitializerAwareCommand {

        @Option(name = "status",
              valueSpec = "DOWN|READ_ONLY|ENABLED",
              usage = "Scan only pools matching this pool status.")
        String status;

        @Option(name = "lastUpdateBefore",
              valueSpec = FORMAT_STRING,
              usage = "Scan only pools whose last update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
              valueSpec = FORMAT_STRING,
              usage = "Scan only pools whose last update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
              valueSpec = FORMAT_STRING,
              usage = "Scan only pools whose scan update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
              valueSpec = FORMAT_STRING,
              usage = "Scan only pools whose scan update was after this date-time.")
        String lastScanAfter;

        @Argument(usage = "Regular expression for pool(s) on which to conduct the adjustment "
              + "of all files.")
        String pools;

        PoolScanCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            PoolFilter filter = new PoolFilter();
            filter.setPools(pools);
            filter.setLastScanAfter(getTimestamp(lastScanAfter));
            filter.setLastScanBefore(getTimestamp(lastScanBefore));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setPoolStatus(status);
            return poolOperationMap.scan(filter).getReply();
        }
    }

    @Command(name = "pool stats", hint = "print diagnostic statistics",
          description = "Reads in the contents of the file recording periodic statistics.")
    class PoolStatsCommand extends InitializerAwareCommand {

        @Option(name = "limit", usage = "Display up to this number of lines (default is 24 * 60 = minutes/day).")
        Integer limit = (int) TimeUnit.DAYS.toMinutes(1);

        @Option(name = "order", valueSpec = "asc|desc",
              usage = "Display lines in ascending (default) or descending order by timestamp.")
        String order = "asc";

        @Option(name = "enable",
              usage = "Turn the recording of statistics to file on or off. Recording to file is "
                    + "off by default.")
        Boolean enable = null;

        PoolStatsCommand() {
            super(initializer);
        }

        protected String doCall() throws Exception {
            if (enable != null) {
                counters.setToFile(enable);
                return "Recording to file is now " + (enable ? "on." : "off.");
            }

            SortOrder order = SortOrder.valueOf(this.order.toUpperCase());
            StringBuilder builder = new StringBuilder();
            counters.readStatistics(builder, 0, limit, order == SortOrder.DESC);
            return builder.toString();
        }
    }

    /**
     * Used for potentially long database queries.  Executing them synchronously can bind the
     * command interpreter.  We store the future in case of cancellation or listing.
     */
    private final Map<String, FutureWrapper> futureMap = Collections.synchronizedMap(
          new HashMap<>());

    private MessageGuard messageGuard;
    private MapInitializer initializer;
    private PoolOperationMap poolOperationMap;
    private QoSScannerCounters counters;
    private NamespaceAccess namespaceAccess;
    private String dataDir;
    private ExecutorService executor;

    public void setCounters(QoSScannerCounters counters) {
        this.counters = counters;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setInitializer(MapInitializer initializer) {
        this.initializer = initializer;
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setNamespaceAccess(NamespaceAccess namespaceAccess) {
        this.namespaceAccess = namespaceAccess;
    }

    public void setPoolOperationMap(PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    private void handleAsync(String fileName,
          String type,
          Function<PrintWriter, Void> function,
          StringBuilder builder) {
        File file = new File(dataDir, fileName);

        Future<?> future = executor.submit(() -> {
            try (PrintWriter pw = new PrintWriter(
                  new FileWriter(file, false))) {
                try {
                    function.apply(pw);
                } catch (RuntimeException e) {
                    // write out the error to the file
                    e.printStackTrace(pw);
                }
            } catch (IOException e) {
                // let it go ...
            }
        });

        FutureWrapper wrapper = new FutureWrapper(type,
              fileName,
              future);

        futureMap.put(wrapper.key.toString(), wrapper);
        builder.append(wrapper).append("\n");
    }

    private void startAll() {
        clearAllFutures();
        initializer.initialize();
        if (poolOperationMap.isRunning()) {
            poolOperationMap.shutdown();
        }
        poolOperationMap.loadPools();
        poolOperationMap.initialize();
        messageGuard.enable();
    }

    private void shutdownAll() {
        if (poolOperationMap.isRunning()) {
            poolOperationMap.shutdown();
        }
        clearAllFutures();
        messageGuard.disable(true);
        initializer.shutDown();
    }

    private void clearAllFutures() {
        synchronized (futureMap) {
            for (Iterator<Entry<String, FutureWrapper>> it
                  = futureMap.entrySet().iterator(); it.hasNext(); ) {
                Entry<String, FutureWrapper> entry = it.next();
                entry.getValue().future.cancel(true);
                it.remove();
            }
        }
    }
}
