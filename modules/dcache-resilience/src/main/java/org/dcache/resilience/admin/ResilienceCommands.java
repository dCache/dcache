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
package org.dcache.resilience.admin;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.resilience.data.FileFilter;
import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolFilter;
import org.dcache.resilience.data.PoolInfoFilter;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.db.NamespaceAccess;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.MessageGuard;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Collects all admin shell commands for convenience.  See further individual
 *      command annotations for details.</p>
 */

public final class ResilienceCommands implements CellCommandListener {
    static final String INACCESSIBLE_PREFIX = "inaccessible_files-";

    private static final String FORMAT_STRING = "yyyy/MM/dd-HH:mm:ss";

    private static final String REQUIRE_LIMIT =
                    "The current table contains %s entries; listing them all "
                                    + "could cause an out-of-memory error and "
                                    + "cause the resilience system to fail and/or "
                                    + "restarts; if you wish to proceed "
                                    + "with this listing, reissue the command "
                                    + "with the explicit option '-limit=%s'";

    private static final long LS_THRESHOLD = 500000L;

    enum ControlMode {
        ON,
        OFF,
        START,
        SHUTDOWN,
        RESET,
        RUN,
        INFO
    }

    enum SortOrder {
        ASC, DESC
    }

    abstract class ResilienceCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            String error = initializer.getInitError();

            if (error != null) {
                return error;
            }

            if (!initializer.isInitialized()) {
                return "Resilience is not yet initialized; "
                                + "use 'show pinboard' to see progress, or "
                                + "'enable' to re-initialize "
                                + "if previously disabled.";
            }

            try {
                return doCall();
            } catch (Exception e) {
                return new ExceptionMessage(e).toString();
            }
        }

        protected abstract String doCall() throws Exception;
    }

    abstract class PoolOpActivateCommand extends ResilienceCommand {
        @Option(name = "status",
                        valueSpec = "DOWN|READ_ONLY|ENABLED|UNINITIALIZED",
                        usage = "Apply only on operations matching this "
                                        + "pool status.")
        String status;

        @Argument(required = false,
                        usage = "Apply only to pools matching "
                                        + "this regular expression.")
        String pools;

        private final boolean activate;

        PoolOpActivateCommand(boolean activate) {
            this.activate = activate;
        }

        @Override
        protected String doCall() throws Exception {
            PoolFilter filter = new PoolFilter();
            filter.setPools(pools);
            filter.setPoolStatus(status);
            return String.format(
                            "Issued command to %s pool operations.",
                            poolOperationMap.setIncluded(filter, activate));
        }
    }

    @Command(name = "diag",
                    hint = "print diagnostic statistics",
                    description = "Lists the total number of messages received  "
                                    + "and operations performed since last start "
                                    + "of the resilience system.  Rate/sec is "
                                    + "sampled over the interval since the last "
                                    + "checkpoint. These values for new "
                                    + "location messages and file "
                                    + "operations are recorded "
                                    + "to a stastistics file located in the "
                                    + "resilience home directory, and which "
                                    + "can be displayed using the history option")
    class DiagCommand extends ResilienceCommand {
        @Argument(required = false,
                  usage = "Include pools matching this regular expression; "
                                  + "default prints only summary info.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            return counters.print(pools);
        }
    }

    @Command(name = "diag history",
                    hint = "print diagnostic history",
                    description = "Reads in the contents of the diagnostic "
                                    + "history file recording periodic statistics "
                                    + "(see diag command).")
    class DiagHistoryCommand extends ResilienceCommand {
        @Option(name = "limit",
                        usage = "Display up to this number of lines "
                                        + "(default is 24 * 60).")
        Integer limit = 24 * 60;

        @Option(name = "order",
                        valueSpec = "asc|desc",
                        usage = "Display lines in ascending or "
                                        + "descending (default) "
                                        + "order by timestamp.")
        String order = "desc";

        @Option(name = "enable",
                        usage = "Turn the recording of statistics "
                                        + "to file on or off. "
                                        + "Recording to file is off by default.")
        Boolean enable = null;

        protected String doCall() throws Exception {
            if (enable != null) {
                counters.setToFile(enable);
                return "Recording to file is now " + (enable ? "on." : "off.");
            }

            SortOrder order = SortOrder.valueOf(this.order.toUpperCase());
            return counters.readStatistics(limit, order == SortOrder.DESC);
        }
    }

    @Command(name = "disable",
                    hint = "turn off replication handling",
                    description = "Prevents messages from being processed by "
                                    + "the replication system (this is useful  "
                                    + "for instance if rebalance is run on "
                                    + "a resilient group). "
                                    + "To disable all internal operations, use "
                                    + "the 'strict' argument to this command; "
                                    + "this option will also cancel all pool and "
                                    + "file operations.")
    class DisableCommand extends ResilienceCommand {
        @Option(name="drop",
                        valueSpec = "true|false",
                        usage = "If true, do not store backlogged messages "
                                        + "(only valid without the 'strict' "
                                        + "argument); false by default.")
        Boolean drop = false;

        @Argument(required = false,
                        valueSpec = "strict",
                        usage = "Whether to shutdown all operations "
                                        + "(without this argument, only "
                                        + "incoming messages are blocked).")
        String strict;

        @Override
        protected String doCall() throws Exception {
            if (strict != null) {
                if (!"strict".equals(strict.toLowerCase())) {
                    return "Unrecognized argument '" + strict + "'.";
                }

                synchronized(futureMap) {
                    for (Iterator<Future<?>> it = futureMap.values().iterator();
                         it.hasNext(); ) {
                        it.next().cancel(true);
                        it.remove();
                    }
                }

                messageGuard.disable(true);
                initializer.shutDown(); // shuts down maps

                return "All resilience operations have been shutdown.";
            }

            if (messageGuard.isEnabled()) {
                messageGuard.disable(drop);
                if (drop) {
                    return "Processing of incoming messages has been disabled; "
                                    + "backlogged messages will be dropped.";
                }
                return "Processing of incoming messages has been disabled; "
                                + "backlogged messages will be stored.";
            }

            return "Resilience already disabled.";
        }
    }

    @Command(name = "enable",
                    hint = "turn on replication handling",
                    description = "Allows messages to be processed by "
                                    + "the replication system. Will also "
                                    + "(re-)enable all internal operations if they "
                                    + "are not running.  Executed asynchronously.")
    class EnableCommand implements Callable<String>  {
        @Override
        public String call() throws Exception {
            if (!messageGuard.isEnabled()) {
                messageGuard.enable();

                if (!initializer.initialize()) {
                    return "Processing of incoming messages has been re-enabled.";
                }

                return "Resilience system has been re-enabled "
                                + "and map reinitialization/reload started.";
            }

            if (initializer.initialize()) {
                return "Resilience system has been re-enabled "
                                + "and map reinitialization/reload started.";
            }

            return "Resilience is already enabled.";
        }
    }

    @Command(name = "file check",
                    hint = "launch an operation to adjust replicas for "
                                    + "one or more pnfsids",
                    description = "For each pnfsid, runs a check to see that "
                                    + "the number of replicas is properly "
                                    + "constrained, creating new copies or "
                                    + "removing redundant ones as necessary.")
    class FileCheckCommand extends ResilienceCommand {
        @Argument(usage = "Comma-delimited list of pnfsids "
                                        + "for which to run the adjustment.")
        String pnfsids;

        @Override
        protected String doCall() throws Exception {
            String[] list = pnfsids.split("[,]");
            for (String pnfsid: list) {
                PnfsId pnfsId = new PnfsId(pnfsid);
                FileAttributes attr = namespaceAccess.getRequiredAttributes(
                                pnfsId);
                Iterator<String> it = attr.getLocations().iterator();
                if (!it.hasNext()) {
                    return pnfsid + " does not seem to have any locations.";
                }
                String pool = it.next();
                FileUpdate update
                                = new FileUpdate(pnfsId, pool,
                                                 MessageType.ADD_CACHE_LOCATION,
                                                 true);
                fileOperationHandler.handleLocationUpdate(update);
            }
            return "An adjustment activity has been started for " + pnfsids + ".";
        }
    }

    @Command(name = "file ctrl",
                    hint = "control checkpointing or "
                                    + "handling of file operations",
                    description = "Runs checkpointing, resets checkpoint "
                                    + "properties, resets operation properties, "
                                    + "turn processing of operations on or off "
                                    + "(start/shutdown), or displays info relevant "
                                    + "to operation processing and checkpointing.")
    class FileControlCommand extends ResilienceCommand {
        @Argument(valueSpec = "ON|OFF|START|SHUTDOWN|RESET|RUN|INFO",
                        required = false,
                        usage = "off = turn checkpointing off; "
                                        + "on = turn checkpointing on; "
                                        + "info = information (default); "
                                        + "reset = reset properties; "
                                        + "start = (re)start processing of file operations; "
                                        + "shutdown = stop all processing of file operations; "
                                        + "run = checkpoint to disk immediately." )
        String arg = "INFO";

        @Option(name = "checkpoint",
                        usage = "With reset mode (one of checkpoint|sweep). "
                                        + "Interval length between checkpointing "
                                        + "of the file operation data.")
        Long checkpoint;

        @Option(name = "sweep",
                        usage = "With reset mode (one of checkpoint|sweep). "
                                        + "Minimal interval between sweeps of "
                                        + "the file operations.")
        Long sweep;

        @Option(name = "unit",
                        valueSpec = "SECONDS|MINUTES|HOURS",
                        usage = "Checkpoint or sweep interval unit.") TimeUnit unit;

        @Option(name = "retries",
                        usage = "Maximum number of retries on a failed operation.")
        Integer retries;

        @Option(name = "file",
                        usage = "Alternate (full) path for checkpoint file.")
        String file;

        @Override
        protected String doCall() throws Exception {
            ControlMode mode = ControlMode.valueOf(arg.toUpperCase());

            switch (mode) {
                case START:
                    if (fileOperationMap.isRunning()) {
                        return "Consumer is already running.";
                    }
                    new Thread(() -> {
                        fileOperationMap.initialize();
                        fileOperationMap.reload();
                    }).start();
                    return "Consumer initialization "
                                    + "and reload of checkpoint file started.";
                case SHUTDOWN:
                    if (!fileOperationMap.isRunning()) {
                        return "Consumer is not running.";
                    }
                    fileOperationMap.shutdown();
                    return "Consumer has been shutdown.";
                case OFF:
                    if (fileOperationMap.isCheckpointingOn()) {
                        fileOperationMap.stopCheckpointer();
                        return "Shut down checkpointing.";
                    }
                    return "Checkpointing already off.";
                case ON:
                    if (!fileOperationMap.isCheckpointingOn()) {
                        fileOperationMap.startCheckpointer();
                        return infoMessage();
                    }
                    return "Checkpointing already on.";
                case RUN:
                    if (!fileOperationMap.isCheckpointingOn()) {
                        return "Checkpointing is off; please turn it on first.";
                    }
                    fileOperationMap.runCheckpointNow();
                    return "Forced checkpoint.";
                case RESET:
                    if (!fileOperationMap.isCheckpointingOn()) {
                        return "Checkpointing is off; please turn it on first.";
                    }

                    if (checkpoint != null) {
                        fileOperationMap.setCheckpointExpiry(checkpoint);
                        if (unit != null) {
                            fileOperationMap.setCheckpointExpiryUnit(unit);
                        }
                    } else if (sweep != null) {
                        fileOperationMap.setTimeout(sweep);
                        if (unit != null) {
                            fileOperationMap.setTimeoutUnit(unit);
                        }
                    }

                    if (retries != null) {
                        fileOperationMap.setMaxRetries(retries);
                    }

                    if (file != null) {
                        fileOperationMap.setCheckpointFilePath(file);
                    }

                    fileOperationMap.reset();
                    // fall through here
                case INFO:
                default:
                    return infoMessage();
            }
        }

        private String infoMessage() {
            StringBuilder info = new StringBuilder();
            info.append(String.format("maximum concurrent operations %s.\n"
                                            + "maximum retries on failure %s\n",
                                      fileOperationMap.getMaxRunning(),
                                      fileOperationMap.getMaxRetries()));
            info.append(String.format("sweep interval %s %s\n",
                                      fileOperationMap.getTimeout(),
                                      fileOperationMap.getTimeoutUnit()));
            info.append(String.format("checkpoint interval %s %s\n"
                                            + "checkpoint file path %s\n",
                                      fileOperationMap.getCheckpointExpiry(),
                                      fileOperationMap.getCheckpointExpiryUnit(),
                                      fileOperationMap.getCheckpointFilePath()));
            counters.getFileOpSweepInfo(info);
            counters.getCheckpointInfo(info);
            return info.toString();
        }
    }

    @Command(name = "file cancel",
                    hint = "cancel file operations",
                    description = "Scans the file table and cancels "
                                    + "operations matching the filter "
                                    + "parameters.")
    class FileOpCancelCommand extends ResilienceCommand {
        @Option(name = "state",
                        valueSpec = "WAITING|RUNNING",
                        separator = ",",
                        usage = "Cancel operations for files matching this "
                                        + "comma-delimited set of operation states; "
                                        + "default is both.")
        String[] state;

        @Option(name = "forceRemoval",
                        usage = "Remove all waiting operations for this match "
                                        + "after cancellation of the running tasks. "
                                        + "(Default is false; this option is "
                                        + "redundant if the state includes WAITING.)")
        boolean forceRemoval = false;

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "retentionPolicy",
                        valueSpec = "REPLICA|CUSTODIAL ",
                        usage = "Cancel only operations for files with this "
                                        + "policy.")
        String retentionPolicy;

        @Option(name = "storageUnit",
                        usage = "Cancel only operations for files with this "
                                        + "storage unit/group.")
        String storageUnit;

        @Option(name = "opCount",
                        usage = "Cancel only operations with this operation count.")
        Integer opCount;

        @Option(name = "parent",
                        usage = "Cancel only operations with this parent pool name; "
                                        + "use the option with no value to match only "
                                        + "operations without a parent pool.")
        String parent;

        @Option(name = "source",
                        usage = "Cancel only operations with this source pool name; "
                                        + "use the option with no value to match only "
                                        + "operations without a source pool.")
        String source;

        @Option(name = "target",
                        usage = "Cancel only operations with this target pool name; "
                                        + "use the option with no value to match only "
                                        + "operations without a target pool.")
        String target;

        @Argument(required = false,
                        usage = "Cancel operations for this comma-delimited "
                                        + "list of pnfsids "
                                        + "(use '*' to cancel all operations).")
        String pnfsids;

        @Override
        protected String doCall() throws Exception {
            if (state != null && state.length == 0) {
                return "Please provide a non-empty string value for state.";
            }

            FileFilter filter = new FileFilter();

            if (!"*".equals(pnfsids)) {
                filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
                filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
                filter.setPnfsIds(pnfsids);
                filter.setRetentionPolicy(retentionPolicy);
                filter.setStorageUnit(storageUnit);
                filter.setParent(parent);
                filter.setSource(source);
                filter.setTarget(target);
                filter.setOpCount(opCount);

                /*
                 * Guard against accidental cancellation of everything.
                 */
                if (filter.isUndefined() && state == null) {
                    return "Please set at least one option or "
                                    + "argument other than 'forceRemoval'.";
                }

                if (filter.isSimplePnfsMatch()) {
                    fileOperationMap.cancel(new PnfsId(pnfsids), forceRemoval);
                    return String.format("Issued cancel command for %s.", pnfsids);
                }
            }

            if (state == null) {
                state = new String[]{"WAITING", "RUNNING"};
            }

            Set<String> stateSet = ImmutableSet.copyOf(state);
            forceRemoval |= stateSet.contains("WAITING");
            filter.setForceRemoval(forceRemoval);
            filter.setState(stateSet);

            fileOperationMap.cancel(filter);

            return "Issued cancel command to cancel file operations.";
        }
    }

    @Command(name = "history",
                    hint = "display a history of the most recent terminated "
                                    + "file operations",
                    description = "When file operations complete or are aborted, "
                                    + "their string representations are added "
                                    + "to a circular buffer whose capacity is set "
                                    + "by the property "
                                    + "'resilience.limits.file.operation-history'.")
    class FileOpHistoryCommand extends ResilienceCommand {
        @Argument(required = false,
                        valueSpec = "errors",
                        usage = "Display just the failures.")
        String errors;

        @Option(name = "limit",
                        usage = "Display up to this number of entries.")
        Integer limit;

        @Option(name = "order",
                        valueSpec = "ASC|DESC",
                        usage = "Display entries in ascending or descending "
                                        + "(default) order of arrival.")
        String order = "DESC";

        @Override
        protected String doCall() throws Exception {
            boolean failed = false;
            if (errors != null) {
                if (!"errors".equals(errors)) {
                    return  "Optional argument must be 'errors'";
                }
                failed = true;
            }

            SortOrder order = SortOrder.valueOf(this.order.toUpperCase());

            switch (order) {
                case ASC:
                    if (limit != null) {
                        return history.ascending(failed, limit);
                    }
                    return history.ascending(failed);
                case DESC:
                default:
                    if (limit != null) {
                        return history.descending(failed, limit);
                    }
                    return history.descending(failed);
            }
        }
    }

    @Command(name = "file ls",
                    hint = "list entries in the file operation table",
                    description = "Scans the table and returns operations  "
                                    + "matching the filter parameters.")
    class FileOpLsCommand extends ResilienceCommand {
        @Option(name = "retentionPolicy",
                        valueSpec = "REPLICA|CUSTODIAL",
                        usage = "List only operations for files with this "
                                        + "policy.")
        String retentionPolicy;

        @Option(name = "storageUnit",
                        usage = "List only operations for files with this "
                                        + "storage unit/group.")
        String storageUnit;

        @Option(name = "state",
                        valueSpec = "WAITING|RUNNING",
                        separator = ",",
                        usage = "List only operations for files matching this "
                                        + "comma-delimited set of operation states; "
                                        + "default is both.")
        String[] state = {"WAITING", "RUNNING"};

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "opCount",
                        usage = "List only operations with this operation count.")
        Integer opCount;

        @Option(name = "parent",
                        usage = "List only operations with this parent pool name; "
                                        + "use the option with no value to match only "
                                        + "operations without a parent pool.")
        String parent;

        @Option(name = "source",
                        usage = "List only operations with this source pool name; "
                                        + "use the option with no value to match only "
                                        + "operations without a source pool.")
        String source;

        @Option(name = "target",
                        usage = "List only operations with this target pool name; "
                                        + "use the option with no value to match only "
                                        + "operations without a target pool.")
        String target;

        @Option(name = "limit",
                        usage = "Maximum number of rows to list.  This "
                                        + "option becomes required when "
                                        + "the operation queues reach "
                                        + LS_THRESHOLD + "; be aware that "
                                        + "listing more than this number of "
                                        + "rows may provoke an out of memory "
                                        + "error for the domain.")
        Integer limit;

        @Argument(required = false,
                        usage = "List only activities for this comma-delimited "
                                        + "list of pnfsids. No argument lists all "
                                        + "operations; use '$' to return just "
                                        + "the number of entries; '$@' to "
                                        + "return the op counts by pool.")
        String pnfsids;

        @Override
        protected String doCall() throws Exception {
            boolean count = "$".equals(pnfsids) || "$@".equals(pnfsids);
            Set<String> stateSet = ImmutableSet.copyOf(state);

            FileFilter filter = new FileFilter();
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setState(stateSet);
            filter.setRetentionPolicy(retentionPolicy);
            filter.setStorageUnit(storageUnit);
            filter.setParent(parent);
            filter.setSource(source);
            filter.setTarget(target);
            filter.setOpCount(opCount);

            if (!count) {
                filter.setPnfsIds(pnfsids);
            } else {
                StringBuilder builder =
                                pnfsids.contains("@") ? new StringBuilder() : null;
                long total = fileOperationMap.count(filter, builder);

                if (builder == null) {
                    return total + " matching pnfsids";
                }

                return String.format("%s matching operations."
                                + "\n\nOperation counts per pool:\n%s",
                                total, builder.toString());
            }

            if (filter.isSimplePnfsMatch()) {
                FileOperation op = fileOperationMap.getOperation(new PnfsId(pnfsids));
                if (op == null) {
                    return String.format("No operation currently registered for %s.",
                                    pnfsids);
                }
                return op.toString() + "\n";
            }

            long size = fileOperationMap.size();
            int limitValue = (int)size;

            if (limit == null) {
                if (stateSet.contains("WAITING") && size >= LS_THRESHOLD) {
                    return String.format(REQUIRE_LIMIT, size, size);
                }
            } else {
                limitValue = limit;
            }

            return fileOperationMap.list(filter, limitValue);
        }
    }

    @Command(name = "inaccessible",
                    hint = "list pnfsids for a pool which "
                                    + "currently have no readable locations",
                    description = "Issues a query to the namespace to scan the pool, "
                                    + "checking locations of each file with online "
                                    + "access latency; results are written to a  "
                                    + "file in resilience home named '"
                                    + INACCESSIBLE_PREFIX
                                    + "' + pool. Executed asynchronously.")
    class InaccessibleFilesCommand extends ResilienceCommand {
        @Option(name = "cancel", usage = "Cancel the running job.")
        boolean cancel = false;

        @Argument(usage = "A regular expression for pool names.")
        String expression;

        @Override
        protected String doCall() throws Exception {
            try {
                StringBuilder builder = new StringBuilder();
                Pattern pattern = Pattern.compile(expression);

                poolInfoMap.getResilientPools()
                           .stream()
                           .filter((pool) -> pattern.matcher(pool).find())
                           .forEach((pool) -> handleOption(cancel, pool, builder));

                builder.insert(0, "Started jobs to write the lists "
                                + "of inaccessible pnfsids "
                                + "to the following files:\n\n");
                builder.append("Check pinboard for progress.\n");

                return builder.toString();
            } catch (Exception e) {
                return new ExceptionMessage(e).toString();
            }
        }

        private void handleOption(boolean cancel,
                                  String pool,
                                  StringBuilder builder) {
            if (cancel) {
                Future<?> future = futureMap.remove(pool);
                if (future != null) {
                    future.cancel(true);
                }

                builder.append("cancelled job for ")
                                .append(pool).append("\n");
            } else {
                File file = printToFile(pool,
                                resilienceDir);
                builder.append("   ")
                                .append(file.getAbsolutePath())
                                .append("\n");
            }
        }

        private File printToFile(String pool, String dir) {
            File file = new File(dir, INACCESSIBLE_PREFIX + pool);
            ListeningExecutorService decoratedExecutor
                            = MoreExecutors.listeningDecorator(executor);

            ListenableFuture<?> future = decoratedExecutor.submit(() -> {
                try {
                    try (PrintWriter pw = new PrintWriter(
                                    new FileWriter(file, false))) {
                        try {
                            namespaceAccess.printInaccessibleFiles(pool,
                                                                   poolInfoMap,
                                                                   pw);
                        } catch (CacheException e) {
                            // write out the error to the file
                            e.printStackTrace(pw);
                        }
                    } catch (IOException e) {
                        // let it go ...
                    }
                } catch (InterruptedException e) {
                    // job was cancelled
                }
            });

            futureMap.put(pool, future);
            future.addListener(() -> futureMap.remove(pool),
                                     MoreExecutors.directExecutor());
            return file;
        }
    }

    @Command(name = "pool ctrl",
                    hint= "control the periodic check of active resilient pools "
                                    + "or processing of pool state changes",
                    description = "Activates, deactivates, or resets the periodic  "
                                    + "checking of active pools; turns all pool  "
                                    + "state handling on or off (start/shutdown).")
    class PoolControlCommand extends ResilienceCommand {
        @Argument(valueSpec = "ON|OFF|START|SHUTDOWN|RESET|RUN|INFO",
                        required = false,
                        usage = "off = turn scanning off; on = turn scanning on; "
                                        + "shutdown = turn off all pool operations; "
                                        + "start = setIncluded all pool operations; "
                                        + "info = show status of watchdog and scan window (default); "
                                        + "reset = reset properties; "
                                        + "run = interrupt current wait and do a sweep." )
        String operation = "INFO";

        @Option(name = "window",
                        usage = "With reset mode (one of window|sweep|down|restart). "
                                        + "Amount of time which must pass since "
                                        + "the last scan of a pool for it to be "
                                        + "scanned again.")
        Integer window;

        @Option(name = "sweep",
                        usage = "With reset mode (one of window|sweep|down|restart). "
                                        + "How often a sweep of the pool "
                                        + "operations is made.")
        Integer sweep;

        @Option(name = "down",
                        usage = "With reset mode (one of window|sweep|down|restart). "
                                        + "Minimum grace period between reception "
                                        + "of a pool down update and scan of  "
                                        + "the given pool.")
        Integer down;

        @Option(name = "restart",
                        usage = "With reset mode (one of window|sweep|down|restart). "
                                        + "Minimum grace period between reception "
                                        + "of a pool restart update and scan of  "
                                        + "the given pool.")
        Integer restart;

        @Option(name = "unit",
                        valueSpec = "SECONDS|MINUTES|HOURS|DAYS",
                        usage = "For the sweep/window/down/restart options.")
        TimeUnit unit;

        @Override
        protected String doCall() throws Exception {
            ControlMode mode = ControlMode.valueOf(operation.toUpperCase());

            switch (mode) {
                case START:
                    if (poolOperationMap.isRunning()) {
                        return "Consumer is already running.";
                    }
                    new Thread(() -> {
                        poolOperationMap.loadPools();
                        poolOperationMap.initialize();
                    }).start();
                    return "Consumer initialization and pool reload started.";
                case SHUTDOWN:
                    if (!poolOperationMap.isRunning()) {
                        return "Consumer is not running.";
                    }
                    poolOperationMap.shutdown();
                    return "Consumer has been shutdown.";
                case OFF:
                    if (poolOperationMap.isWatchdogOn()) {
                        poolOperationMap.setWatchdog(false);
                        return "Shut down watchdog.";
                    }
                    return "Watchdog already off.";
                case ON:
                    if (!poolOperationMap.isWatchdogOn()) {
                        poolOperationMap.setWatchdog(true);
                        return infoMessage();
                    }
                    return "Watchdog already on.";
                case RUN:
                    if (!poolOperationMap.isWatchdogOn()) {
                        return "Watchdog is off; please turn it on first.";
                    }
                    poolOperationMap.runNow();
                    return "Forced watchdog scan.";
                case RESET:
                    if (!poolOperationMap.isWatchdogOn()) {
                        return "Watchdog is off; please turn it on first.";
                    }

                    if (window != null) {
                        poolOperationMap.setRescanWindow(window);
                        if (unit != null) {
                            poolOperationMap.setRescanWindowUnit(unit);
                        }
                    } else if (sweep != null) {
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
                    }

                    poolOperationMap.reset();

                    // fall through to return info message
                case INFO:
                default:
                    return infoMessage();
            }
        }

        private String infoMessage() {
            return String.format("down grace period %s %s\n"
                                            + "restart grace period %s %s\n"
                                            + "maximum concurrent operations %s\n"
                                            + "scan window set to %s %s\n"
                                            + "period set to %s %s\n",
                            poolOperationMap.getDownGracePeriod(),
                            poolOperationMap.getDownGracePeriodUnit(),
                            poolOperationMap.getRestartGracePeriod(),
                            poolOperationMap.getRestartGracePeriodUnit(),
                            poolOperationMap.getMaxConcurrentRunning(),
                            poolOperationMap.getScanWindow(),
                            poolOperationMap.getScanWindowUnit(),
                            poolOperationMap.getTimeout(),
                            poolOperationMap.getTimeoutUnit());
        }
    }

    @Command(name = "pool group info",
                    hint = "list the storage units linked to a pool group "
                                    + "and confirm resilience constraints "
                                    + "can be met by the member pools",
                    description = "Lists name, key and storage units linked to "
                                    + "the pool group. Tries to satisfy the "
                                    + "constraints on replica count and "
                                    + "exclusivity tags for all the storage "
                                    + "units in the pool group by attempting "
                                    + "to assign the required number "
                                    + "of locations for a hypothetical file "
                                    + "belonging to each unit.")
    class PoolGroupInfoCommand extends ResilienceCommand {
        @Option(name = "key",
                        usage = "List pool group info for this group key.")
        Integer key;

        @Option(name = "showUnits",
                        usage = "List storage units linked to this group.")
        boolean showUnits = false;

        @Option(name = "verify",
                        usage = "Run the verification procedure for "
                                        + "the units in the group "
                                        + "(default is false).")
        boolean verify = false;

        @Argument(required = false,
                        usage = "Name of the resilient pool group "
                                        + "(default is false).")
        String name;

        @Override
        protected String doCall() throws Exception {
            if (name == null && key == null) {
                return "Please provide either the name or "
                                + "the key of the pool group.";
            }

            try {
                if (name == null) {
                    name = poolInfoMap.getGroupName(key);
                } else {
                    key = poolInfoMap.getGroupIndex(name);
                }
            } catch (NoSuchElementException | IndexOutOfBoundsException e ) {
                if (name == null) {
                    return String.format("No pool group with key = %s.", key);
                }
                return String.format("No such pool group: %s.", name);
            }

            try {
                poolInfoMap.getStorageUnitConstraints(key);
                return String.format("%s (%s) is not a pool group.",
                                     name, key);
            } catch (NoSuchElementException e) {
                // OK, not a storage unit.
            }

            if (!poolInfoMap.isResilientGroup(key)) {
                return String.format("%s (%s) is not a resilient group.",
                                     name, key);
            }

            StringBuilder builder = new StringBuilder();
            builder.append("Name         : ").append(name).append("\n");
            builder.append("Key          : ").append(key).append("\n");

            if (showUnits) {
                poolInfoMap.getStorageUnitsFor(name).stream()
                           .map(poolInfoMap::getGroupName)
                           .forEach((u) -> builder.append("    ")
                                                  .append(u).append("\n"));
            }

            if (verify) {
                try {
                    poolInfoMap.verifyConstraints(key);
                    builder.append("As configured, member pools can satisfy "
                                                   + "resilience constraints.");
                } catch (NoSuchElementException | IllegalStateException e) {
                    builder.append("As configured, member pools cannot satisfy "
                                                   + "resilience constraints: ")
                           .append(new ExceptionMessage(e));
                }
            }

            return builder.toString();
        }
    }

    @Command(name = "pool info",
                    hint = "list tags and mode for a pool or pools",
                    description = "Lists pool key, name, mode, "
                                        + "status, tags and last update time.")
    class PoolInfoCommand extends ResilienceCommand {
        @Option(name = "status",
                        valueSpec = "DOWN|READ_ONLY|ENABLED|UNINITIALIZED",
                        separator = ",",
                        usage = "List only information for pools matching this "
                                        + "comma-delimited set of pool states.")
        String[] status = {"DOWN", "READ_ONLY", "ENABLED", "UNINITIALIZED"};

        @Option(name = "keys",
                        separator = ",",
                        usage = "List only information for pools matching this "
                                        + "comma-delimited set of pool keys.")
        Integer[] keys;

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Argument(required = false,
                        usage = "Regular expression to match pool "
                                        + "names; no argument matches all pools.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            PoolInfoFilter filter = new PoolInfoFilter();
            filter.setPools(pools);
            filter.setKeys(keys);
            filter.setStatus(status);
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));

            try {
                return poolInfoMap.listPoolInfo(filter);
            } catch (IndexOutOfBoundsException | NoSuchElementException e) {
                return "No such pools: " + pools;
            }
        }
    }

    @Command(name = "pool cancel",
                    hint = "cancel pool operations",
                    description = "Scans the pool table and cancels "
                                    + "operations matching the filter parameters; "
                                    + "if 'includeChildren' is true, also "
                                    + "scans the file table.")
    class PoolOpCancelCommand extends ResilienceCommand {
        @Option(name = "status",
                        valueSpec = "DOWN|READ_ONLY|ENABLED",
                        usage = "Cancel only operations on pools matching this "
                                        + "pool status.")
        String status;

        @Option(name = "state",
                        valueSpec = "WAITING|RUNNING",
                        separator = ",",
                        usage = "Cancel only operations on pools matching this "
                                        + "comma-delimited set of operation states.")
        String[] state;

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose scan "
                                        + "update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose scan "
                                        + "update was after this date-time.")
        String lastScanAfter;

        @Option(name = "includeChildren",
                        usage = "Cancel file operations whose parents match "
                                        + "the pool pattern.  Note that this "
                                        + "option automatically sets 'forceRemoval' "
                                        + "to true on the child operation cancel. "
                                        + "Default is false.")
        boolean includeChildren = false;

        @Argument(required = false,
                        valueSpec = "regular expression",
                        usage = "Cancel only operations on pools matching this "
                                        + "expression.")
        String pools;

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
            filter.setParent(includeChildren);

            if (filter.isUndefined() && state == null) {
                return "Please set at least one option or argument "
                                + "other than 'includeChildren'.";
            }

            if (state == null) {
                state = new String[]{"WAITING", "RUNNING"};
            }

            filter.setState(ImmutableSet.copyOf(state));

            StringBuilder sb = new StringBuilder();

            sb.append("Issued cancel command to ")
                            .append(poolOperationMap.cancel(filter))
                            .append(" pool operations.");

            if (includeChildren) {
                fileOperationMap.cancel(filter);
                sb.append("  Also issued cancel command to file operations.");
            }

            return sb.toString();
        }
    }

    @Command(name = "pool exclude",
                    hint = "exclude pool operations",
                    description = "Scans the pool table and excludes "
                                    + "operations for the matching pools; "
                                    + "exclusion will cancel any running "
                                    + "operations.  The pool will not be included "
                                    + "in periodic, forced, or status-change "
                                    + "scans; locations on it are still considered "
                                    + "valid regarding replica count, but "
                                    + "cannot be used as copy sources.")
    class PoolOpExcludeCommand extends PoolOpActivateCommand {
        PoolOpExcludeCommand() {
            super(false);
        }
    }

    @Command(name = "pool include",
                    hint = "include pool operations",
                    description = "Scans the pool table and includes "
                                    + "operations for the matching pools; "
                                    + "include will only affect pool operations "
                                    + "that are currently excluded.")
    class PoolOpIncludeCommand extends PoolOpActivateCommand {
        PoolOpIncludeCommand() {
            super(true);
        }
    }

    @Command(name = "pool ls", hint = "list entries in the pool operation table",
                    description = "Scans the table and returns "
                        + "operations matching the filter parameters.")
    class PoolOpLsCommand extends ResilienceCommand {
        @Option(name = "status",
                        valueSpec = "DOWN|READ_ONLY|ENABLED|UNINITIALIZED",
                        usage = "List only operations on pools matching this "
                                        + "pool status.")
        String status;

        @Option(name = "state",
                        valueSpec = "IDLE|WAITING|RUNNING|FAILED|CANCELED"
                                        + "|EXCLUDED|INACTIVE",
                        separator = ",",
                        usage = "List only operations on pools matching this "
                                        + "comma-delimited set of operation states "
                                        + "(default is everything).")
        String[] state = {"IDLE", "WAITING", "RUNNING", "FAILED",
                          "CANCELED", "EXCLUDED", "INACTIVE"};

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose scan "
                                        + "update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose scan "
                                        + "update was after this date-time.")
        String lastScanAfter;

        @Argument(required = false,
                        usage = "List only operations on pools matching this "
                                        + "regular expression.")
        String pools;

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

    @Command(name = "pool scan",
                    hint = "launch a scan of one or more pools",
                    description = "A check will be initiated to see that the "
                                        + "number of replicas on the pool is "
                                        + "properly constrained, creating new "
                                        + "copies or removing redundant ones "
                                        + "as necessary. Note: will not override "
                                        + "a currently running operation; matching "
                                        + "operations in the waiting state will "
                                        + "be guaranteed to run at the next  "
                                        + "available slot opening.")
    class PoolScanCommand extends ResilienceCommand {
        @Option(name = "status",
                        valueSpec = "DOWN|READ_ONLY|ENABLED",
                        usage = "Scan only pools matching this "
                                        + "pool status.")
        String status;

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Scan only pools whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Scan only pools whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Scan only pools whose scan "
                                        + "update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Scan only pools whose scan "
                                        + "update was after this date-time.")
        String lastScanAfter;

        @Argument(usage = "Regular expression for pool(s) on which to "
                                        + "conduct the adjustment of "
                                        + "all files.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            PoolFilter filter = new PoolFilter();
            filter.setPools(pools);
            filter.setLastScanAfter(getTimestamp(lastScanAfter));
            filter.setLastScanBefore(getTimestamp(lastScanBefore));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setPoolStatus(status);

            StringBuilder builder = new StringBuilder("Scans have been issued for:\n");
            StringBuilder errors = new StringBuilder("ERRORS:\n");

            poolOperationMap.scan(filter, builder, errors);

            if (errors.length() > 8) {
                builder.append(errors);
            }

            return builder.toString();
        }
    }

    private static Long getTimestamp(String datetime) throws ParseException {
        if (datetime == null) {
            return null;
        }
        DateFormat format = new SimpleDateFormat(FORMAT_STRING);
        return format.parse(datetime).getTime();
    }

    /**
     * Used for potentially long database queries.  Executing them
     * synchronously can bind the command interpreter.  We store
     * the future in case of cancellation.
     */
    private final Map<String, Future<?>>  futureMap
                    = Collections.synchronizedMap(new HashMap<>());

    private ExecutorService      executor;
    private PoolInfoMap poolInfoMap;
    private MessageGuard messageGuard;
    private MapInitializer initializer;
    private PoolOperationMap poolOperationMap;
    private FileOperationHandler fileOperationHandler;
    private FileOperationMap fileOperationMap;
    private NamespaceAccess namespaceAccess;
    private OperationStatistics counters;
    private OperationHistory history;
    private String               resilienceDir;

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setHistory(OperationHistory history) {
        this.history = history;
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

    public void setFileOperationHandler(
                    FileOperationHandler fileOperationHandler) {
        this.fileOperationHandler = fileOperationHandler;
    }

    public void setFileOperationMap(FileOperationMap fileOperationMap) {
        this.fileOperationMap = fileOperationMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolOperationMap(PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    public void setResilienceDir(String resilienceDir) {
        this.resilienceDir = resilienceDir;
    }
}
