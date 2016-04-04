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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import dmg.util.command.Option;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PnfsFilter;
import org.dcache.resilience.data.PnfsOperation;
import org.dcache.resilience.data.PnfsOperationMap;
import org.dcache.resilience.data.PnfsUpdate;
import org.dcache.resilience.data.PoolFilter;
import org.dcache.resilience.data.PoolInfoFilter;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.db.NamespaceAccess;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.MessageGuard;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Collects all admin shell commands for convenience.  See further individual
 *      command annotations for details.</p>
 *
 * <p>In order to be able to change the name of the commands via the annotation,
 *      this class, which contains the actual command code, is abstract. This
 *      is done in anticipation of a possibly embedded version of resilience
 *      (running inside another service, such as PnfsManager).</p>
 *
 * @see StandaloneResilienceCommands
 */
public abstract class ResilienceCommands implements CellCommandListener {
    static final String HINT_CHECK     =
                    "launch an operation to adjust replicas for "
                                    + "one or more pnfsids";
    static final String HINT_COUNTS    = "list pnfsids and replica counts for a pool";
    static final String HINT_DIAG      = "print diagnostic statistics";
    static final String HINT_DIAG_HIST = "print diagnostic history";
    static final String HINT_DISABLE   = "turn off replication handling";
    static final String HINT_ENABLE    = "turn on replication handling";
    static final String HINT_HIST      =
                    "display a history of the most recent terminated "
                                    + "pnfs operations";
    static final String HINT_PNFSCNCL = "cancel pnfs operations";
    static final String HINT_PNFS_CTRL = "control checkpointing or handling of pnfs operations";
    static final String HINT_PNFSLS = "list entries in the pnfs operation table";
    static final String HINT_POOLCNCL = "cancel pool operations";
    static final String HINT_POOL_CTRL =
                    "control the periodic check of active resilient pools "
                                    + "or processing of pool state changes";
    static final String HINT_POOLEXCL = "exclude pool operations";
    static final String HINT_POOLINCL = "include pool operations";
    static final String HINT_PGROUP_INFO =
                    "list the storage units linked to a pool group "
                                    + "and confirm resilience constraints "
                                    + "can be met by the member pools";
    static final String HINT_POOLINFO = "list tags and mode for a pool or pools";
    static final String HINT_POOLLS = "list entries in the pool operation table";
    static final String HINT_SCAN = "launch a scan of one or more pools";

    static final String DESC_CHECK     =
                    "For each pnfsid, runs a check to see that the number of "
                                    + "replicas is properly constrained, creating "
                                    + "new copies or removing redundant ones "
                                    + "as necessary.";
    static final String DESC_PNFS_CTRL =
                    "Runs checkpointing, resets checkpoint "
                                    + "properties, resets operation properties, "
                                    + "turn processing of operations on or off "
                                    + "(start/shutdown), or displays info relevant "
                                    + "to operation processing and checkpointing.";
    static final String DESC_COUNTS    =
                    "Issues a query to the namespace; "
                                    + "results are written to a file.  "
                                    + "Executed asynchronously.";
    static final String DESC_DIAG      =
                    "Lists the total number of messages received  "
                                    + "and operations performed since last start "
                                    + "of the resilience system.  Rate/sec is "
                                    + "sampled over the interval since the last "
                                    + "checkpoint. These values for new "
                                    + "location messages and pnfsid "
                                    + "operations are recorded "
                                    + "to a stastistics file located in the "
                                    + "resilience home directory, and which "
                                    + "can be displayed using the history option";
    static final String DESC_DIAG_HIST =
                    "Reads in the contents of the diagnostic history file "
                                    + "recording periodic statics "
                                    + "(see diag command).";
    static final String DESC_DISABLE =
                    "Prevents messages from being processed by "
                                    + "the replication system (this is useful  "
                                    + "for instance if rebalance is run on "
                                    + "a resilient group). "
                                    + "To disable all internal operations, use "
                                    + "the 'strict' argument to this command; "
                                    + "this option will also cancel all pool and "
                                    + "pnfs operations.";
    static final String DESC_ENABLE =
                    "Allows messages to be processed by "
                                    + "the replication system. Will also "
                                    + "(re-)enable all internal operations if they "
                                    + "are not running.  Executed asynchronously.";
    static final String DESC_HIST =
                    "When pnfs operations complete or are aborted, their "
                                    + "string representations are added to a "
                                    + "circular buffer whose capacity is set "
                                    + "by the property "
                                    + "'pnfsmanager.resilience.history.buffer-size'.";
    static final String DESC_PNFSCNCL =
                    "Scans the pnfs table and cancels "
                                    + "operations matching the filter parameters.";
    static final String DESC_PNFSLS =
                    "Scans the table and returns operations matching "
                                    + "the filter parameters.";
    static final String DESC_POOLCNCL =
                    "Scans the pool table and cancels "
                                    + "operations matching the filter parameters; "
                                    + "if 'includeParents' is true, also "
                                    + "scans the pnfs table.";
    static final String DESC_POOLEXCLINCL =
                    "Scans the pool table and excludes or includes "
                                    + "operations for the matching pools; "
                                    + "exclusion will cancel any running "
                                    + "operations; include "
                                    + "will only affect pool operations "
                                    + "that are currently excluded.  "
                                    + "When a pool is excluded (for the purposes "
                                    + "of operations), it will not be included "
                                    + "in periodic, forced, or status-change "
                                    + "scans, though the pool will still "
                                    + "appear to its pool group as a member "
                                    + "in whatever state the pool manager "
                                    + "knows about.";
    static final String DESC_POOL_CTRL =
                    "Activates, deactivates, or resets the periodic checking "
                                    + "of active pools; turns all pool state "
                                    + "handling on or off (start/shutdown)";
    static final String DESC_PGROUP_INFO =
                    "Lists name, key and storage units linked to the pool group."
                                    + "Tries to satisfy the constraints on replica "
                                    + "count and exclusivity tags for all the "
                                    + "storage units in the pool group by "
                                    + "attempting to assign the required number "
                                    + "of locations for a hypothetical file "
                                    + "belonging to each unit.";
    static final String DESC_POOLINFO = "Lists pool key, name, mode, "
                                    + "status, tags and last update time.";
    static final String DESC_POOLLS = "Scans the table and returns "
                                    + "operations matching the filter parameters.";
    static final String DESC_SCAN =
                    "A check will be initiated to see that the "
                                    + "number of replicas on the pool is "
                                    + "properly constrained, creating new "
                                    + "copies or removing redundant ones "
                                    + "as necessary. Note: will not override "
                                    + "a currently running operation; matching "
                                    + "operations in the waiting state will "
                                    + "be guaranteed to run at the next available "
                                    + "slot opening.";

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

    abstract class CountsCommand extends ResilienceCommand {
        @Option(name = "eq",  usage = "Filter on '=' this value.")
        Integer eq;

        @Option(name = "gt",  usage = "Filter on '>' this value.")
        Integer gt;

        @Option(name = "lt",  usage = "Filter on '<' this value.")
        Integer lt;

        @Option(name = "cancel", usage = "Cancel the running job (default is false).")
        boolean cancel = false;

        @Argument(usage = "Specify a regular expression for pool names.")
        String expression;

        @Override
        protected String doCall() throws Exception {
            try {
                StringBuilder builder = new StringBuilder();
                Pattern pattern = Pattern.compile(expression);
                String inequality;

                if (eq != null) {
                    inequality = "=" + eq;
                } else if (gt != null) {
                    inequality = ">" + gt;
                } else if (lt != null) {
                    inequality = "<" + lt;
                } else {
                    inequality = null;
                }

                poolInfoMap.getResilientPools().stream().forEach((pool) -> {
                    if (pattern.matcher(pool).find()) {
                        if (cancel) {
                            synchronized(futureMap) {
                                Future<?> future = futureMap.remove(pool);
                                if (future != null) {
                                    future.cancel(true);
                                }
                            }

                            builder.append("cancelled job for ")
                                   .append(pool).append("\n");
                        } else {
                            File file = printToFile(pool,
                                                    resilienceDir,
                                                    inequality);
                            builder.append("   ")
                                   .append(file.getAbsolutePath()).append("\n");
                        }
                    }
                });

                builder.insert(0, "Started jobs to write the lists "
                                + "to the following files:\n\n");
                builder.append("Check pinboard for progress.\n");

                return builder.toString();
            } catch (Exception e) {
                return new ExceptionMessage(e).toString();
            }
        }

        private File printToFile(String pool, String dir, String inequality) {
            File file = new File(dir, pool);

            Future<?> future = executor.submit(() -> {
                synchronized(futureMap) {
                    while (!futureMap.containsKey(pool)) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }

                if (!Thread.currentThread().isInterrupted()) {
                    try (PrintWriter pw = new PrintWriter(new FileWriter(file,
                                                                         false))) {
                        try {
                            if (inequality != null) {
                                namespaceAccess.getPnfsidCountsFor(pool,
                                                                   inequality,
                                                                   pw);
                            } else {
                                namespaceAccess.getPnfsidCountsFor(pool, pw);
                            }
                        } catch (CacheException | ParseException e) {
                            // write out the error to the file
                            e.printStackTrace(pw);
                        }
                    } catch (IOException e) {
                        // let it go ...
                    }
                }

                synchronized(futureMap) {
                    futureMap.remove(pool);
                }
            });

            synchronized(futureMap) {
                futureMap.put(pool, future);
            }

            return file;
        }
    }

    abstract class DiagCommand extends ResilienceCommand {
        @Argument(required = false,
                  usage = "Include pools matching this regular expression; "
                                  + "default prints only summary info.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            return counters.print(pools);
        }
    }

    abstract class DiagHistoryCommand extends ResilienceCommand {
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

    abstract class DisableCommand extends ResilienceCommand {
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
                    return "Unrecognized argument " + strict;
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

    abstract class EnableCommand implements Callable<String>  {
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

    abstract class PnfsCheckCommand extends ResilienceCommand {
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
                PnfsUpdate update
                                = new PnfsUpdate(pnfsId, pool,
                                                 MessageType.ADD_CACHE_LOCATION,
                                                 true);
                pnfsOperationHandler.handleLocationUpdate(update);
            }
            return "An adjustment activity has been started for " + pnfsids + ".";
        }
    }

    abstract class PnfsControlCommand extends ResilienceCommand {
        @Argument(valueSpec = "ON|OFF|START|SHUTDOWN|RESET|RUN|INFO",
                        required = false,
                        usage = "off = turn checkpointing off; "
                                        + "on = turn checkpointing on; "
                                        + "info = information (default); "
                                        + "reset = reset properties; "
                                        + "start = (re)start processing of pnfs operations; "
                                        + "shutdown = stop all processing of pnfs operations; "
                                        + "run = checkpoint to disk immediately." )
        String arg = "INFO";

        @Option(name = "checkpoint",
                        usage = "With reset mode (one of checkpoint|sweep). "
                                        + "Interval length between checkpointing "
                                        + "of the pnfs operation data.")
        Long checkpoint;

        @Option(name = "sweep",
                        usage = "With reset mode (one of checkpoint|sweep). "
                                        + "Minimal interval between sweeps of "
                                        + "the pnfs operations.")
        Long sweep;

        @Option(name = "unit",
                        valueSpec = "SECONDS|MINUTES|HOURS",
                        usage = "Checkpoint or sweep interval unit.")
        TimeUnit unit;

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
                    if (pnfsOperationMap.isRunning()) {
                        return "Consumer is already running.";
                    }
                    new Thread(() -> {
                        pnfsOperationMap.initialize();
                        pnfsOperationMap.reload();
                    }).start();
                    return "Consumer initialization "
                                    + "and reload of checkpoint file started.";
                case SHUTDOWN:
                    if (!pnfsOperationMap.isRunning()) {
                        return "Consumer is not running.";
                    }
                    pnfsOperationMap.shutdown();
                    return "Consumer has been shutdown.";
                case OFF:
                    if (pnfsOperationMap.isCheckpointingOn()) {
                        pnfsOperationMap.stopCheckpointer();
                        return "Shut down checkpointing.";
                    }
                    return "Checkpointing already off.";
                case ON:
                    if (!pnfsOperationMap.isCheckpointingOn()) {
                        pnfsOperationMap.startCheckpointer();
                        return infoMessage();
                    }
                    return "Checkpointing already on.";
                case RUN:
                    if (!pnfsOperationMap.isCheckpointingOn()) {
                        return "Checkpointing is off; please turn it on first.";
                    }
                    pnfsOperationMap.runCheckpointNow();
                    return "Forced checkpoint.";
                case RESET:
                    if (!pnfsOperationMap.isCheckpointingOn()) {
                        return "Checkpointing is off; please turn it on first.";
                    }

                    if (checkpoint != null) {
                        pnfsOperationMap.setCheckpointExpiry(checkpoint);
                        if (unit != null) {
                            pnfsOperationMap.setCheckpointExpiryUnit(unit);
                        }
                    } else if (sweep != null) {
                        pnfsOperationMap.setTimeout(sweep);
                        if (unit != null) {
                            pnfsOperationMap.setTimeoutUnit(unit);
                        }
                    }

                    if (retries != null) {
                        pnfsOperationMap.setMaxRetries(retries);
                    }

                    if (file != null) {
                        pnfsOperationMap.setCheckpointFilePath(file);
                    }

                    pnfsOperationMap.reset();
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
                            pnfsOperationMap.getMaxRunning(),
                            pnfsOperationMap.getMaxRetries()));
            info.append(String.format("sweep interval %s %s\n",
                            pnfsOperationMap.getTimeout(),
                            pnfsOperationMap.getTimeoutUnit()));
            info.append(String.format("checkpoint interval %s %s\n"
                                            + "checkpoint file path %s\n",
                            pnfsOperationMap.getCheckpointExpiry(),
                            pnfsOperationMap.getCheckpointExpiryUnit(),
                            pnfsOperationMap.getCheckpointFilePath()));
            counters.getPnfsSweepInfo(info);
            counters.getCheckpointInfo(info);
            return info.toString();
        }
    }

    abstract class PnfsOpCancelCommand extends ResilienceCommand {
        @Option(name = "state",
                        valueSpec = "WAITING|RUNNING",
                        separator = ",",
                        usage = "Cancel operations for pnfsids matching this "
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
                        usage = "Cancel only operations for pnfsids with this "
                                        + "policy.")
        String retentionPolicy;

        @Option(name = "storageUnit",
                        usage = "Cancel only operations for pnfsids with this "
                                        + "storage unit/group.")
        String storageUnit;

        @Option(name = "opCount",
                        usage = "Cancel only operations with this operation count.")
        Integer opCount;

        @Option(name = "parent",
                        usage = "Cancel only operations with this parent pool name.")
        String parent;

        @Option(name = "source",
                        usage = "Cancel only operations with this source pool name.")
        String source;

        @Option(name = "target",
                        usage = "Cancel only operations with this target pool name.")
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

            PnfsFilter filter = new PnfsFilter();

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
                    pnfsOperationMap.cancel(new PnfsId(pnfsids), forceRemoval);
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

            pnfsOperationMap.cancel(filter);

            return "Issued cancel command to cancel pnfs operations.";
        }
    }

    abstract class PnfsOpHistoryCommand extends ResilienceCommand {
        @Argument(required = false,
                        valueSpec = "errors",
                        usage = "Display just the failures")
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

    abstract class PnfsOpLsCommand extends ResilienceCommand {
        @Option(name = "retentionPolicy",
                        valueSpec = "REPLICA|CUSTODIAL",
                        usage = "List only operations for pnfsids with this "
                                        + "policy.")
        String retentionPolicy;

        @Option(name = "storageUnit",
                        usage = "List only operations for pnfsids with this "
                                        + "storage unit/group.")
        String storageUnit;

        @Option(name = "state",
                        valueSpec = "WAITING|RUNNING",
                        separator = ",",
                        usage = "List only operations for pnfsids matching this "
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
                        usage = "List only operations with parent pool name.")
        String parent;

        @Option(name = "source",
                        usage = "List only operations with source pool name.")
        String source;

        @Option(name = "target",
                        usage = "List only operations with target pool name.")
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
                                        + "the number of pnfsid entries; '$@' to "
                                        + "return the op counts by pool.")
        String pnfsids;

        @Override
        protected String doCall() throws Exception {
            boolean count = "$".equals(pnfsids) || "$@".equals(pnfsids);
            Set<String> stateSet = ImmutableSet.copyOf(state);

            PnfsFilter filter = new PnfsFilter();
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
                long total = pnfsOperationMap.count(filter, builder);

                if (builder == null) {
                    return total + " matching pnfsids";
                }

                return String.format("%s matching pnfsids."
                                + "\n\nOperation counts per pool:\n%s",
                                total, builder.toString());
            }

            if (filter.isSimplePnfsMatch()) {
                PnfsOperation op = pnfsOperationMap.getOperation(new PnfsId(pnfsids));
                if (op == null) {
                    return String.format("No operation currently registered for %s.",
                                    pnfsids);
                }
                return op.toString() + "\n";
            }

            long size = pnfsOperationMap.size();
            int limitValue = (int)size;

            if (limit == null) {
                if (stateSet.contains("WAITING") && size >= LS_THRESHOLD) {
                    return String.format(REQUIRE_LIMIT, size, size);
                }
            } else {
                limitValue = limit;
            }

            return pnfsOperationMap.list(filter, limitValue);
        }
    }

    abstract class PoolControlCommand extends ResilienceCommand {
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
        String window;

        @Option(name = "sweep",
                        usage = "With reset mode (one of window|sweep|down|restart). "
                                        + "How often a sweep of the pool "
                                        + "operations is made.")
        String sweep;

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
                    return "Forced watchdog scan";
                case RESET:
                    if (!poolOperationMap.isWatchdogOn()) {
                        return "Watchdog is off; please turn it on first.";
                    }

                    if (window != null) {
                        poolOperationMap.setRescanWindow(Integer.parseInt(window));
                        if (unit != null) {
                            poolOperationMap.setRescanWindowUnit(unit);
                        }
                    } else if (sweep != null) {
                        poolOperationMap.setTimeout(Integer.parseInt(sweep));
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

    abstract class PoolGroupInfoCommand extends ResilienceCommand {
        @Option(name = "key",
                        usage = "List pool group info for this group key.")
        Integer key;

        @Option(name = "showUnits",
                        usage = "List storage units linked to this group.")
        boolean showUnits = false;

        @Option(name = "verify",
                        usage = "Run the verification procedure for "
                                        + "the units in the group "
                                        + "(default is false)")
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

    abstract class PoolInfoCommand extends ResilienceCommand {
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

    abstract class PoolOpCancelCommand extends ResilienceCommand {
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
                        usage = "Cancel pnfsId operations whose parents match "
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
                pnfsOperationMap.cancel(filter);
                sb.append("  Also issued cancel command to pnfsId operations.");
            }

            return sb.toString();
        }
    }

    abstract class PoolOpExcludeCommand extends PoolOpActivateCommand {
        PoolOpExcludeCommand() {
            super(false);
        }
    }

    abstract class PoolOpIncludeCommand extends PoolOpActivateCommand {
        PoolOpIncludeCommand() {
            super(true);
        }
    }

    abstract class PoolOpLsCommand extends ResilienceCommand {
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

    abstract class PoolScanCommand extends ResilienceCommand {
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
                                        + "all pnfsids.")
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
     * synchronously can bind the command interpreter.
     */
    private final Map<String, Future<?>>      futureMap = new HashMap<>();

    private ExecutorService      executor;
    private PoolInfoMap          poolInfoMap;
    private MessageGuard         messageGuard;
    private MapInitializer       initializer;
    private PoolOperationMap     poolOperationMap;
    private PnfsOperationHandler pnfsOperationHandler;
    private PnfsOperationMap     pnfsOperationMap;
    private NamespaceAccess      namespaceAccess;
    private OperationStatistics  counters;
    private OperationHistory     history;
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

    public void setPnfsOperationHandler(
                    PnfsOperationHandler pnfsOperationHandler) {
        this.pnfsOperationHandler = pnfsOperationHandler;
    }

    public void setPnfsOperationMap(PnfsOperationMap pnfsOperationMap) {
        this.pnfsOperationMap = pnfsOperationMap;
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
