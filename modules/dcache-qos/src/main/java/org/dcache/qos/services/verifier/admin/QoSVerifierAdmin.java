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
package org.dcache.qos.services.verifier.admin;

import static org.dcache.qos.services.verifier.data.VerifyOperationState.READY;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.RUNNING;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.WAITING;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.PoolInfoFilter;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.data.VerifyOperationCancelFilter;
import org.dcache.qos.services.verifier.data.VerifyOperationFilter;
import org.dcache.qos.services.verifier.data.VerifyOperationManager;
import org.dcache.qos.services.verifier.data.VerifyOperationState;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.util.InitializerAwareCommand;
import org.dcache.qos.util.MapInitializer;
import org.dcache.qos.util.QoSHistory;
import org.dcache.vehicles.FileAttributes;

public final class QoSVerifierAdmin implements CellCommandListener {

    private static Set<FileAttribute> LOCATION_ATTRIBUTES
          = Collections.unmodifiableSet(EnumSet.of(FileAttribute.PNFSID,
          FileAttribute.LOCATIONS));

    abstract class FilteredVerifyOpCommand extends InitializerAwareCommand {

        @Option(name = "action",
              valueSpec = "COPY_REPLICA|CACHE_REPLICA|PERSIST_REPLICA|WAIT_FOR_STAGE|FLUSH",
              usage = "Match only operations for files with these action types.")
        protected QoSAction[] action;

        @Option(name = "msgType",
              valueSpec = "CORRUPT_FILE|CLEAR_CACHE_LOCATION|ADD_CACHE_LOCATION|QOS_MODIFIED"
                    + "|QOS_MODIFIED_CANCELED|POOL_STATUS_DOWN|POOL_STATUS_UP|SYSTEM_SCAN"
                    + "|VALIDATE_ONLY",
              usage = "Match only operations for files with these message types.")
        protected QoSMessageType[] msgType;

        @Option(name = "state",
              valueSpec = "READY|RUNNING|WAITING|DONE|CANCELED|FAILED|ABORTED",
              separator = ",",
              usage =
                    "Match only operations for files matching this comma-delimited set of states; "
                          + "default is RUNNING, READY, WAITING.")
        protected VerifyOperationState[] state = {RUNNING, READY, WAITING};

        @Option(name = "storageUnit",
              usage = "Match only operations for files with this storage unit.")
        protected String storageUnit;

        @Option(name = "group",
              usage =
                    "Match only operations with this preferred pool group; use the option with no "
                          + "value to match only operations without a specified group.")
        protected String poolGroup;

        @Option(name = "parent",
              usage =
                    "Match only operations with this parent pool name; use the option with no value "
                          + "to match only operations without a parent pool.")
        String parent;

        @Option(name = "source",
              usage =
                    "Match only operations with this source pool name; use the option with no value "
                          + "to match only operations without a source pool.")
        protected String source;

        @Option(name = "target",
              usage =
                    "Match only operations with this target pool name; use the option with no value "
                          + "to match only operations without a target pool.")
        protected String target;

        @Option(name = "retriedMoreThan",
              usage = "Match only the operation with this number of retries or greater.")
        protected Integer retried;

        @Option(name = "lastUpdateBefore", valueSpec = FORMAT_STRING,
              usage = "Match only operations whose start time is before this date-time.")
        protected String lastUpdateBefore;

        @Option(name = "lastUpdateAfter", valueSpec = FORMAT_STRING,
              usage = "Match only operations whose start time is after this date-time.")
        protected String lastUpdateAfter;

        @Argument(required = false,
              usage = "Match only activities for this comma-delimited list of pnfsids. "
                    + "Leaving the argument unspecified or using '*' matches all pnfsids.")

        protected String pnfsids;

        protected FilteredVerifyOpCommand() {
            super(initializer);
        }

        protected VerifyOperationFilter getFilter() {
            PnfsId[] ids;

            if (pnfsids == null || pnfsids.equals("*")) {
                ids = null;
            } else {
                ids = Arrays.stream(pnfsids.split(","))
                      .map(String::trim)
                      .map(PnfsId::new)
                      .collect(Collectors.toSet()).toArray(PnfsId[]::new);
            }

            VerifyOperationFilter filter = new VerifyOperationFilter();
            filter.setPnfsIds(ids);
            filter.setLastUpdateAfter(lastUpdateAfter);
            filter.setLastUpdateBefore(lastUpdateBefore);
            filter.setMsgType(msgType);
            filter.setState(state);
            filter.setAction(action);
            filter.setPoolGroup(poolGroup);
            filter.setStorageUnit(storageUnit);
            filter.setParent(parent);
            filter.setSource(source);
            filter.setTarget(target);
            filter.setRetried(retried);
            return filter;
        }
    }

    @Command(name = "pool info",
          hint = "list tags and mode for a pool or pools",
          description = "Lists pool name, mode, status, tags and last update time.")
    class PoolInfoCommand extends InitializerAwareCommand {

        @Option(name = "status",
              valueSpec = "DOWN|READ_ONLY|ENABLED|UNINITIALIZED",
              separator = ",",
              usage = "List only information for pools matching this  comma-delimited set of states.")
        String[] status = {"DOWN", "READ_ONLY", "ENABLED", "UNINITIALIZED"};

        @Option(name = "lastUpdateBefore", valueSpec = FORMAT_STRING,
              usage = "List only operations whose last update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter", valueSpec = FORMAT_STRING,
              usage = "List only operations whose last update was after this date-time.")
        String lastUpdateAfter;

        @Argument(required = false,
              usage = "Regular expression to match pool names; no argument matches all pools.")
        String pools;

        PoolInfoCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            PoolInfoFilter filter = new PoolInfoFilter();
            filter.setPools(pools);
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

    @Command(name = "verify",
          hint = "launch an operation to verify one or more pnfsids",
          description =
                "For each pnfsid, runs a check to see that the number of replicas is properly "
                      + "constrained, creating new copies or removing redundant ones as necessary.")
    class VerifyCommand extends InitializerAwareCommand {

        @Argument(usage = "Comma-delimited list of pnfsids for which to run the adjustment.")
        String pnfsids;

        VerifyCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            try {
                return runFileChecks(Arrays.stream(pnfsids.split("[,]"))
                      .map(PnfsId::new).collect(Collectors.toSet()));
            } catch (Throwable t) {
                return t.getMessage();
            }
        }
    }

    @Command(name = "verify cancel",
          hint = "cancel verify operations",
          description = "Scans the file table and cancels operations matching the filter parameters.")
    class VerifyCancelCommand extends FilteredVerifyOpCommand {

        @Option(name = "forceRemoval",
              usage =
                    "Remove all waiting operations for this match after cancellation of the running "
                          + "tasks. (Default is false; this option is redundant if the state includes WAITING.)")
        boolean forceRemoval = false;

        @Override
        protected String doCall() throws Exception {
            if (pnfsids == null) {
                return "To cancel you must specify one or more pnfsids, or '*' for all matching pnfsids.";
            }

            try {
                VerifyOperationFilter filter = getFilter();
                forceRemoval |= Arrays.stream(state).collect(Collectors.toSet()).contains(WAITING);
                manager.cancel(new VerifyOperationCancelFilter(filter, forceRemoval));
                return "Issued cancel command to cancel verify operations.";
            } catch (IllegalArgumentException e) {
                return "Improper input: " + e.getMessage();
            }
        }
    }

    @Command(name = "verify failed",
          hint = "launch operations to rerun verify for all pnfsids currently appearing in "
                + "the history errors list",
          description = "For each pnfsid, runs a check to see that "
                + "the requirements are satisfied, and submits the appropriate adjustment "
                + "requests if not. NOTE: running this command also clears the current errors list.")
    class VerifyFailedCommand extends InitializerAwareCommand {

        VerifyFailedCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            return runFileChecks(history.getAndClearErrorPnfsids()
                  .stream().map(PnfsId::new)
                  .collect(Collectors.toSet()));
        }
    }

    @Command(name = "verify details",
          hint = "list diagnostic information concerning verification by pool",
          description = "Gives statistics for verification completed or failed by pool.")
    class VerifyDetailsCommand extends InitializerAwareCommand {

        VerifyDetailsCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            StringBuilder builder = new StringBuilder();
            counters.appendDetails(builder);
            return builder.toString();
        }
    }

    @Command(name = "verify history",
          hint = "display a history of the most recent terminated operations",
          description = "When operations complete or are aborted, their string representations "
                + "are added to a circular buffer whose capacity is set by the property "
                + "'qos.limits.file.operation-history'.")
    class VerifyHistoryCommand extends InitializerAwareCommand {

        @Argument(required = false, valueSpec = "errors", usage = "Display just the failures.")
        String errors;

        @Option(name = "limit", usage = "Display up to this number of entries.")
        Integer limit;

        @Option(name = "order", valueSpec = "ASC|DESC",
              usage = "Display entries in ascending (default) or descending order of arrival.")
        String order = "ASC";

        VerifyHistoryCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            boolean failed = false;
            if (errors != null) {
                if (!"errors".equals(errors)) {
                    return "Optional argument must be 'errors'";
                }
                failed = true;
            }

            SortOrder order = SortOrder.valueOf(this.order.toUpperCase());

            switch (order) {
                case DESC:
                    if (limit != null) {
                        return history.descending(failed, limit);
                    }
                    return history.descending(failed);
                default:
                    if (limit != null) {
                        return history.ascending(failed, limit);
                    }
                    return history.ascending(failed);
            }
        }
    }

    @Command(name = "verify ls",
          hint = "list entries in the operation table",
          description = "Scans the table and returns operations matching the filter parameters.")
    class VerifyLsCommand extends FilteredVerifyOpCommand {

        @Option(name = "count", usage = "Do not list, but return only the number of matches.")
        boolean count = false;

        @Option(name = "limit",
              usage = "Maximum number of rows to list.  This option becomes required when "
                    + "the operation queues reach " + LS_THRESHOLD + "; be aware that "
                    + "listing more than this number of rows may provoke an out of memory "
                    + "error for the domain.")
        Integer limit;

        @Option(name = "order", valueSpec = "ASC|DESC",
              usage = "Display entries in ascending (default) or descending order of arrival.")
        String order = "ASC";

        @Override
        protected String doCall() throws Exception {
            boolean reverse = order.toUpperCase().equals("DESC");

            VerifyOperationFilter filter;
            try {
                filter = getFilter();
            } catch (IllegalArgumentException e) {
                return "Improper input: " + e.getMessage();
            }

            filter.setReverse(reverse);

            if (count) {
                return manager.count(filter) + " matching pnfsids";
            }

            long size = manager.size();
            int limitValue = (int) size;

            if (limit == null) {
                Set<VerifyOperationState> stateSet = Arrays.stream(state)
                      .collect(Collectors.toSet());
                if ((stateSet.contains(READY) || stateSet.contains(WAITING))
                      && size >= LS_THRESHOLD) {
                    return String.format(REQUIRE_LIMIT, size, size);
                }
            } else {
                limitValue = limit;
            }

            return manager.list(filter, limitValue);
        }
    }

    @Command(name = "verify reset",
          hint = "control the settings for operation handling",
          description = "Reset operation properties.")
    class VerifyResetCommand extends InitializerAwareCommand {

        @Option(name = "sweep",
              usage = "With reset mode (one of sweep|delay). Minimal interval between "
                    + "sweeps of the verify operations.")
        Long sweep;

        @Option(name = "unit", valueSpec = "SECONDS|MINUTES|HOURS",
              usage = "sweep interval unit.")
        TimeUnit unit;

        @Option(name = "maxRunning", usage = "Maximum number of concurrent running operations.")
        Integer maxRunning;

        @Option(name = "retries", usage = "Maximum number of retries on a failed operation.")
        Integer retries;

        VerifyResetCommand() {
            super(initializer);
        }

        @Override
        protected String doCall() throws Exception {
            if (sweep != null) {
                manager.setTimeout(sweep);
                if (unit != null) {
                    manager.setTimeoutUnit(unit);
                }
            }

            if (maxRunning != null) {
                manager.setMaxRunning(maxRunning);
            }

            if (retries != null) {
                manager.setMaxRetries(retries);
            }

            return manager.infoMessage();
        }
    }

    @Command(name = "verify stats", hint = "print diagnostic statistics",
          description = "Reads in the contents of the file recording periodic statistics.")
    class VerifyStatsCommand extends InitializerAwareCommand {

        @Option(name = "limit", usage = "Display up to this number of lines.")
        Integer limit = (int)TimeUnit.DAYS.toMinutes(1);

        @Option(name = "order", valueSpec = "asc|desc",
              usage = "Display lines in ascending (default) or descending order by timestamp.")
        String order = "asc";

        @Option(name = "enable",
              usage = "Turn the recording of statistics to file on or off. Recording to file is "
                    + "off by default.")
        Boolean enable = null;

        VerifyStatsCommand() {
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

    private CellStub pnfsManager;
    private MapInitializer initializer;
    private PoolInfoMap poolInfoMap;
    private VerifyOperationManager manager;
    private QoSVerifierCounters counters;
    private QoSHistory history;

    public void setCounters(QoSVerifierCounters counters) {
        this.counters = counters;
    }

    /*
     *  Needs concrete implementation type to set initialization and running parameters.
     */
    public void setManager(VerifyOperationManager manager) {
        this.manager = manager;
    }

    public void setHistory(QoSHistory history) {
        this.history = history;
    }

    public void setInitializer(MapInitializer initializer) {
        this.initializer = initializer;
    }

    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    private String runFileChecks(Collection<PnfsId> list) {
        StringBuilder reply = new StringBuilder();
        int successful = 0;
        for (PnfsId pnfsId : list) {
            try {
                FileAttributes attr
                      = getPnfsHandler().getFileAttributes(pnfsId, LOCATION_ATTRIBUTES);
                Iterator<String> it = attr.getLocations().iterator();
                FileQoSUpdate update = new FileQoSUpdate(pnfsId, it.hasNext() ? it.next() : null,
                      QoSMessageType.VALIDATE_ONLY);
                manager.createOrUpdateOperation(update);
                ++successful;
            } catch (NoSuchElementException | CacheException e) {
                reply.append(pnfsId).append(" ").append(e.getMessage()).append("\n");
            }
        }

        reply.append("verification started for ").append(successful).append(" files.\n");

        return reply.toString();
    }

    private PnfsHandler getPnfsHandler() {
        PnfsHandler handler = new PnfsHandler(pnfsManager);
        handler.setSubject(Subjects.ROOT);
        handler.setRestriction(Restrictions.none());
        return handler;
    }
}
