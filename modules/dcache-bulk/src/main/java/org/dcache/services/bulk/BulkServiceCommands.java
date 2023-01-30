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
package org.dcache.services.bulk;

import static dmg.util.PagedCommandResult.EOL;
import static java.util.stream.Collectors.joining;
import static org.dcache.services.bulk.store.jdbc.JdbcBulkDaoUtils.toSetOrNull;

import com.google.common.base.Splitter;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.PagedCommandResult;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.activity.BulkActivityArgumentDescriptor;
import org.dcache.services.bulk.activity.BulkActivityFactory;
import org.dcache.services.bulk.activity.BulkActivityProvider;
import org.dcache.services.bulk.handler.BulkSubmissionHandler;
import org.dcache.services.bulk.manager.BulkRequestManager;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestFilter;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.services.bulk.util.BulkTargetFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Collects all admin shell commands for convenience.
 * <p/>
 * See further individual command annotations for details.
 */
public final class BulkServiceCommands implements CellCommandListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkServiceCommands.class);

    /**
     * id | arrived | started | modified | owner | activity | depth | prestore | status :
     * urlPrefix/uid
     */
    private static final String FORMAT_REQUEST_FULL
          = "%-12s | %-19s | %19s | %19s | %12s | %15s | %7s | %11s | %8s | %s/%s";

    /**
     * (urlPrefix/uid):   target string
     */
    private static final String FORMAT_REQUEST_TARGET = "(%s/%s):    %s";

    /**
     * (policy configuration property:  value)
     */
    private static final String FORMAT_REQUEST_POLICY = "%-40s : %10s\n";

    /**
     * createdAt | startedAt| completedAt | state | target
     */
    private static final String FORMAT_TARGET_INFO = "%-25s | %25s | %25s | %12s | %s";

    /**
     * error type | error message
     */
    private static final String FORMAT_TARGET_INFO_ERROR = " –– (ERROR: %s : %s)";

    /**
     * id | arrived | modified | owner | status | uid
     */
    private static final String FORMAT_REQUEST = "%-12s | %-19s | %19s | %12s | %10s | %s";

    /**
     * id | pid | created | updated | ruid | activity | state | type | [pnfsid]: path
     */
    private static final String FORMAT_TARGET_FULL = "%-16s | %-12s | %-19s | %-19s | %36s | %15s | %11s | %8s | [%s]: %s";

    /**
     * id | updated | rid | path
     */
    private static final String FORMAT_TARGET = "%-16s | %-19s | %12s | %s";

    /**
     * name | class | type | permits
     */
    private static final String FORMAT_ACTIVITY = "%-20s | %100s | %7s | %10s ";

    /**
     * name | required | description
     */
    private static final String FORMAT_ARGUMENT = "%-20s | %15s | %30s |  %s";

    /**
     * state | count
     */
    private static final String FORMAT_COUNTS = "%11s | %15s";

    /**
     * Date format
     */
    private static final String DATE_FORMAT = "yyyy/MM/dd-HH:mm:ss";

    private static final DateTimeFormatter DATE_FORMATTER
          = DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(ZoneId.systemDefault());

    private static final int MAX_PARTIAL_RESULT = 10000;

    private static class Sorter implements Comparator<String> {

        private final SortOrder sortOrder;

        Sorter(SortOrder sortOrder) {
            this.sortOrder = sortOrder;
        }

        @Override
        public int compare(String o1, String o2) {
            if (o1 == null) {
                return 1;
            }

            if (o2 == null) {
                return -1;
            }

            switch (sortOrder) {
                case ASC:
                    return o1.compareTo(o2);
                case DESC:
                    return -1 * o1.compareTo(o2);
            }

            return 0;
        }
    }

    private static class PagedRequestResult extends PagedCommandResult {

        private static final long serialVersionUID = -1206477896319388141L;

        PagedRequestResult(String partialResult, long offset) {
            super(partialResult, offset);
        }

        @Override
        public String nextCommand() {
            if (command.contains("-id=")) {
                return command.replaceFirst("-id=[\\d]+", " -id=" + offset);
            }

            return command + " -id=" + offset;
        }
    }

    private static class PagedTargetResult extends PagedCommandResult {

        private static final long serialVersionUID = -1206477896319388141L;

        PagedTargetResult(String partialResult, long offset) {
            super(partialResult, offset);
        }

        @Override
        public String nextCommand() {
            if (command.contains("-offset=")) {
                return command.replaceFirst("-offset=[\\d]+", " -offset=" + offset);
            }

            return command + " -offset=" + offset;
        }
    }

    private static Long getTimestamp(String datetime) throws ParseException {
        if (datetime == null) {
            return null;
        }

        return Instant.from(DATE_FORMATTER.parse(datetime)).toEpochMilli();
    }

    private static String formatActivity(Entry<String, BulkActivityProvider> entry) {
        BulkActivityProvider provider = entry.getValue();
        return String.format(FORMAT_ACTIVITY,
              entry.getKey(),
              provider.getActivityClass(),
              provider.getTargetType(),
              provider.getMaxPermits());
    }

    private static String formatArgument(BulkActivityArgumentDescriptor descriptor) {
        return String.format(FORMAT_ARGUMENT,
              descriptor.getName(),
              descriptor.isRequired() ? "(required)" :
                    descriptor.getDefaultValue(),
              descriptor.getValueSpec(),
              descriptor.getDescription());
    }

    private static String formatTarget(BulkRequestTarget target, boolean full) {
        if (full) {
            PnfsId pnfsId = target.getPnfsId();
            FileType type = target.getType();
            return String.format(FORMAT_TARGET_FULL,
                  target.getId(),
                  target.getPid().name(),
                  DATE_FORMATTER.format(Instant.ofEpochMilli(target.getCreatedAt())),
                  DATE_FORMATTER.format(Instant.ofEpochMilli(target.getLastUpdated())),
                  target.getRuid(),
                  target.getActivity(),
                  target.getState().name(),
                  type == null ? "?" : type,
                  pnfsId == null ? "?" : pnfsId,
                  target.getPath());
        }

        return String.format(FORMAT_TARGET,
              target.getId(),
              DATE_FORMATTER.format(Instant.ofEpochMilli(target.getLastUpdated())),
              target.getRid(),
              target.getPath());
    }

    private static String formatRequest(BulkRequest request,
          BulkRequestStore store,
          RequestListOption option) {

        String uid = request.getUid();
        Optional<Subject> subject;

        String statusName = null;
        try {
            subject = store.getSubject(uid);
        } catch (BulkStorageException e) {
            subject = Optional.empty();
        }

        String user = subject.isPresent() ? BulkRequestStore.uidGidKey(subject.get()) : "?";

        long arrivalTime = 0L;
        long modifiedTime = 0L;
        Long started = null;

        BulkRequestStatusInfo statusInfo = request.getStatusInfo();
        if (statusInfo != null) {
            arrivalTime = statusInfo.getCreatedAt();
            modifiedTime = statusInfo.getLastModified();
            started = statusInfo.getStartedAt();
            statusName = statusInfo.getStatus().name();
        }

        if (statusName == null) {
            statusName = "?";
        }

        String startedAt =
              started == null ? "?" : DATE_FORMATTER.format(Instant.ofEpochMilli(started));

        switch (option) {
            case FULL:
                return String.format(FORMAT_REQUEST_FULL,
                      request.getId(),
                      DATE_FORMATTER.format(Instant.ofEpochMilli(arrivalTime)),
                      startedAt,
                      DATE_FORMATTER.format(Instant.ofEpochMilli(modifiedTime)),
                      user,
                      request.getActivity(),
                      request.getExpandDirectories(),
                      statusName,
                      request.isPrestore(),
                      request.getUrlPrefix(),
                      uid);
            case TARGET:
                return String.format(FORMAT_REQUEST_TARGET,
                      request.getUrlPrefix(),
                      uid,
                      request.getTarget());
            case SHORT:
            default:
                return String.format(FORMAT_REQUEST,
                      request.getId(),
                      DATE_FORMATTER.format(Instant.ofEpochMilli(arrivalTime)),
                      DATE_FORMATTER.format(Instant.ofEpochMilli(modifiedTime)),
                      user,
                      statusName,
                      uid);
        }
    }

    enum SortOrder {
        ASC, DESC
    }

    enum RequestListOption {
        FULL, TARGET, SHORT
    }

    abstract class FilteredRequest implements Callable<Serializable> {

        protected Set<String> ids;
        protected Set<String> activities;
        protected Set<String> owners;
        protected Set<String> urlPrefixes;
        protected Set<BulkRequestStatus> statuses;
        protected Depth depth;

        protected BulkRequestFilter rFilter;

        @Argument(usage = "The request id proper; can be a single string,"
              + " '*' for all, or a comma-delimited list.)",
              required = false)
        String uid;

        @Option(name = "id",
              usage = "Select requests with an id greater than or equal to this.")
        long id = 0L;

        @Option(name = "before",
              valueSpec = DATE_FORMAT,
              usage = "Select requests with start date-time before date-time.")
        String before;

        @Option(name = "after",
              valueSpec = DATE_FORMAT,
              usage = "Select requests with start date-time after date-time.")
        String after;

        @Option(name = "owner",
              separator = ",",
              usage = "The uid:gid(primary) of the request owner(s).")
        String[] owner;

        @Option(name = "urlPrefix",
              separator = ",",
              usage = "The url preceding the <id> part of the request identifier.")
        String[] urlPrefix;

        @Option(name = "activity",
              separator = ",",
              usage = "The request activity.")
        String[] activity;

        @Option(name = "cancelOnFailure",
              usage = "Whether the request has this set to true or false.")
        Boolean cancelOnFailure;

        @Option(name = "clearOnSuccess",
              usage = "Whether the request has this set to true or false.")
        Boolean clearOnSuccess;

        @Option(name = "clearOnFailure",
              usage = "Whether the request has this set to true or false.")
        Boolean clearOnFailure;

        @Option(name = "delayClear",
              usage = "True means the request has a non-zero value of this.")
        Boolean delayClear;

        @Option(name = "expandDirectories",
              valueSpec = "NONE|TARGETS|ALL",
              usage = "The recursion depth of the request.")
        String expandDirectories;

        @Option(name = "prestore",
              usage = "True means the request has indicated that all targets be stored "
                    + "first before processing.")
        Boolean prestore;

        @Option(name = "status",
              valueSpec = "QUEUED|STARTED|COMPLETED|CANCELLED",
              separator = ",",
              usage = "Status of the request.")
        String[] status;

        protected void configureFilters() throws ParseException {
            if (uid == null || uid.equals("*")) {
                ids = Set.of();
            } else if (uid.indexOf(",") > 0) {
                ids = Arrays.stream(uid.split(",")).collect(Collectors.toSet());
            } else {
                ids = Set.of(uid);
            }

            Long beforeStart = getTimestamp(before);
            Long afterStart = getTimestamp(after);

            activities = toSetOrNull(activity);
            if (activities != null) {
                activities = activities.stream().map(String::toUpperCase)
                      .collect(Collectors.toSet());
            }
            owners = toSetOrNull(owner);
            urlPrefixes = toSetOrNull(urlPrefix);
            if (status != null) {
                statuses = toSetOrNull(status).stream().map(String::toUpperCase)
                      .map(BulkRequestStatus::valueOf)
                      .collect(Collectors.toSet());
            }

            if (expandDirectories != null) {
                depth = Depth.valueOf(expandDirectories.toUpperCase());
            }

            rFilter = new BulkRequestFilter(beforeStart, afterStart, owners, urlPrefixes, ids,
                  activities, statuses, cancelOnFailure, clearOnSuccess, clearOnFailure, delayClear,
                  depth, prestore);
            rFilter.setId(id);
        }

        protected List<String> requestUids() throws BulkStorageException {
            /*
             *  The find() method uses inclusive matching, in the sense
             *  that it interprets null clauses as always true, not false.
             *  For some operations (such as clear or cancel), however, it
             *  may be desirable to require explicit matching, so this
             *  method provides that protection by checking first for
             *  an empty filter and allowing only a filter with some
             *  element defined to be used in connection with find().
             */
            if (emptyFilter()) {
                return Collections.EMPTY_LIST;
            }

            return requestStore
                  .find(Optional.of(rFilter), null)
                  .stream()
                  .map(BulkRequest::getUid)
                  .collect(Collectors.toList());
        }

        private boolean emptyFilter() {
            return before == null
                  && after == null
                  && owner == null
                  && urlPrefix == null
                  && uid == null
                  && activity == null
                  && clearOnFailure == null
                  && clearOnSuccess == null
                  && delayClear == null
                  && expandDirectories == null
                  && status == null;
        }
    }

    @Command(name = "activities",
          hint = "Configured activities mappings.",
          description = "Prints the activities with their settings.")
    class Activities implements Callable<String> {

        @Option(name = "sort",
              valueSpec = "ASC|DESC",
              usage = "sort the list (default = ASC).")
        String sort = SortOrder.ASC.name();

        @Override
        public String call() throws Exception {
            Sorter sorter = new Sorter(SortOrder.valueOf(sort.toUpperCase()));
            String activities = activityFactory.getProviders().entrySet()
                  .stream()
                  .map(BulkServiceCommands::formatActivity)
                  .sorted(sorter)
                  .collect(joining("\n"));
            if (activities == null) {
                return "There are no mapped activities!";
            }

            return String.format(FORMAT_ACTIVITY, "NAME", "CLASS", "TYPE", "PERMITS")
                  + "\n" + activities;
        }
    }

    @Command(name = "arguments",
          hint = "List the arguments for a given job.",
          description = "Prints the arguments.")
    class Arguments implements Callable<String> {

        @Argument(usage = "The activity to list the arguments for.")
        String activity;

        @Option(name = "sort",
              valueSpec = "ASC|DESC",
              usage = "sort the list (default = ASC).")
        String sort = SortOrder.ASC.name();

        @Override
        public String call() throws Exception {
            Sorter sorter = new Sorter(SortOrder.valueOf(sort.toUpperCase()));
            Set<BulkActivityArgumentDescriptor> descriptors = activityFactory.getProviders()
                  .get(activity)
                  .getArguments();
            String arguments = descriptors.stream()
                  .map(BulkServiceCommands::formatArgument)
                  .sorted(sorter)
                  .collect(joining("\n"));

            return String.format(FORMAT_ARGUMENT,
                  "NAME", "DEFAULT", "VALUE SPEC", "DESCRIPTION")
                  + "\n" + arguments;
        }
    }

    @Command(name = "manager signal",
          hint = "Interrupt the request manager.",
          description = "Signals and wakes up the consumer thread.")
    class ManagerSignal implements Callable<String> {

        @Override
        public String call() throws Exception {
            requestManager.signal();
            return "Woke up consumer.";
        }
    }

    @Command(name = "request cancel",
          hint = "Cancel one or more requests.",
          description = "Recursively cancels all jobs that have not yet completed or run; signals "
                + "the request manager to remove cancelled jobs. Note that because cancellation "
                + "can momentarily block, cancellation requests are run on a seperate thread. "
                + "This command requires at least one argument or option to be defined.")
    class RequestCancel extends FilteredRequest {

        @Override
        public String call() throws Exception {
            configureFilters();
            List<String> uids = requestUids();
            StringBuilder requests = new StringBuilder();
            uids.stream().forEach(id -> requests.append("\t").append(id).append("\n"));

            for (String id : uids) {
                try {
                    submissionHandler.cancelRequest(Subjects.ROOT, id);
                    requestManager.signal();
                } catch (BulkServiceException e) {
                    LOGGER.error("cancel of request {} failed: {}.", id, e.toString());
                }
            }

            return "Request manager instructed to cancel request(s):\n" + requests;
        }
    }

    @Command(name = "request cancel targets",
          hint = "Cancel one or more targets of a specific request",
          description = "Target paths passed to the request container if running; this is a NOP "
                + "if no container job corresponding to the request id exists.")
    class RequestCancelTarget implements Callable<String> {

        @Option(name = "rid",
              required = true,
              usage = "The id of the request.")
        String rid;

        @Option(name = "path",
              required = true,
              separator = ",",
              usage = "The path of the target.")
        String[] path;

        @Override
        public String call() throws Exception {
            List<String> targetPaths = List.of(path);
            requestManager.cancelTargets(rid, targetPaths);
            return "Request " + rid + ", cancelled: \n"
                  + targetPaths.stream().collect(Collectors.joining("\n"));
        }
    }

    @Command(name = "request clear",
          hint = "Clear one or more requests.",
          description = "Removes request data from store. This command requires at least one "
                + "argument or option to be defined.")
    class RequestClear extends FilteredRequest {

        @Override
        public String call() throws Exception {
            configureFilters();
            List<String> uids = requestUids();
            StringBuilder requests = new StringBuilder();
            StringBuilder errors = new StringBuilder();
            for (String id : uids) {
                try {
                    submissionHandler.clearRequest(Subjects.ROOT, id, false);
                    requestManager.signal();
                    requests.append("\t").append(id).append("\n");
                } catch (BulkServiceException e) {
                    errors.append(id).append(": ").append(e).append("\n");
                }
            }

            if (errors.length() > 0) {
                return "The following requests could not be cleared:\n" + errors;
            }

            return "Cleared:\n" + requests;
        }
    }

    @Command(name = "request info",
          hint = "Get status information on a particular request.",
          description = "Prints request status and lists the current targets with their metadata.")
    class RequestInfo implements Callable<PagedTargetResult> {

        @Argument(usage = "the id of the request.")
        String id;

        @Option(name = "offset",
              usage = "Offset into the target list:  targets must have this id or "
                    + "greater to be included (only 5K targets in the list at a time).")
        long offset = 0L;

        @Override
        public PagedTargetResult call() throws Exception {
            Subject subject = Subjects.ROOT;
            BulkRequestInfo info = requestStore.getRequestInfo(subject, id, offset);
            Long startedAt = info.getStartedAt();
            String prefix = info.getTargetPrefix();
            StringBuilder builder = new StringBuilder(id).append(":\n")
                  .append("status:           ").append(info.getStatus()).append("\n")
                  .append("arrived at:       ").append(new Timestamp(info.getArrivedAt()))
                  .append("\n")
                  .append("started at:       ")
                  .append(startedAt == null ? "?" : new Timestamp(startedAt)).append("\n")
                  .append("last modified at: ").append(new Timestamp(info.getLastModified()))
                  .append("\n")
                  .append("target prefix:    ").append(prefix == null ? "" : prefix).append("\n")
                  .append("targets:\n")
                  .append(String.format(FORMAT_TARGET_INFO, "CREATED", "STARTED", "COMPLETED",
                        "STATE", "TARGET")).append("\n");
            info.getTargets().forEach(tinfo -> builder.append(format(tinfo)).append("\n"));

            int len = info.getTargets().size();

            if (len < MAX_PARTIAL_RESULT) {
                return new PagedTargetResult(builder.toString(), EOL);
            }

            return new PagedTargetResult(builder.toString(), info.getNextId());
        }

        private String format(BulkRequestTargetInfo info) {
            Long started = info.getStartedAt();
            Long completed = info.getFinishedAt();
            StringBuilder builder = new StringBuilder().append(
                  String.format(FORMAT_TARGET_INFO, new Timestamp(info.getSubmittedAt()),
                        started == null ? "?" : new Timestamp(started),
                        completed == null ? "?" : new Timestamp(completed),
                        info.getState(), info.getTarget()));
            if (info.getErrorType() != null) {
                builder.append((String.format(FORMAT_TARGET_INFO_ERROR, info.getErrorType(),
                      info.getErrorMessage())));
            }
            return builder.toString();
        }
    }

    @Command(name = "request ls",
          hint = "List the current requests in the store.",
          description = "Optional filters can be applied.")
    class RequestLs extends FilteredRequest {

        @Option(name = "l",
              usage = "Print the full listing; otherwise just the owner, id, status and timestamps.")
        Boolean l = false;

        @Option(name = "t",
              usage = "Print the request id plus full request target string. "
                    + "Cannot be used with l option.")
        Boolean t = false;

        @Option(name = "limit",
              usage = "Return no more than this many results at a time (maximum 5000).")
        Integer limit = MAX_PARTIAL_RESULT;

        @Option(name = "count",
              usage = "Return only the number of matching requests.")
        boolean count = false;

        String partialResult;

        @Override
        public PagedRequestResult call() throws Exception {
            configureFilters();

            if (count) {
                return new PagedRequestResult(requestStore.count(rFilter) + " matching requests.",
                      EOL);
            }

            RequestListOption option = l ? RequestListOption.FULL
                  : t ? RequestListOption.TARGET : RequestListOption.SHORT;

            Collection<BulkRequest> results = requestStore.find(Optional.of(rFilter),
                  limit > MAX_PARTIAL_RESULT ? MAX_PARTIAL_RESULT : limit);
            int len = results.size();

            if (len == 0) {
                return new PagedRequestResult("No requests.", EOL);
            }

            String requests = results.stream()
                  .map(request -> formatRequest(request, requestStore, option))
                  .collect(joining("\n"));

            switch (option) {
                case FULL:
                    partialResult =
                          String.format(FORMAT_REQUEST_FULL, "ID", "ARRIVED", "STARTED",
                                "MODIFIED", "OWNER", "ACTIVITY", "DEPTH", "STATUS", "PRESTORE",
                                "URL PREF",
                                "UID") + "\n" + requests;
                    break;
                case TARGET:
                    partialResult = String.format(FORMAT_REQUEST_TARGET, "URL PREF", "ID", "TARGET")
                          + "\n" + requests;
                    break;
                case SHORT:
                default:
                    partialResult = String.format(FORMAT_REQUEST, "ID", "ARRIVED", "MODIFIED",
                          "OWNER", "STATUS", "UID") + "\n" + requests;
            }

            if (len < MAX_PARTIAL_RESULT) {
                return new PagedRequestResult(partialResult, EOL);
            }

            long offset = results.toArray(BulkRequest[]::new)[len - 1].getId();
            return new PagedRequestResult(partialResult, offset);
        }
    }

    @Command(name = "request policy",
          hint = "Change service policy limits.",
          description = "Allows modification of policy configuration without requiring domain restart.")
    class RequestPolicy implements Callable<String> {

        @Option(name = "maxConcurrentRequests",
              usage = "Maximum number of requests that can be processed at a time.")
        Integer maxConcurrentRequests;

        @Option(name = "maxRequestsPerUser",
              usage =
                    "Maximum number of queued or active requests a user may have at a given time. "
                          + "Requests which have completed but have not been cleared/deleted are not counted.")
        Integer maxRequestsPerUser;

        @Option(name = "maxAllowedDepth",
              valueSpec = "NONE|TARGETS|ALL",
              usage =
                    "Maximum level of directory recursion allowed. NONE = no directory expansion; "
                          + "TARGETS = process only the children of directory targets; "
                          + "ALL = full recursion.  (Requests with expandDirectories set to "
                          + "a stronger value than allowed will be immediately rejected.)")
        Depth maxAllowedDepth;

        @Option(name = "maxFlatTargets",
              usage = "Maximum number of targets a request can contain if the expandDirectories "
                    + "attribute of the request = NONE.")
        Integer maxFlatTargets;

        @Option(name = "maxShallowTargets",
              usage = "Maximum number of targets a request can contain if the expandDirectories "
                    + "attribute of the request = TARGETS.")
        Integer maxShallowTargets;

        @Option(name = "maxRecursiveTargets",
              usage = "Maximum number of targets a request can contain if the expandDirectories "
                    + "attribute of the request = ALL.")
        Integer maxRecursiveTargets;

        @Override
        public String call() {
            if (maxConcurrentRequests != null) {
                requestManager.setMaxActiveRequests(maxConcurrentRequests);
            }

            if (maxRequestsPerUser != null) {
                service.setMaxRequestsPerUser(maxRequestsPerUser);
            }

            if (maxAllowedDepth != null) {
                service.setAllowedDepth(maxAllowedDepth);
            }

            if (maxFlatTargets != null) {
                service.setMaxFlatTargets(maxFlatTargets);
            }

            if (maxShallowTargets != null) {
                service.setMaxShallowTargets(maxShallowTargets);
            }

            if (maxRecursiveTargets != null) {
                service.setMaxRecursiveTargets(maxRecursiveTargets);
            }

            return new StringBuilder().append(
                        String.format(FORMAT_REQUEST_POLICY, "Maximum concurrent (active) requests",
                              requestManager.getMaxActiveRequests()))
                  .append(String.format(FORMAT_REQUEST_POLICY, "Maximum requests per user",
                        service.getMaxRequestsPerUser()))
                  .append(String.format(FORMAT_REQUEST_POLICY, "Maximum expansion depth",
                        service.getAllowedDepth()))
                  .append(String.format(FORMAT_REQUEST_POLICY, "Maximum flat targets",
                        service.getMaxFlatTargets()))
                  .append(String.format(FORMAT_REQUEST_POLICY, "Maximum shallow targets",
                        service.getMaxShallowTargets()))
                  .append(String.format(FORMAT_REQUEST_POLICY, "Maximum recursive targets",
                        service.getMaxRecursiveTargets())).toString();
        }
    }


    @Command(name = "request reset",
          hint = "Reset requests to be rerun.",
          description =
                "Sets status back to QUEUED, zeros out status counts and removes all targets.")
    class RequestReset extends FilteredRequest {

        @Override
        public String call() throws Exception {
            configureFilters();
            List<String> uids = requestUids();
            StringBuilder requests = new StringBuilder();
            for (String id : uids) {
                executor.submit(()-> {
                    try {
                        requestStore.reset(id);
                    } catch (BulkStorageException e) {
                        LOGGER.error("could not reset {}: {}.", id, e.toString());
                    }
                });
                requests.append("\t").append(id).append("\n");
            }

            return "Resetting:\n" + requests + "\nCheck pinboard for any errors.";
        }
    }

    @Command(name = "request submit",
          hint = "Launch a bulk request.",
          description = "Command-line version of the RESTful request.")
    class RequestSubmit implements Callable<String> {

        @Argument(usage = "Comma-separated list of paths of the root directory "
              + "from which to run the command.")
        String target;

        @Option(name = "activity",
              required = true,
              usage = "Name of activity to run (must be mapped; use 'activities' to see the "
                    + "current list).")
        String activity;

        @Option(name = "expandDirectories",
              valueSpec = "NONE|TARGETS|ALL",
              usage = "NONE = do not expand directories; TARGETS = shallow expansion of "
                    + "directories (only take action on its targets with no further expansion); "
                    + "ALL = full recursion. Recursion is done depth-first.")
        String expand = "NONE";

        @Option(name = "cancelOnFailure",
              usage = "Cancel request preemptively on first failure.")
        Boolean cancelOnFailure = false;

        @Option(name = "clearOnFailure",
              usage = "Remove request from storage if any targets failed.")
        Boolean clearOnFailure = false;

        @Option(name = "clearOnSuccess",
              usage = "Remove request from storage if all targets succeeded.")
        Boolean clearOnSuccess = false;

        @Option(name = "prestore",
              usage =
                    "Store all targets first before performing the activity on them. (This applies "
                          + "to recursive as well as non-recursive, and usually results in significantly "
                          + "lower throughput.)")
        Boolean prestore = false;

        @Option(name = "arguments",
              usage = "Optional comma-delimited list of name:value strings specific to the activity.")
        String arguments;

        @Override
        public String call() {
            Subject subject = Subjects.ROOT;
            Restriction restriction = Restrictions.none();
            BulkRequest request = new BulkRequest();
            request.setUrlPrefix("ssh://admin");
            request.setTarget(new ArrayList<>(Arrays.asList(target.split(","))));
            request.setActivity(activity.toUpperCase());
            request.setCancelOnFailure(cancelOnFailure);
            request.setClearOnSuccess(clearOnSuccess);
            request.setClearOnFailure(clearOnFailure);
            request.setExpandDirectories(Depth.valueOf(expand.toUpperCase()));
            request.setUid(UUID.randomUUID().toString());
            request.setPrestore(activity.equalsIgnoreCase("STAGE") || prestore);

            if (arguments != null) {
                request.setArguments(Splitter.on(',')
                      .trimResults()
                      .omitEmptyStrings()
                      .withKeyValueSeparator(':')
                      .split(arguments));
            }

            BulkRequestMessage message = new BulkRequestMessage(request, restriction);
            message.setSubject(subject);

            service.messageArrived(message);

            return "Sent message to service to submit " + request.getUrlPrefix() + "/"
                  + request.getUid();
        }
    }

    @Command(name = "request owners",
          hint = "Get owner information for requests submitted since start.",
          description = "Prints uid:gid owner counts.")
    class RequestOwners implements Callable<String> {

        @Override
        public String call() {
            return statistics.getOwnerCounts();
        }
    }

    @Command(name = "target cancel",
          hint = "Cancel a job bound to a single target.",
          description = "Signals the manager to cancel a single target.")
    class TargetCancel implements Callable<String> {

        @Argument(usage = "The target id.")
        long id;

        @Override
        public String call() throws Exception {
            Optional<BulkRequestTarget> target = targetStore.getTarget(id);
            if (!target.isPresent()) {
                return "target " + id + " not found.";
            }
            requestManager.cancel(target.get());
            return "target " + id + " cancelled.";
        }
    }

    @Command(name = "target info",
          hint = "For a single target.",
          description = "Prints out the target metadata.")
    class TargetInfo implements Callable<String> {

        @Argument(usage = "The target id.")
        long id;

        @Override
        public String call() throws Exception {
            Optional<BulkRequestTarget> optional = targetStore.getTarget(id);
            if (!optional.isPresent()) {
                return "target " + id + " not found.";
            }

            BulkRequestTarget target = optional.get();
            StringBuilder builder = new StringBuilder(formatTarget(target, true)).append("\n");
            Throwable t = target.getThrowable();
            if (t != null) {
                builder.append("ERROR: ").append(t.getClass().getCanonicalName()).append(" : ")
                      .append(t.getMessage()).append("\n");
            }
            return builder.toString();
        }
    }

    @Command(name = "target ls",
          hint = "List the current target in the store.",
          description = "Optional filters can be applied.")
    class TargetLs implements Callable<PagedTargetResult> {

        @Option(name = "l", usage = "Print the full listing.")
        Boolean l = false;

        @Option(name = "offset",
              usage = "Print only those targets with an id greater than "
                    + "or equal to this.")
        long offset = 0L;

        @Option(name = "pid",
              separator = ",",
              valueSpec = "ROOT|INITIAL|DISCOVERED",
              usage = "Node type of the target.")

        String[] pid;

        @Option(name = "rid",
              separator = ",",
              usage = "Ids of the request(s) to which the target belongs.")
        String[] rid;

        @Option(name = "pnfsid",
              separator = ",",
              usage = "The pnfsid of the target.")
        String[] pnfsid;

        @Option(name = "activity",
              separator = ",",
              usage = "The activity of the target/request.")
        String[] activity;

        @Option(name = "state",
              separator = ",",
              valueSpec = "CREATED|READY|RUNNING|CANCELLED|COMPLETED|FAILED|ALL",
              usage = "Current state of the target.")
        String[] state;

        @Option(name = "type",
              separator = ",",
              valueSpec = "DIR|REGULAR|LINK|SPECIAL",
              usage = "File type of the target.")
        String[] type;

        @Option(name = "limit",
              usage = "Return no more than this many results at a time (maximum 5000).")
        Integer limit = MAX_PARTIAL_RESULT;

        @Option(name = "count",
              usage = "Return only the number of matching targets; will automatically display "
                    + "counts grouped by states if states are provided.")
        boolean count = false;

        String partialResult;

        @Override
        public PagedTargetResult call() throws Exception {
            Set<String> rids = toSetOrNull(rid);
            Set<String> activities = toSetOrNull(activity);
            if (activities != null) {
                activities = activities.stream().map(String::toUpperCase)
                      .collect(Collectors.toSet());
            }
            Set<String> types = toSetOrNull(type);
            if (types != null) {
                types = types.stream().map(String::toUpperCase).collect(Collectors.toSet());
            }
            Set<String> pnfsids = toSetOrNull(pnfsid);
            Set<String> pids = toSetOrNull(pid);
            Set<Integer> nodeType = null;
            if (pids != null) {
                nodeType = pids.stream().map(String::toUpperCase).map(PID::valueOf)
                      .map(PID::ordinal).collect(Collectors.toSet());
            }

            Set<State> states = null;
            if (state != null) {
                if (Arrays.asList(state).contains("ALL")) {
                    states = Arrays.stream(State.values()).collect(Collectors.toSet());
                } else {
                    states = toSetOrNull(state).stream().map(String::toUpperCase)
                          .map(State::valueOf).collect(Collectors.toSet());
                }
            }

            BulkTargetFilter filter = new BulkTargetFilter(rids, offset, nodeType, pnfsids,
                  activities, types, states);

            if (count) {
                if (states != null) {
                    boolean excludeRoot = nodeType != null && !nodeType.contains(PID.ROOT.ordinal());
                    return new PagedTargetResult(
                          String.format(FORMAT_COUNTS + "\n", "STATE", "COUNT") +
                                targetStore.counts(filter, excludeRoot, "state").entrySet().stream()
                                      .map(e -> String.format(FORMAT_COUNTS, e.getKey(),
                                            e.getValue()))
                                      .collect(joining("\n")), EOL);
                }
                return new PagedTargetResult(targetStore.count(filter) + " matching targets.",
                      EOL);
            }

            List<BulkRequestTarget> results = targetStore.find(filter,
                  limit > MAX_PARTIAL_RESULT ? MAX_PARTIAL_RESULT : limit);
            int len = results.size();

            if (len == 0) {
                return new PagedTargetResult("No targets.", EOL);
            }

            String targets = results.stream().map(target -> formatTarget(target, l))
                  .collect(joining("\n"));

            String header =
                  l ? String.format(FORMAT_TARGET_FULL, "ID", "PID", "CREATED", "UPDATED", "REQUEST",
                        "ACTIVITY", "STATE", "TYPE", "PNFSID", "PATH") :
                        String.format(FORMAT_TARGET, "ID", "STARTED", "REQUEST", "PATH");

            partialResult = header + "\n" + targets;

            if (len < MAX_PARTIAL_RESULT) {
                return new PagedTargetResult(partialResult, EOL);
            }

            offset = results.get(len - 1).getId() + 1;
            return new PagedTargetResult(partialResult, offset);
        }
    }

    private BulkService service;
    private BulkRequestManager requestManager;
    private BulkSubmissionHandler submissionHandler;
    private BulkRequestStore requestStore;
    private BulkActivityFactory activityFactory;
    private BulkTargetStore targetStore;
    private BulkServiceStatistics statistics;
    private ExecutorService executor;

    @Required
    public void setActivityFactory(BulkActivityFactory activityFactory) {
        this.activityFactory = activityFactory;
    }

    @Required
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Required
    public void setRequestManager(BulkRequestManager requestManager) {
        this.requestManager = requestManager;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setService(BulkService service) {
        this.service = service;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    @Required
    public void setSubmissionHandler(BulkSubmissionHandler submissionHandler) {
        this.submissionHandler = submissionHandler;
    }

    @Required
    public void setTargetStore(BulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }
}
