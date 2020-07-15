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
package org.dcache.services.bulk.admin;

import static org.dcache.services.bulk.admin.BulkServiceCommands.RequestListOption.FULL;
import static org.dcache.services.bulk.admin.BulkServiceCommands.RequestListOption.SHORT;
import static org.dcache.services.bulk.admin.BulkServiceCommands.RequestListOption.TARGET;
import static org.dcache.services.bulk.admin.BulkServiceCommands.SortOrder.ASC;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.services.bulk.BulkFailures;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestMessage;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.BulkRequestStorageException;
import org.dcache.services.bulk.BulkService;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.handlers.BulkRequestHandler;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkJob.State;
import org.dcache.services.bulk.job.BulkJobArgumentDescriptor;
import org.dcache.services.bulk.job.BulkJobFactory;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.BulkJobProvider;
import org.dcache.services.bulk.store.BulkJobStore;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Collects all admin shell commands for convenience.
 * <p/>
 * See further individual command annotations for details.
 */
public class BulkServiceCommands implements CellCommandListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkServiceCommands.class);

  /**
   * arrived | modified | owner | activity | depth | status (targets, processed, failures):
   * urlPrefix/id
   */
  private static final String FORMAT_REQUEST_FULL
      = "%-19s | %19s | %12s | %20s | %7s | %10s (%10s, %10s, %10s): %s/%s";

  /**
   * (urlPrefix/id):   target string
   */
  private static final String FORMAT_REQUEST_TARGET = "(%s/%s):    %s";

  /**
   * arrived | modified | owner | status | id
   */
  private static final String FORMAT_REQUEST = "%-19s | %19s | %12s | %10s | %s";

  /**
   * type | path & message
   */
  private static final String FORMAT_FAILURE = "%-50s | %s";

  /**
   * started | owner | activity | type | state | key : target
   */
  private static final String FORMAT_JOB_FULL = "%-19s | %12s | %20s | %20s | %15s | %s : %s";

  /**
   * started | owner | key
   */
  private static final String FORMAT_JOB = "%-19s | %12s | %s";

  /**
   * name | class | target | expansion
   */
  private static final String FORMAT_ACTIVITY = "%-20s | %60s | %5s | %s";

  /**
   * name | required | description
   */
  private static final String FORMAT_ARGUMENT = "%-20s | %15s | %30s |  %s";

  /**
   * Date format
   */
  private static final String DATE_FORMAT = "yyyy/MM/dd-HH:mm:ss";

  private static final DateTimeFormatter DATE_FORMATTER
      = DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(ZoneId.systemDefault());
  private ExecutorService cancelExecutor;
  private BulkService service;
  private BulkRequestHandler handler;
  private BulkJobFactory jobFactory;
  private BulkJobStore jobStore;

  private static Long getTimestamp(String datetime) throws ParseException {
    if (datetime == null) {
      return null;
    }

    return Instant.from(DATE_FORMATTER.parse(datetime)).toEpochMilli();
  }

  @Required
  public void setCancelExecutor(ExecutorService cancelExecutor) {
    this.cancelExecutor = cancelExecutor;
  }

  @Required
  public void setJobFactory(BulkJobFactory jobFactory) {
    this.jobFactory = jobFactory;
  }

  @Required
  public void setJobStore(BulkJobStore jobStore) {
    this.jobStore = jobStore;
  }

  @Required
  public void setRequestHandler(BulkRequestHandler handler) {
    this.handler = handler;
  }

  @Required
  public void setService(BulkService service) {
    this.service = service;
  }

  private String formatActivity(Entry<String, BulkJobProvider> entry) {
    BulkJobProvider provider = entry.getValue();
    return String.format(FORMAT_ACTIVITY,
                         entry.getKey(),
                         provider.getJobClass(),
                         provider.getTargetType(),
                         provider.getExpansionAlgorithm());
  }

  private String formatArgument(BulkJobArgumentDescriptor descriptor) {
    return String.format(FORMAT_ARGUMENT,
                         descriptor.getName(),
                         descriptor.isRequired() ? "(required)" :
                         descriptor.getDefaultValue(),
                         descriptor.getValueSpec(),
                         descriptor.getDescription());
  }

  private String formatFailures(String id, BulkRequestStatus status) {
    BulkFailures failures = status.getFailures();

    if (failures == null) {
      return "No failures for " + id;
    }

    Map<String, List<String>> map = failures.cloneFailures();

    if (map == null) {
      return "No failures for " + id;
    }

    StringBuilder sb = new StringBuilder(String.format(FORMAT_FAILURE,
        "KEY",
        "PATH & MESSAGE"));

    for (Entry<String, List<String>> entry : map.entrySet()) {
      String key = entry.getKey();
      entry.getValue().stream()
                      .forEach(path -> sb.append("\n").append(String.format(FORMAT_FAILURE,
                                                                            key, path)));
    }

    return sb.toString();
  }

  private String formatJob(BulkJob job, boolean full) {
    Subject subject = job.getSubject();

    String user = subject != null ? BulkRequestStore.uidGidKey(subject) : "?";

    if (full) {
      return String.format(FORMAT_JOB_FULL,
                           DATE_FORMATTER.format(Instant.ofEpochMilli(job.getStartTime())),
                           user,
                           job.getActivity(),
                           job.getClass().getSimpleName(),
                           job.getState().name(),
                           job.getKey(),
                           job.getTarget());
    }

    return String.format(FORMAT_JOB,
                         DATE_FORMATTER.format(Instant.ofEpochMilli(job.getStartTime())),
                         user,
                         job.getKey());
  }

  private String formatRequest(BulkRequest request,
      BulkRequestStore store,
      RequestListOption option) {
    String requestId = request.getId();
    Optional<BulkRequestStatus> statusOptional;
    Optional<Subject> subject;

    String statusName = null;
    int targets = 0;
    int processed = 0;
    int failures = 0;

    try {
      statusOptional = store.getStatus(requestId);
    } catch (BulkRequestStorageException e) {
      statusOptional = Optional.empty();
    }

    try {
      subject = store.getSubject(requestId);
    } catch (BulkRequestStorageException e) {
      subject = Optional.empty();
    }

    String user = subject.isPresent() ? BulkRequestStore.uidGidKey(subject.get()) : "?";

    long arrivalTime = 0L;
    long modifiedTime = 0L;

    if (statusOptional.isPresent()) {
      BulkRequestStatus bulkRequestStatus = statusOptional.get();
      arrivalTime = bulkRequestStatus.getFirstArrived();
      modifiedTime = bulkRequestStatus.getLastModified();
      Status status = bulkRequestStatus.getStatus();
      statusName = status == null ? null : status.name();
      targets = bulkRequestStatus.getTargets();
      processed = bulkRequestStatus.getProcessed();
      BulkFailures bulkFailures = bulkRequestStatus.getFailures();
      if (bulkFailures != null) {
        failures = bulkFailures.count();
      }
    }

    if (statusName == null) {
      statusName = "?";
    }

    switch (option) {
      case FULL:
        return String.format(FORMAT_REQUEST_FULL,
                             DATE_FORMATTER.format(Instant.ofEpochMilli(arrivalTime)),
                             DATE_FORMATTER.format(Instant.ofEpochMilli(modifiedTime)),
                             user,
                             request.getActivity(),
                             request.getExpandDirectories(),
                             statusName,
                             targets,
                             processed,
                             failures,
                             request.getUrlPrefix(),
                             requestId);
      case TARGET:
        return String.format(FORMAT_REQUEST_TARGET,
                             request.getUrlPrefix(),
                             requestId,
                             request.getTarget());
      case SHORT:
      default:
        return String.format(FORMAT_REQUEST,
                             DATE_FORMATTER.format(Instant.ofEpochMilli(arrivalTime)),
                             DATE_FORMATTER.format(Instant.ofEpochMilli(modifiedTime)),
                             user,
                             statusName,
                             requestId);
    }
  }

  private Predicate<BulkJob> getJobFilter(String owner,
                                          String requestId,
                                          String jobId,
                                          Set<String> activity,
                                          Set<String> type,
                                          Set<State> states)
      throws ClassNotFoundException {
    List<Class> classes = new ArrayList<>();
    if (type != null) {
      for (String t : type) {
        classes.add(Class.forName(t));
      }
    }

    Pattern[] p = {owner == null ? null : Pattern.compile(owner),
        requestId == null ? null : Pattern.compile(requestId),
        jobId == null ? null : Pattern.compile(jobId)};

    Predicate<BulkJob> matchesOwner = (job) -> p[0] == null
        || p[0].matcher(Strings.nullToEmpty(BulkRequestStore.uidGidKey(job.getSubject()))).find();

    Predicate<BulkJob> matchesRequestId = (job) -> p[1] == null
        || p[1].matcher(Strings.nullToEmpty(job.getKey().getRequestId())).find();

    Predicate<BulkJob> matchesJobId = (job) -> p[2] == null
        || p[2].matcher(String.valueOf(job.getKey().getJobId())).find();

    Predicate<BulkJob> matchesActivity = (job) -> activity == null
        || activity.isEmpty()
        || activity.contains(Strings.nullToEmpty(job.getActivity()));

    Predicate<BulkJob> matchesType = (job) -> type == null
        || type.isEmpty()
        || classes.stream().filter(c -> c.isAssignableFrom(job.getClass())).findFirst().isPresent();

    Predicate<BulkJob> matchesState = (job) -> states == null
        || states.isEmpty() || states.contains(job.getState());

    return matchesOwner.and(matchesRequestId)
        .and(matchesJobId)
        .and(matchesActivity)
        .and(matchesType)
        .and(matchesState);
  }

  private Predicate<BulkRequest> getRequestFilter(Long before,
                                                  Long after,
                                                  String owner,
                                                  String urlPrefix,
                                                  String id,
                                                  String target,
                                                  String targetPrefix,
                                                  Set<String> activity,
                                                  Boolean cancelOnFailure,
                                                  Boolean clearOnSuccess,
                                                  Boolean clearOnFailure,
                                                  Boolean delayClear,
                                                  Depth expandDirectories) {
    Pattern[] p = {owner == null ? null : Pattern.compile(owner),
                   urlPrefix == null ? null : Pattern.compile(urlPrefix),
                   id == null ? null : Pattern.compile(id),
                   target == null ? null : Pattern.compile(target),
                   targetPrefix == null ? null : Pattern.compile(targetPrefix)};

    Predicate<BulkRequest> matchesBefore = (request) -> {
      if (before == null) {
        return true;
      }

      try {
        Optional<BulkRequestStatus> status = service.getRequestStore().getStatus(request.getId());
        if (!status.isPresent()) {
          return true;
        }

        return status.get().getFirstArrived() < before;
      } catch (BulkServiceException e) {
        return false;
      }
    };

    Predicate<BulkRequest> matchesAfter = (request) -> {
      if (after == null) {
        return true;
      }

      try {
        Optional<BulkRequestStatus> status = service.getRequestStore().getStatus(request.getId());
        if (!status.isPresent()) {
          return true;
        }
        return status.get().getFirstArrived() >= after;
      } catch (BulkServiceException e) {
        return false;
      }
    };

    Predicate<BulkRequest> matchesOwner = (request) -> {
      try {
        if (p[0] == null) {
          return true;
        }

        Optional<Subject> subject = service.getRequestStore().getSubject(request.getId());

        if (!subject.isPresent()) {
          return true;
        }

        return p[0].matcher(BulkRequestStore.uidGidKey(subject.get())).find();
      } catch (BulkServiceException e) {
        return false;
      }
    };

    Predicate<BulkRequest> matchesUrlPrefix = (request) -> p[1] == null
        || p[1].matcher(Strings.nullToEmpty(request.getUrlPrefix())).find();

    Predicate<BulkRequest> matchesId = (request) -> p[2] == null
        || p[2].matcher(Strings.nullToEmpty(request.getId())).find();

    Predicate<BulkRequest> matchesTarget = (request) -> p[3] == null
        || p[3].matcher(Strings.nullToEmpty(request.getTarget())).find();

    Predicate<BulkRequest> matchesTargetPrefix = (request) -> p[4] == null
        || p[4].matcher(Strings.nullToEmpty(request.getTargetPrefix())).find();

    Predicate<BulkRequest> matchesActivity = (request) -> activity == null
        || activity.isEmpty()
        || activity.contains(Strings.nullToEmpty(request.getActivity()));

    Predicate<BulkRequest> matchesDepth = (request) -> expandDirectories == null
        || expandDirectories == request.getExpandDirectories();

    Predicate<BulkRequest> matchesCancelOnFailure = (request) -> cancelOnFailure == null
        || cancelOnFailure == request.isCancelOnFailure();

    Predicate<BulkRequest> matchesClearOnSuccess = (request) -> clearOnSuccess == null
        || clearOnSuccess == request.isClearOnSuccess();

    Predicate<BulkRequest> matchesClearOnFailure = (request) -> clearOnFailure == null
        || clearOnFailure == request.isClearOnFailure();

    Predicate<BulkRequest> matchesDelay = (request) -> delayClear == null
        || delayClear == request.getDelayClear() > 0;

    return matchesBefore.and(matchesAfter)
                        .and(matchesOwner)
                        .and(matchesUrlPrefix)
                        .and(matchesId)
                        .and(matchesTarget)
                        .and(matchesTargetPrefix)
                        .and(matchesActivity)
                        .and(matchesDepth)
                        .and(matchesCancelOnFailure)
                        .and(matchesClearOnSuccess)
                        .and(matchesClearOnFailure)
                        .and(matchesDelay);
  }

  private Predicate<BulkRequestStatus> getRequestStatusFilter(Set<Status> statuses) {
    return (status) -> statuses == null || statuses.isEmpty()
        || statuses.contains(status.getStatus());
  }

  enum SortOrder {
    ASC, DESC
  }

  enum RequestListOption {
    FULL, TARGET, SHORT
  }

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

  abstract class FilteredRequest implements Callable<String> {
    protected Set<String> activitySet;
    protected Depth depth;
    protected Predicate<BulkRequest> rFilter;
    protected Set<Status> statusSet;
    protected Predicate<BulkRequestStatus> sFilter;

    @Argument(usage = "the request id proper; this can be a regex (including the use of 'or' = '|')",
              required = false)
    String id;

    @Option(name = "before",
            valueSpec = DATE_FORMAT,
            usage = "select requests with start date-time before date-time.")
    String before;

    @Option(name = "after",
            valueSpec = DATE_FORMAT,
            usage = "select requests with start date-time after date-time.")
    String after;

    @Option(name = "owner",
            usage = "the uid:gid(primary) of the request owner(s); "
            + "this can be a regex (including the use of 'or' = '|'")
    String owner;

    @Option(name = "urlPrefix",
            usage = "the url preceding the <id> part of the request identifier; "
            + "this can be a regex (including the use of 'or' = '|'")
    String urlPrefix;

    @Option(name = "target",
            usage = "the request target; this can be a regex (including the use of 'or' = '|'")
    String target;

    @Option(name = "targetPrefix",
            usage = "the request targetPrefix; this can be a regex (including the use of 'or' = '|'")
    String targetPrefix;

    @Option(name = "activity",
            separator = ",",
            usage = "the request activity; multiple activities should be expressed as "
            + "a comma-delimited list")
    String[] activity;

    @Option(name = "cancelOnFailure",
            usage = "whether the request has this set to true or false")
    Boolean cancelOnFailure;

    @Option(name = "clearOnSuccess",
            usage = "whether the request has this set to true or false")
    Boolean clearOnSuccess;

    @Option(name = "clearOnFailure",
            usage = "whether the request has this set to true or false")
    Boolean clearOnFailure;

    @Option(name = "delayClear",
            usage = "true means the request has a non-zero value of this")
    Boolean delayClear;

    @Option(name = "expandDirectories",
            valueSpec = "NONE|TARGETS|ALL",
            usage = "the recursion depth of the request.")
    String expandDirectories;

    @Option(name = "status",
            valueSpec = "QUEUED|STARTED|COMPLETED|CANCELLED",
            separator = ",",
            usage = "job status; multiple statuses should be expressed as a comma-delimited list")
    String[] status;

    protected void configureFilters() throws ParseException {
      Long beforeStart = getTimestamp(before);
      Long afterStart = getTimestamp(after);

      activitySet = activity == null ? null : Arrays.stream(activity).collect(Collectors.toSet());

      if (expandDirectories != null) {
        depth = Depth.valueOf(expandDirectories.toUpperCase());
      }

      rFilter = getRequestFilter(beforeStart,
                                 afterStart,
                                 owner,
                                 urlPrefix,
                                 id,
                                 target,
                                 targetPrefix,
                                 activitySet,
                                 cancelOnFailure,
                                 clearOnSuccess,
                                 clearOnFailure,
                                 delayClear,
                                 depth);

      statusSet = status == null ? null : Arrays.stream(status)
                                                .map(String::toUpperCase)
                                                .map(Status::valueOf)
                                                .collect(Collectors.toSet());

      sFilter = getRequestStatusFilter(statusSet);
    }

    /*
     *  The find() method uses inclusive matching, in the sense
     *  that it interprets null clauses as always true, not false.
     *  For some operations (such as clear or cancel), however, it
     *  may be desirable to require explicit matching, so this
     *  method provides that protection by checking first for
     *  an empty filter and allowing only a filter with some
     *  element defined to be used in connection with find().
     */
    protected List<String> requestIds() throws BulkRequestStorageException {
      if (emptyFilter()) {
        return Collections.EMPTY_LIST;
      }

      return service.getRequestStore()
                    .find(Optional.of(rFilter), Optional.of(sFilter),null)
                    .stream()
                    .map(BulkRequest::getId)
                    .collect(Collectors.toList());
    }

    private boolean emptyFilter() {
      return before == null
          && after == null
          && owner == null
          && urlPrefix == null
          && id == null
          && target == null
          && activity == null
          && clearOnFailure == null
          && clearOnSuccess == null
          && delayClear == null
          && expandDirectories == null
          && status == null;
    }
  }

  @Command(name = "activities",
           hint = "configured activities mappings.",
           description = "Prints the activities with their settings.  Output has the form: "
            + "'name | class | target | expansion'. For the arguments to a given activity, "
            + "use the 'arguments' command.")
  class Activities implements Callable<String> {
    @Option(name = "sort",
            valueSpec = "ASC|DESC",
            usage = "sort the list (default = ASC)")
    String sort = ASC.name();

    @Override
    public String call() throws Exception {
      Sorter sorter = new Sorter(SortOrder.valueOf(sort.toUpperCase()));
      String activities = jobFactory.getProviders().entrySet()
                                    .stream()
                                    .map(BulkServiceCommands.this::formatActivity)
                                    .sorted(sorter)
                                    .collect(Collectors.joining("\n"));
      if (activities == null) {
        return "There are no mapped activities!";
      }

      return String.format(FORMAT_ACTIVITY, "NAME", "CLASS", "TARGET", "EXPANSION")
             + "\n" + activities;
    }
  }

  @Command(name = "arguments",
           hint = "list the arguments for a given job.",
           description = "Prints the arguments.  Output has the form: 'name | default/required "
            + "| value spec | description'.")
  class Arguments implements Callable<String> {
    @Argument(usage = "The activity to list the arguments for.")
    String activity;

    @Option(name = "sort",
            valueSpec = "ASC|DESC",
            usage = "sort the list (default = ASC)")
    String sort = ASC.name();

    @Override
    public String call() throws Exception {
      Sorter sorter = new Sorter(SortOrder.valueOf(sort.toUpperCase()));
      Set<BulkJobArgumentDescriptor> descriptors = jobFactory.getProviders()
                                                             .get(activity)
                                                             .getArguments();
      String arguments = descriptors.stream()
                                    .map(BulkServiceCommands.this::formatArgument)
                                    .sorted(sorter)
                                    .collect(Collectors.joining("\n"));

      return String.format(FORMAT_ARGUMENT,
          "NAME", "DEFAULT", "VALUE SPEC", "DESCRIPTION")
          + "\n" + arguments;
    }
  }

  @Command(name = "job cancel",
           hint = "cancel a single job",
           description = "Signals the queue to remove the job.  This command should be rarely "
            + "necessary, as its use most likely indicates something is wrong systemically.")
  class JobCancel implements Callable<String> {
    @Argument(usage = "The job key string, which has the form: requestId::jobId.")
    String keyString;

    @Override
    public String call() throws Exception {
      BulkJobKey key = BulkJobKey.parse(keyString);
      Optional<BulkJob> job = jobStore.getJob(key);
      if (job.isPresent()) {
        job.get().cancel();
        return "job " + key.toString() + " cancelled.";
      } else {
        return "job " + key.toString() + " not found.";
      }
    }
  }

  @Command(name = "job ls",
           hint = "list the current jobs in the store",
           description = "Optional filters can be applied. Output has the form: "
               + "'owner | activity | type | state | key : target'")
  class JobLs implements Callable<String> {
    @Option(name = "l",
            usage = "print the full listing; otherwise just the owner, id and time started.")
    Boolean l = false;

    @Option(name = "owner",
            usage = "the uid:gid(primary) of the request owner(s); this can be a regex "
                + "(including the use of regex 'or' = '|'")
    String owner;

    @Option(name = "requestId",
            usage = "the id of the request to which the job belongs; this can be a regex "
                + "(including the use of regex 'or' = '|'")
    String requestId;

    @Option(name = "jobId",
            usage = "the id of the job proper; this can be a regex (including the "
                + "use of regex 'or' = '|'")
    String jobId;

    @Option(name = "activity",
            separator = ",",
            usage = "the request activity; multiple activities should be expressed as "
                + "a comma-delimited list")
    String[] activity;

    @Option(name = "type",
            separator = ",",
            usage = "the fully qualified class name of the job; multiple activities should be "
                + "expressed as a comma-delimited list")
    String[] type;

    @Option(name = "state",
            separator = ",",
            valueSpec = "CREATED|INITIALIZED|STARTED|CANCELLED|COMPLETED|FAILED",
            usage = "job state; multiple states should be expressed as a comma-delimited list")
    String[] state;

    @Option(name = "sort",
            valueSpec = "ASC|DESC",
            usage = "sort the list (default = ASC)")
    String sort = ASC.name();

    @Option(name = "limit",
            usage = "return no more than this many results")
    Long limit;

    @Override
    public String call() throws Exception {
      Set<String> activitySet = activity == null
          ? null : Arrays.stream(activity).collect(Collectors.toSet());
      Set<String> typeSet = type == null ? null : Arrays.stream(type).collect(Collectors.toSet());
      Set<State> stateSet = state == null
          ? null : Arrays.stream(state)
                         .map(String::toUpperCase)
                         .map(State::valueOf)
                         .collect(Collectors.toSet());

      Sorter sorter = new Sorter(SortOrder.valueOf(sort.toUpperCase()));

      String jobs = jobStore.find(getJobFilter(owner,
                                               requestId,
                                               jobId,
                                               activitySet,
                                               typeSet,
                                               stateSet), limit)
                            .stream()
                            .map(job -> formatJob(job, l))
                            .sorted(sorter)
                            .collect(Collectors.joining("\n"));

      if (jobs.isEmpty()) {
        return "No jobs.";
      }

      String header = l ? String.format(FORMAT_JOB_FULL,
                                        "STARTED",
                                        "OWNER",
                                        "ACTIVITY",
                                        "TYPE",
                                        "STATE",
                                        "KEY",
                                        "TARGET") :
                          String.format(FORMAT_JOB,
                                        "STARTED",
                                        "OWNER",
                                        "KEY");

      return header + "\n" + jobs;
    }
  }

  @Command(name = "queue ping",
           hint = "ping the job queue.",
           description = "Signals and wakes up the consumer thread.")
  class QueuePing implements Callable<String> {
    public String call() throws Exception {
      service.getQueue().signal();
      return "Woke up queue consumer.";
    }
  }

  @Command(name = "request failures",
           hint = "list the failures for the given request",
           description = "Prints out the exception information as stored.")
  class RequestFailures implements Callable<String> {
    @Argument(usage = "The request id.")
    String id;

    public String call() throws Exception {
      Optional<BulkRequestStatus> status = service.getRequestStore().getStatus(id);
      if (!status.isPresent()) {
        return "No data for " + id;
      }

      return formatFailures(id, status.get());
    }
  }

  @Command(name = "request cancel",
           hint = "cancel one or more requests.",
           description = "Recursively cancels all jobs that have not yet completed or run; signals "
               + "the queue to remove the cancelled job. Note that because cancellation "
               + "can momentarily block, cancellation requests are run on a seperate thread. "
               + "This command requires at least one argument or option to be defined.")
  class RequestCancel extends FilteredRequest {
    public String call() throws Exception {
      configureFilters();
      List<String> ids = requestIds();
      StringBuilder requests = new StringBuilder();
      ids.stream().forEach(id -> requests.append("\t").append(id).append("\n"));

      cancelExecutor.submit(() -> {
        for (String id : ids) {
          try {
            handler.requestCancelled(id);
            service.getQueue().signal();
          } catch (BulkServiceException e) {
            LOGGER.error("cancel of request {} failed: {}.", id, e.toString());
          }
        }
      });

      return "Queue instructed to cancel request(s):\n" + requests;
    }
  }

  @Command(name = "request clear",
           hint = "clear one or more requests.",
           description = "Removes request data from store; this includes backup on disk."
               + "This command requires at least one argument or option to be defined.")
  class RequestClear extends FilteredRequest {
    public String call() throws Exception {
      configureFilters();
      List<String> ids = requestIds();
      StringBuilder requests = new StringBuilder();
      StringBuilder errors = new StringBuilder();
      for (String id : ids) {
        try {
          handler.clearRequest(Subjects.ROOT, id);
          service.getQueue().signal();
          requests.append("\t").append(id).append("\n");
        } catch (BulkServiceException e) {
          errors.append(id).append(": ").append(e.toString()).append("\n");
        }
      }

      if (errors.length() > 0) {
        return "The following requests could not be cleared:\n" + errors;
      }

      return "Cleared:\n" + requests;
    }
  }

  @Command(name = "request ls",
           hint = "list the current requests in the store",
           description = "Optional filters can be applied. Output has the form: "
               + "'owner | activity | depth | status ( T targets P processed F failures ): "
               + "urlPrefix/id'")
  class RequestLs extends FilteredRequest {
    @Option(name = "l",
            usage = "print the full listing; otherwise just the owner, id and time arrived.")
    Boolean l = false;

    @Option(name = "t",
            usage = "print the request id plus full request target string. "
                + "Cannot be used with l option.")
    Boolean t = false;

    @Option(name = "sort",
            valueSpec = "ASC|DESC",
            usage = "sort the list (default = ASC)")
    String sort = ASC.name();

    @Option(name = "limit",
            usage = "return no more than this many results")
    Long limit;

    @Override
    public String call() throws Exception {
      configureFilters();
      Sorter sorter = new Sorter(SortOrder.valueOf(sort.toUpperCase()));
      RequestListOption option = l ? FULL : t ? TARGET : SHORT;

      BulkRequestStore store = service.getRequestStore();
      String requests = store.find(Optional.of(rFilter),Optional.of(sFilter), limit)
                             .stream()
                             .map(request -> formatRequest(request, store, option))
                             .sorted(sorter)
                             .collect(Collectors.joining("\n"));

      if (requests.isEmpty()) {
        return "No requests.";
      }

      switch (option) {
        case FULL:
          return String.format(FORMAT_REQUEST_FULL,
                               "ARRIVED",
                               "MODIFIED",
                               "OWNER",
                               "ACTIVITY",
                               "DEPTH",
                               "STATUS",
                               "TARGETS",
                               "PROCESSED",
                               "FAILED",
                               "URL PREF",
                               "ID") + "\n" + requests;
        case TARGET:
          return String.format(FORMAT_REQUEST_TARGET, "URL PREF", "ID", "TARGET") + "\n" + requests;
        case SHORT:
        default:
          return String.format(FORMAT_REQUEST, "ARRIVED", "MODIFIED","OWNER", "STATUS", "ID")
              + "\n" + requests;
      }
    }
  }

  @Command(name = "request reset",
           hint = "reset requests.",
           description = "Sets status back to QUEUED and zeros out status counts; "
               + "places request back on queue.")
  class RequestReset extends FilteredRequest {
    public String call() throws Exception {
      configureFilters();
      List<String> ids = requestIds();
      StringBuilder requests = new StringBuilder();
      for (String id : ids) {
        service.getRequestStore().reset(id);
        requests.append("\t")
            .append(id)
            .append("\n");

      }

      return "Reset:\n" + requests;
    }
  }

  @Command(name = "request submit",
           hint = "launch a bulk request to print out file metadata",
           description = "Does a breadth-first bulk listing of all files recursively below "
               + "the root directory; logs at info level, so pinboard can be checked for output.")
  class RequestSubmit implements Callable<String> {
    @Argument(usage = "Path of the root directory from which to run the command.")
    String target;

    @Option(name = "activity",
            required = true,
            usage = "name of activity to run (must be currently mapped; test activities available "
                + "are 'BREADTH-FIRST-WALK' and 'DEPTH-FIRST-WALK')")
    String activity;

    @Option(name = "expand",
            valueSpec = "NONE|TARGETS|ALL",
            usage = "NONE = do not expand directories; TARGETS = shallow expansion of "
                + "directories (only take action on its targets with no further expansion); "
                + "ALL = full recursion")
    String expand = "NONE";

    @Option(name = "cancelOnFailure",
            usage = "cancel request preemptively on first failure; default is false")
    Boolean cancelOnFailure = false;

    @Option(name = "clearOnFailure",
            usage = "remove request from storage if any jobs failed; default is false")
    Boolean clearOnFailure = false;

    @Option(name = "clearOnSuccess",
            usage = "remove request from storage if all jobs succeeded; default is false)")
    Boolean clearOnSuccess = false;

    @Option(name = "delayClear",
            usage = "wait in seconds before clearing (one of the clear options must be "
                + "true for this to have effect); default = 0.")
    Integer delayClear = 0;

    @Option(name = "arguments",
            usage = "comma-delimited list of name-value strings")
    String arguments;

    @Override
    public String call() {
      Subject subject = Subjects.ROOT;
      Restriction restriction = Restrictions.none();
      BulkRequest request = new BulkRequest();
      request.setUrlPrefix("ssh://admin");
      request.setTarget(target);
      request.setActivity(activity.toUpperCase());
      request.setCancelOnFailure(cancelOnFailure);
      request.setClearOnSuccess(clearOnSuccess);
      request.setClearOnFailure(clearOnFailure);
      request.setDelayClear(delayClear);
      request.setExpandDirectories(Depth.valueOf(expand.toUpperCase()));
      request.setId(UUID.randomUUID().toString());

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

      return "sent message to service to submit " + request.getUrlPrefix() + "/" + request.getId();
    }
  }
}
