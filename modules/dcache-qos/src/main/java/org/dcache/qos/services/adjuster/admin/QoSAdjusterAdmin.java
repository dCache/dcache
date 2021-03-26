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
package org.dcache.qos.services.adjuster.admin;

import com.google.common.base.Strings;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.adjuster.data.QoSAdjusterTaskMap;
import org.dcache.qos.services.adjuster.util.QoSAdjusterCounters;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.util.InitializerAwareCommand;
import org.dcache.qos.util.MapInitializer;
import org.dcache.qos.util.MessageGuard;
import org.dcache.qos.util.QoSHistory;

public final class QoSAdjusterAdmin implements CellCommandListener {

  abstract class FilteredAdjusterTaskCommand extends InitializerAwareCommand {
    @Option(name = "action",
        valueSpec = "COPY_REPLICA|CACHE_REPLICA|PERSIST_REPLICA|WAIT_FOR_STAGE|FLUSH",
        usage = "Match only operations for files with this action type.")
    protected QoSAction action;

    @Option(name = "state",
        valueSpec = "INITIALIZED|RUNNING|CANCELLED|DONE",
        separator = ",",
        usage = "Match only operations for files matching this comma-delimited set of states; "
            + "default is RUNNING.")
    protected String[] state = {"RUNNING"};

    @Option(name = "group",
        usage = "Match only operations with this preferred pool group; use the option with no "
            + "value to match only operations without a specified group.")
    protected String poolGroup;

    @Option(name = "source",
        usage = "Match only operations with this source pool name; use the option with no value "
            + "to match only operations without a source pool.")
    protected String source;

    @Option(name = "target",
        usage = "Match only operations with this target pool name; use the option with no value "
            + "to match only operations without a target pool.")
    protected String target;

    @Option(name = "retry",
        usage = "Match only the operation with this number of retries.")
    protected Integer retry;

    @Option(name = "startedBefore",
        valueSpec = FORMAT_STRING,
        usage = "Match only operations whose start time is before this date-time.")
    protected String startedBefore;

    @Option(name = "startedAfter",
        valueSpec = FORMAT_STRING,
        usage = "Match only operations whose start time is after this date-time.")
    protected String startedAfter;

    @Argument(required = false,
        usage = "Match only activities for this comma-delimited list of pnfsids. '*' "
            + "matches all operations.")
    protected String pnfsids;

    protected FilteredAdjusterTaskCommand() {
      super(initializer);
    }

    protected Predicate<QoSAdjusterTask> getFilter() {
      Pattern grpPattern = poolGroup == null ? null : Pattern.compile(poolGroup);
      Pattern srcPattern = source == null ? null : Pattern.compile(source);
      Pattern tgtPattern = target == null ? null : Pattern.compile(target);

      Set<String> stateSet;
      if (state == null) {
        stateSet = null;
      } else {
        stateSet = Arrays.stream(state).collect(Collectors.toSet());
      }

      Set<String> pnfsIdSet;
      if (pnfsids == null || pnfsids.equals("*")) {
        pnfsIdSet = null;
      } else {
        pnfsIdSet = Arrays.stream(pnfsids.split(",")).collect(Collectors.toSet());
      }

      Long before = startedBefore == null ? null : getTimestamp(startedBefore);
      Long after = startedAfter == null ? null : getTimestamp(startedAfter);

      Predicate<QoSAdjusterTask> matchesAction = (task) -> action == null
          || action == task.getAction();

      Predicate<QoSAdjusterTask> matchesState = (task) -> stateSet == null
          || stateSet.contains(task.getStatusName());

      Predicate<QoSAdjusterTask> matchesGroup = (task) -> grpPattern == null
          || grpPattern.matcher(Strings.nullToEmpty(task.getPoolGroup())).find();

      Predicate<QoSAdjusterTask> matchesSource = (task) -> srcPattern == null
          || srcPattern.matcher(Strings.nullToEmpty(task.getSource())).find();

      Predicate<QoSAdjusterTask> matchesTarget = (task) -> tgtPattern == null
          || tgtPattern.matcher(Strings.nullToEmpty(task.getTarget())).find();

      Predicate<QoSAdjusterTask> matchesRetry = (task) -> retry == null
          || task.getRetry() == retry;

      Predicate<QoSAdjusterTask> matchesPnfsIds = (task) -> pnfsIdSet == null
          || pnfsIdSet.contains(task.getPnfsId().toString());

      Predicate<QoSAdjusterTask> matchesBefore = (task) -> before == null
          || task.getStartTime() <= before;

      Predicate<QoSAdjusterTask> matchesAfter = (task) -> after == null
          || task.getStartTime() >= after;

      return matchesBefore.and(matchesAfter)
                          .and(matchesAction)
                          .and(matchesState)
                          .and(matchesGroup)
                          .and(matchesSource)
                          .and(matchesTarget)
                          .and(matchesRetry)
                          .and(matchesPnfsIds);
    }
  }

  @Command(name = "task cancel",
      hint = "cancel adjuster tasks ",
      description = "Scans the file table and cancels tasks matching the filter parameters.")
  class TaskCancelCommand extends FilteredAdjusterTaskCommand {

    @Override
    protected String doCall() {
      if (pnfsids == null) {
        return "To cancel you must specify one or more pnfsids, or '*' for all matching pnfsids.";
      }

      taskMap.cancel(getFilter());
      return "Issued cancel command to cancel file operations.";
    }
  }

  @Command(name = "task ctrl",
      hint = "control  handling of tasks",
      description = "Turn processing of task on or off "
          + "(start/shutdown), wake up the queue, or display info relevant to queue processing. "
          + "NOTE that the max number of running tasks must be reset through properties "
          + "(with domain restart) as it is also used to initialize the thread pool.")
  class TaskControlCommand extends InitializerAwareCommand {
    @Argument(valueSpec = "START|SHUTDOWN|RESET|RUN|INFO",
        required = false,
        usage = "info = information (default); reset = reset properties; start = (re)start "
            + "processing of file operations; shutdown = stop all processing of file operations; "
            + "run = wake up queue." )
    String arg = "INFO";

    @Option(name = "sweep",
        usage = "With reset mode. Minimal interval between sweeps of the task queues.")
    Long sweep;

    @Option(name = "unit",
        valueSpec = "SECONDS|MINUTES|HOURS",
        usage = "sweep interval unit.")
    TimeUnit unit;

    @Option(name = "retries",
        usage = "Maximum number of retries on a failed operation.")
    Integer retries;

    private ControlMode mode;

    TaskControlCommand() {
      super(initializer);
    }

    @Override
    public String call() {
      mode = ControlMode.valueOf(arg.toUpperCase());
      if (mode == ControlMode.START) {
        new Thread(() -> startAll()).start();
        return "Consumer initialization started.";
      }
      return super.call();
    }

    @Override
    protected String doCall() throws Exception {
      switch (mode) {
        case SHUTDOWN:
          shutdownAll();
          return "Consumer has been shutdown.";
        case RUN:
          if (!taskMap.isRunning()) {
            return "Consumer is not running.";
          }
          taskMap.signalAll();
          return "Woke up consumer.";
        case RESET:
          if (sweep != null) {
            taskMap.setTimeout(sweep);
          }

          if (unit != null) {
            taskMap.setTimeoutUnit(unit);
          }

          if (retries != null) {
            taskMap.setMaxRetries(retries);
          }

          taskMap.signalAll();
          // fall through here
        case INFO:
        default:
          StringBuilder builder = new StringBuilder();
          taskMap.getInfo(builder);
          return builder.toString();
      }
    }
  }

  @Command(name = "task details",
           hint = "list diagnostic information concerning tasks by pool",
      description = "Gives task counts by activity and role (source, target).")
  class TaskDetailsCommand extends InitializerAwareCommand {
    TaskDetailsCommand() { super(initializer); }

    @Override
    protected String doCall() throws Exception {
      StringBuilder builder = new StringBuilder();
      counters.appendDetails(builder);
      return builder.toString();
    }
  }

  @Command(name = "task history",
      hint = "display a history of the most recent terminated file operations",
      description = "When file operations complete or are aborted, their string representations "
          + "are added to a circular buffer whose capacity is set by the property "
          + "'qos.limits.file.operation-history'.")
  class TaskHistoryCommand extends InitializerAwareCommand {
    @Argument(required = false, valueSpec = "errors", usage = "Display just the failures.")
    String errors;

    @Option(name = "limit", usage = "Display up to this number of entries.")
    Integer limit;

    @Option(name = "order", valueSpec = "ASC|DESC",
            usage = "Display entries in ascending (default) or descending order of arrival.")
    String order = "ASC";

    TaskHistoryCommand() {
      super(initializer);
    }

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

  @Command(name = "task ls",
      hint = "list entries in the adjuster task table",
      description = "Scans the table and returns tasks matching the filter parameters.")
  class TaskLsCommand extends FilteredAdjusterTaskCommand {

    @Option(name = "count", usage="Do not list, but return only the number of matches.")
    boolean count = false;

    @Option(name = "limit",
        usage = "Maximum number of rows to list.  This option becomes required when "
            + "the operation queues reach " + LS_THRESHOLD + "; be aware that "
            + "listing more than this number of rows may provoke an out of memory "
            + "error for the domain.")
    protected Integer limit;

    @Override
    protected String doCall() throws Exception {
      Predicate<QoSAdjusterTask> filter = getFilter();
      if (count) {
        return taskMap.count(filter) + " matching pnfsids";
      } else {
        if (limit == null) {
          int size = taskMap.size();
          if ((state == null || Arrays.asList(state).contains("WAITING"))
              && size >= LS_THRESHOLD) {
            return String.format(REQUIRE_LIMIT, size, size);
          }
        }
        return taskMap.list(filter, limit == null ? Integer.MAX_VALUE : limit);
      }
    }
  }

  @Command(name = "task stats", hint = "print diagnostic statistics history",
      description = "Reads in the contents of the diagnostic history file recording task statistics.")
  class TaskStatsCommand extends InitializerAwareCommand {
    @Option(name = "offset", usage = "Start at this line number.")
    Integer offset = 0;

    @Option(name = "limit", usage = "Display up to this number of lines (default is 5000).")
    Integer limit = 5000;

    @Option(name = "order", valueSpec = "asc|desc",
            usage = "Display lines in ascending (default) or descending order by timestamp.")
    String order = "asc";

    @Option(name = "enable", usage = "Turn the recording of statistics to file on or off (default).")
    Boolean enable = null;

    TaskStatsCommand() {
      super(initializer);
    }

    protected String doCall() throws Exception {
      if (enable != null) {
        counters.setToFile(enable);
        return "Recording to file is now " + (enable ? "on." : "off.");
      }

      SortOrder order = SortOrder.valueOf(this.order.toUpperCase());
      StringBuilder builder = new StringBuilder();
      counters.readStatistics(builder, offset, limit, order == SortOrder.DESC);
      return builder.toString();
    }
  }

  class NoPMapInitializer extends MapInitializer {
    NoPMapInitializer() {
      setInitialized();
    }

    public boolean initialize() {
      if (isInitialized()) {
        return false;
      }
      setInitialized();
      return true;
    }

    @Override
    public void run() {
    }

    @Override
    protected long getRefreshTimeout() {
      return 0;
    }

    @Override
    protected TimeUnit getRefreshTimeoutUnit() {
      return TimeUnit.MILLISECONDS;
    }
  }

  private final MapInitializer initializer = new NoPMapInitializer();

  private MessageGuard messageGuard;
  private QoSHistory history;
  private QoSAdjusterCounters counters;
  private QoSAdjusterTaskMap taskMap;

  public void setHistory(QoSHistory history) {
    this.history = history;
  }

  public void setCounters(QoSAdjusterCounters counters) {
    this.counters = counters;
  }

  public void setMessageGuard(MessageGuard messageGuard) {
    this.messageGuard = messageGuard;
  }

  public void setTaskMap(QoSAdjusterTaskMap taskMap) {
    this.taskMap = taskMap;
  }

  private void startAll() {
    initializer.initialize();
    if (taskMap.isRunning()) {
      taskMap.shutdown();
    }
    taskMap.initialize();
    messageGuard.enable();
  }

  private void shutdownAll() {
    if (taskMap.isRunning()) {
      taskMap.shutdown();
    }
    messageGuard.disable(true);
    initializer.shutDown();
  }
}
