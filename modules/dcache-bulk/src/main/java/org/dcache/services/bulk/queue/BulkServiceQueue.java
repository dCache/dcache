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
package org.dcache.services.bulk.queue;

import static org.dcache.services.bulk.BulkRequestStatus.Status.STARTED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.TreeMultimap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestStorageException;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.handlers.BulkRequestCompletionHandler;
import org.dcache.services.bulk.handlers.BulkSubmissionHandler;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkRequestJob;
import org.dcache.services.bulk.job.SingleTargetJob;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.util.FireAndForgetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * The heart of the bulk service, encapsulating its queueing logic.
 *
 * <p>Initializes three processor threads, one to handle the promotion of jobs from waiting to
 * running, another to post-process terminated jobs, and a sweeper to clean up the ready and scan
 * the waiting queues. See comments to each processor for further details.
 *
 * <p>For the moment, there is no retry policy for failed jobs.
 */
public class BulkServiceQueue implements SignalAware {

  private static final Logger LOGGER = LoggerFactory.getLogger(BulkServiceQueue.class);
  private static final int DEFAULT_MAX_RUNNING_JOBS = 100;
  private static final int DEFAULT_MAX_QUEUED_JOBS = 1_000_000;
  private static final int DEFAULT_AVG_JOBS_PER_REQUEST = 100_000;

  /**
   * Favoring single target jobs on the ready queue achieves two things: first, it constrains the
   * size of the queue a bit more, because expansion is de-privileged and not allowed to
   * continuously pile up new jobs. Second, it gives priority to the real work to be done by the
   * various plugins.
   */
  private static final Comparator<BulkJob> SINGLE_TARGET_PRIORITY =
      (j1, j2) -> {
        if (!(j1 instanceof SingleTargetJob)) {
          if (j2 instanceof SingleTargetJob) {
            return 1;
          }
        } else if (!(j2 instanceof SingleTargetJob)) {
          return -1;
        }

        return j1.compareTo(j2);
      };

  /*
   *  Submitted jobs that need to go onto the waiting/ready
   *  queue.
   *
   *  Top-level request jobs are held here until they complete.
   */
  @VisibleForTesting
  Map<String, BulkJob> submitted;

  /*
   *  Ready jobs go here.
   *
   *  Maps request id to jobs; the id is ordered according to
   *  time of arrival of its request, via a comparator which
   *  accesses the request store's status map.
   */
  @VisibleForTesting
  Multimap<String, BulkJob> readyQueue;

  /*
   *  Running jobs go here.
   */
  @VisibleForTesting
  Queue<BulkJob> runningQueue;

  /*
   *  Synchronous jobs that are waiting for completion go here.
   */
  @VisibleForTesting
  Set<BulkJob> waitingQueue;

  /**
   * Used to fetch ready requests for queueing.
   */
  @VisibleForTesting
  BulkRequestStore requestStore;

  /**
   * Handles the promotion of jobs to running.
   */
  @VisibleForTesting
  NextJobProcessor jobProcessor;

  /**
   * Handles terminated jobs.
   */
  @VisibleForTesting
  JobPostProcessor postProcessor;

  /**
   * Cleans up the ready queue.
   */
  @VisibleForTesting
  TerminalSweeper sweeper;

  /**
   * Computed (maxQueuedJobs/avgJobsPerRequest).
   */
  @VisibleForTesting
  int maxRequests;

  /**
   * Necessary for callbacks telling the handler that requests can be processed or that targets have
   * terminated.
   */
  private BulkRequestCompletionHandler completionHandler;

  /**
   * Handles the initial request activation.
   */
  private BulkSubmissionHandler submissionHandler;

  /**
   * The job executor. Threads dedicated to running bulk jobs.
   */
  private ExecutorService bulkJobExecutorService;

  /**
   * The cleanup executor. Threads dedicated to post-processing jobs.
   */
  private ExecutorService cleanupExecutorService;

  /**
   * Threads dedicated to running the three processors.
   */
  private ExecutorService processorExecutorService;

  /**
   * Serves as the upper limit on the number of jobs which can be run at a given time. This should
   * reflect the available number of threads.
   */
  private int maxRunningJobs = DEFAULT_MAX_RUNNING_JOBS;

  /**
   * Serves as the upper limit on the number of requests which can be active at a given time. Sets a
   * limit on the size of the readyQueue and waitingQueue.
   */
  private int maxQueuedJobs = DEFAULT_MAX_QUEUED_JOBS;

  /**
   * Estimated average number of jobs generated by a request.
   */
  private int avgJobsPerRequest = DEFAULT_AVG_JOBS_PER_REQUEST;

  /**
   * Uniformly for the three processors.
   */
  private long timeout = 30;
  private TimeUnit timeoutUnit = TimeUnit.SECONDS;

  /**
   * Records number of jobs and requests processed.
   */
  private BulkServiceStatistics statistics;

  public void cancelRequestJob(String requestId) {
    synchronized (submitted) {
      BulkJob job = submitted.get(requestId);
      if (job != null) {
        if (!(job instanceof BulkRequestJob)) {
          throw new RuntimeException(
              "Job registered under request id " + requestId + " was not a request job!.");
        }
        job.cancel();
        LOGGER.trace("Request job cancelled for {}.", requestId);
      }
    }
  }

  @Override
  public int countSignals() {
    return jobProcessor.signals.get();
  }

  public void initialize() {
    maxRequests = maxQueuedJobs / avgJobsPerRequest;
    submitted = new LinkedHashMap<>();
    runningQueue = new ArrayDeque<>();
    waitingQueue = new HashSet<>();
    readyQueue =
        Multimaps.synchronizedMultimap(
            TreeMultimap.create(requestStore.getStatusComparator(), SINGLE_TARGET_PRIORITY));

    jobProcessor = new NextJobProcessor();
    postProcessor = new JobPostProcessor();
    sweeper = new TerminalSweeper();

    processorExecutorService.execute(jobProcessor);
    processorExecutorService.execute(postProcessor);
    processorExecutorService.execute(sweeper);
  }

  @Required
  public void setAvgJobsPerRequest(int avgJobsPerRequest) {
    this.avgJobsPerRequest = avgJobsPerRequest;
  }

  @Required
  public void setBulkJobExecutorService(ExecutorService bulkJobExecutorService) {
    this.bulkJobExecutorService = bulkJobExecutorService;
  }

  @Required
  public void setCleanupExecutorService(ExecutorService cleanupExecutorService) {
    this.cleanupExecutorService = cleanupExecutorService;
  }

  @Required
  public void setCompletionHandler(BulkRequestCompletionHandler completionHandler) {
    this.completionHandler = completionHandler;
  }

  @Required
  public void setMaxQueuedJobs(int maxQueuedJobs) {
    this.maxQueuedJobs = maxQueuedJobs;
  }

  @Required
  public void setMaxRunningJobs(int maxRunningJobs) {
    this.maxRunningJobs = maxRunningJobs;
  }

  @Required
  public void setProcessorExecutorService(ExecutorService processorExecutorService) {
    this.processorExecutorService = processorExecutorService;
  }

  @Required
  public void setRequestStore(BulkRequestStore requestStore) {
    this.requestStore = requestStore;
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
  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  @Required
  public void setTimeoutUnit(TimeUnit timeoutUnit) {
    this.timeoutUnit = timeoutUnit;
  }

  /**
   * For external calls (from the service, request handler and admin interface).
   */
  @Override
  public void signal() {
    /*
     *  Wake up the job processor.
     */
    jobProcessor.signal();
  }

  /**
   * Place the job on the submitted queue.
   *
   * <p>Called by the queue handler.
   */
  public void submit(BulkJob job) {
    synchronized (submitted) {
      if (job instanceof BulkRequestJob) {
        String requestId = job.getKey().getRequestId();
        LOGGER.trace("submit top-level job for {}.", requestId);
        submitted.put(requestId, job);
      } else {
        LOGGER.trace("submit {}.", job.getKey().getKey());
        submitted.put(job.getKey().getKey(), job);
      }
    }

    jobProcessor.signal();
  }

  /**
   * @return number of requests currently in the STARTED state.
   */
  @VisibleForTesting
  int activeRequests() {
    try {
      return requestStore.countActive();
    } catch (BulkRequestStorageException e) {
      LOGGER.error("problem finding number of active requests: {}.", e.toString());
      return Integer.MAX_VALUE;
    }
  }

  private void activateRequest(BulkRequest request) {
    LOGGER.trace("activateRequest {}.", request.getId());
    try {
      submissionHandler.submitRequest(request);
      LOGGER.debug("activateRequest, updating {} to STARTED.", request.getId());
      requestStore.update(request.getId(), STARTED);
    } catch (BulkRequestStorageException e) {
      LOGGER.error(
          "Unrecoverable storage update error for {}: {}; aborting.",
          request.getId(),
          e.toString());
      requestStore.abort(request.getId(), e);
    } catch (BulkServiceException e) {
      LOGGER.error(
          "Problem activating request for {}: {}; aborting.", request.getId(), e.toString());
      requestStore.abort(request.getId(), e);
    }
  }

  private void addToReady(BulkJob job) {
    synchronized (readyQueue) {
      readyQueue.put(job.getKey().getRequestId(), job);
    }
  }

  private void addToWaiting(BulkJob job) {
    synchronized (waitingQueue) {
      waitingQueue.add(job);
    }
  }

  private void removeFromReady(BulkJob job) {
    synchronized (readyQueue) {
      readyQueue.remove(job.getKey().getRequestId(), job);
    }
  }

  private void removeFromWaiting(BulkJob job) {
    synchronized (waitingQueue) {
      waitingQueue.remove(job);
    }
  }

  private void startJob(BulkJob job) {
    if (bulkJobExecutorService.isShutdown()) {
      LOGGER.trace("execution was shut down");
      return;
    }
    LOGGER.trace("submitting job {} to executor.", job.getKey());
    runningQueue.offer(job);
    job.setFuture(bulkJobExecutorService.submit(new FireAndForgetTask(job)));
  }

  private void updateReadyStats() {
    synchronized (readyQueue) {
      statistics.currentlyQueuedJobs(readyQueue.size());
    }
  }

  private void updateWaitingStats() {
    synchronized (waitingQueue) {
      statistics.currentlyWaitingJobs(waitingQueue.size());
    }
  }

  abstract class JobProcessor implements Runnable {

    /*
     *  For reporting operations terminated or cancelled while the
     *  consumer thread is doing work outside the wait monitor.
     */
    @VisibleForTesting
    AtomicInteger signals = new AtomicInteger(0);

    public void run() {
      try {
        while (!Thread.interrupted()) {
          doRun();
          await();
        }
      } catch (InterruptedException e) {
        LOGGER.warn("interrupted.");
      }

      LOGGER.trace("exiting.");
    }

    /*
     *  Factored out so that it can be tested synchronously.
     */
    protected abstract void doRun() throws InterruptedException;

    /*
     *  For intercommunication between processor instances.
     */
    void signal() {
      signals.incrementAndGet();
      synchronized (this) {
        notifyAll();
      }
    }

    private void await() throws InterruptedException {
      if (signals.get() > 0) {
        LOGGER.trace(
            "received {} signals; returning immediately " + "for more work.", signals.get());
        signals.set(0);
        return;
      }
      synchronized (this) {
        LOGGER.trace("waiting ...");
        wait(timeoutUnit.toMillis(timeout));
      }
    }
  }

  /**
   * Does the main processing work.
   *
   * <p>First it removes all terminated jobs from the running queue and passes them off to the
   * post-processor.
   *
   * <p>Second, it elects from the store the next available requests to be submitted and calls
   * activate on them, converting them into BulkRequestJobs and placing them on the submitted
   * queue.
   *
   * <p>Third, it appends the submitted jobs to the ready queue, again removing cancelled jobs
   * there
   * and passing them to post-processing.
   *
   * <p>Finally, it checks the size of the running queue for available slots, and fills these from
   * the ready queue.
   *
   * <p>The sweeper is signalled/awakened and statistics are updated.
   */
  @VisibleForTesting
  class NextJobProcessor extends JobProcessor {

    protected void doRun() throws InterruptedException {
      LOGGER.trace("NextJobProcessor, starting doRun().");

      long start = System.currentTimeMillis();

      removeFromRunning();

      if (Thread.interrupted()) {
        throw new InterruptedException();
      }

      processNextRequests();

      if (Thread.interrupted()) {
        throw new InterruptedException();
      }

      appendSubmitted();

      if (Thread.interrupted()) {
        throw new InterruptedException();
      }

      processNextReady();

      sweeper.signal();

      statistics.sweepFinished(System.currentTimeMillis() - start);

      LOGGER.trace("doRun() completed");

      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }

    /**
     * Moves terminated (cancelled) request jobs to the post processor, merging the rest with the
     * ready queue.
     *
     * <p>This method runs before the processNextReady() method, so that the ready queue is already
     * filled with all jobs currently available to run.
     */
    private void appendSubmitted() {
      synchronized (submitted) {
        LOGGER.trace("appendSubmitted(), {} submitted", submitted.size());

        for (Iterator<BulkJob> jobIterator = submitted.values().iterator();
            jobIterator.hasNext(); ) {
          BulkJob job = jobIterator.next();
          switch (job.getState()) {
            case CANCELLED:
            case FAILED:
            case COMPLETED:
              postProcessor.offer(job);
              jobIterator.remove();
              LOGGER.trace(
                  "appendSubmitted(), removed job {} from " + "submitted queue", job.getKey());
              break;
            case CREATED:
              BulkServiceQueue.this.addToReady(job);
              LOGGER.trace(
                  "appendSubmitted(), job {} will be put " + "on ready queue", job.getKey());
              /*
               *  Fall-through to check if we can remove the job.
               */
            default:
              /*
               *  Keep the BulkRequestJobs around in case we
               *  need to access them for cancellation,
               *  since they are not stored.
               */
              if (!(job instanceof BulkRequestJob)) {
                jobIterator.remove();
                LOGGER.trace(
                    "appendSubmitted(), removed job {} " + "from  submitted " + "queue",
                    job.getKey());
              }
              break;
          }
        }
      }
    }

    /**
     * Populates the open slots on the running queue from the ready queue (calling startJob).
     *
     * <p>Skips the terminated tasks, leaving their removal to the sweeper. Since the job is
     * initialized when selected, jobs will not be selected more than once. This procedure makes for
     * less efficient iteration of the queue by the inner loops, but allows for better slicing
     * between the job processor and the sweeper.
     */
    private void processNextReady() {
      LOGGER.trace("processNextReady().");

      /*
       *  StartJob synchronizes on the runningQueue,
       *  so we avoid potential deadlocks via an intermediate queue.
       */
      List<BulkJob> toRun = new ArrayList<>();

      int available;
      int lastAvailable = 0;

      synchronized (runningQueue) {
        available = maxRunningJobs - runningQueue.size();
      }

      LOGGER.trace("processNextReady(): {} slots available.", available);

      /*
       *  A rudimentary fair-share algorithm which selects
       *  one job from each request, prioritized according
       *  to arrival time, then iterates until the max is reached.
       *
       *  As noted, the jobs are ordered on the ready queue such
       *  that expansion/multiple target jobs go last.
       */
      synchronized (readyQueue) {
        while (available > 0 && lastAvailable != available) {
          lastAvailable = available;
          for (Iterator<Entry<String, Collection<BulkJob>>> i =
              readyQueue.asMap().entrySet().iterator();
              i.hasNext() && available > 0; ) {
            Entry<String, Collection<BulkJob>> entry = i.next();
            for (Iterator<BulkJob> j = entry.getValue().iterator(); j.hasNext(); ) {
              BulkJob job = j.next();
              if (job.isReady()) {
                LOGGER.debug(
                    "processNextReady(): selecting " + "for run {} " + "from ready queue.",
                    job.getKey());
                job.initialize();
                toRun.add(job);
                --available;
                /*
                 *  Maximum of one job per request per
                 *  iteration.
                 *
                 *  Go back to the second loop and
                 *  select from the next request.
                 */
                break;
              }
            }
          }

          /*
           *  If less jobs than available slots have been selected,
           *  but there were new selections this iteration,
           *  try again from the beginning.
           */
        }
      }

      /*
       *  Remove without provoking a ConcurrentModificationException.
       *  Synchronized on readyQueue.
       */
      toRun.stream().forEach(BulkServiceQueue.this::removeFromReady);
      updateReadyStats();

      /*
       *  Synchronize on the running queue, again only to avoid
       *  ConcurrentModificationExceptions, as only this thread touches it.
       */

      synchronized (runningQueue) {
        toRun.stream().forEach(BulkServiceQueue.this::startJob);
        statistics.currentlyRunningJobs(runningQueue.size());
      }
    }

    /**
     * If there is space available for new requests, selects the next in line (by order of arrival)
     * from the store and activates them. The conversion from request to a request job does not
     * involve much work, so it is done on this thread. The job is then added to the submitted
     * queue.
     *
     * <p>This method runs before the appendSubmitted() method, so that the append queue also has
     * the requests which have become available at this pass.
     */
    private void processNextRequests() {
      LOGGER.trace("processNextRequests()");

      int limit = maxRequests - activeRequests();

      if (limit > 0) {
        LOGGER.debug("processNextRequests(), {} requests can still " + "be started.", limit);
        try {
          List<BulkRequest> next = requestStore.next(limit);
          if (next.isEmpty()) {
            LOGGER.debug("processNextRequests(), no more QUEUED " + "requests to process.");
          } else {
            next.forEach(BulkServiceQueue.this::activateRequest);
          }
        } catch (BulkRequestStorageException e) {
          LOGGER.error(
              "processNextRequests(), problem retrieving " + "queued requests: {}", e.toString());
        }
      }

      statistics.activeRequests(activeRequests());
    }

    /**
     * Scans the running queue for terminal jobs and removes them, sending them to the
     * post-processor. It also checks to see if the running job is in a waiting state, and if so, it
     * removes it and places it on the waiting queue.
     *
     * <p>Synchronized on the running queue only to avoid ConcurrentModificationExceptions, as that
     * queue is only accessed on this thread.
     */
    private void removeFromRunning() {
      synchronized (runningQueue) {
        for (Iterator<BulkJob> jiterator = runningQueue.iterator(); jiterator.hasNext(); ) {
          BulkJob job = jiterator.next();
          if (job.isTerminated()) {
            postProcessor.offer(job);
            jiterator.remove();
          } else if (job.isWaiting()) {
            addToWaiting(job);
            jiterator.remove();
          }
        }

        LOGGER.trace(
            "after remove, running {}, waiting {}.", runningQueue.size(), waitingQueue.size());
      }

      updateWaitingStats();
    }
  }

  /**
   * This sweeper has two purposes.
   *
   * <p>First, because the ready queue is unbounded, it can grow rather large as directories get
   * expanded. Checking for cancelled jobs on the same thread as the one which activates new jobs
   * does not scale. In order not to slow down the recycling of running slots, this extra thread is
   * dedicated to sweeping the ready queue periodically to remove prematurely terminated jobs.
   *
   * <p>Before it does so, however, it first scans all the jobs synchronously waiting for
   * completion
   * to see if any have terminated.
   *
   * <p>Terminated jobs are removed from their respective queues and sent to post-processing.
   */
  @VisibleForTesting
  class TerminalSweeper extends JobProcessor {

    private AtomicInteger swept = new AtomicInteger(0);

    @Override
    protected void doRun() throws InterruptedException {
      LOGGER.trace("TerminalSweeper, starting sweep.");

      removeFromWaiting();
      removeFromReady();

      if (swept.get() > 0) {
        swept.set(0);
        jobProcessor.signal();
      }

      LOGGER.trace("TerminalSweeper, sweep completed");

      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }

    /*
     *  To avoid ConcurrentModificationExceptions on
     *  removal, selection to an intermediate list is
     *  then followed by removal synchronized job-by-job.
     *  The job has a 'valid' flag which is unset
     *  when it is about to be removed, and checked
     *  before including it for removal.
     */
    private List<BulkJob> findJobsToRemove(Collection<BulkJob> jobs) {
      return jobs.stream()
          .filter(BulkJob::isValid)
          .filter(BulkJob::isTerminated)
          .map(BulkJob::invalidate)
          .collect(Collectors.toList());
    }

    private List<BulkJob> findReadyJobsToRemove() {
      synchronized (readyQueue) {
        if (readyQueue.isEmpty()) {
          return Collections.EMPTY_LIST;
        }

        return findJobsToRemove(readyQueue.values());
      }
    }

    private List<BulkJob> findWaitingJobsToRemove() {
      synchronized (waitingQueue) {
        if (waitingQueue.isEmpty()) {
          return Collections.EMPTY_LIST;
        }

        return findJobsToRemove(waitingQueue);
      }
    }

    /**
     * Scans the ready queue to see which jobs should be removed and removes them. Jobs are sent to
     * the post-processing queue.
     */
    private void removeFromReady() {
      List<BulkJob> jobs = findReadyJobsToRemove();
      if (jobs.isEmpty()) {
        return;
      }

      LOGGER.trace(
          "{}, removeFromReady, processing {} jobs.",
          Thread.currentThread().getName(),
          jobs.size());
      jobs.stream().forEach(BulkServiceQueue.this::removeFromReady);
      updateReadyStats();
      jobs.stream().forEach(postProcessor::offer);
      swept.accumulateAndGet(jobs.size(), (x, y) -> x + y);
    }

    /**
     * Scans the waiting queue to see which jobs have terminated and removes them. Jobs are sent to
     * the post-processing queue.
     */
    private void removeFromWaiting() {
      List<BulkJob> jobs = findWaitingJobsToRemove();
      if (jobs.isEmpty()) {
        return;
      }

      LOGGER.trace(
          "{}, removeFromWaiting, processing {} jobs.",
          Thread.currentThread().getName(),
          jobs.size());
      jobs.stream().forEach(BulkServiceQueue.this::removeFromWaiting);
      updateWaitingStats();
      jobs.stream().forEach(postProcessor::offer);
      swept.accumulateAndGet(jobs.size(), (x, y) -> x + y);
    }
  }

  /**
   * Post-processes terminated (COMPLETED, FAILED, CANCELLED) jobs.
   *
   * <p>This involves calling the completion handler, and then checking to see if the request has
   * terminated.
   *
   * <p>Uses the usual consumer-producer model on its internal queue.
   *
   * <p>The jobs are dequeued in batches and passed off to a fire-and-forget task.
   */
  @VisibleForTesting
  class JobPostProcessor implements Runnable {

    private final Queue<BulkJob> queue = new LinkedBlockingQueue<>();

    public void run() {
      try {
        while (!Thread.interrupted()) {
          doRun(false);
        }
      } catch (InterruptedException e) {
        LOGGER.debug("interrupted.");
      }
    }

    /**
     * @param noWait only for testing purposes will this be set to true.
     */
    protected void doRun(boolean noWait) throws InterruptedException {
      List<BulkJob> jobs;

      synchronized (this) {
        while (queue.isEmpty()) {
          if (noWait) {
            break;
          }
          wait();
        }

        jobs = queue.stream().limit(maxRunningJobs).collect(Collectors.toList());
        queue.removeAll(jobs);
      }

      if (!cleanupExecutorService.isShutdown()) {
        cleanupExecutorService.submit(
            new FireAndForgetTask(
                () -> {
                  jobs.stream().forEach(this::postProcessJob);
                }));
      }
    }

    /*
     *  For intercommunication with processor instances.
     */
    synchronized void offer(BulkJob bulkJob) {
        if (!queue.offer(bulkJob)) {
          /*
           *  Queue is unbounded, so this means something
           *  is wrong.
           */
          throw new RuntimeException(
              "Job post processor is refusing " + "new jobs; " + "this is a bug.");
        }

        notifyAll();
    }

    private void postProcessJob(BulkJob job) {
      try {
        if (!(job instanceof BulkRequestJob)) {
          /*
           *  Terminal processing of targets
           *  (jobs) removes them from the job store
           *  and also checks for request completion.
           */
          switch (job.getState()) {
            case CANCELLED:
              completionHandler.requestTargetCancelled(job);
              break;
            case FAILED:
              completionHandler.requestTargetFailed(job);
              break;
            case COMPLETED:
              completionHandler.requestTargetCompleted(job);
              break;
            default:
              throw new RuntimeException("Non-terminal job passed to "
                  + "post-processing; "
                  + "this is a bug.");
          }
        }
      } catch (BulkServiceException e) {
        LOGGER.error("Failed to post-process {}: {}.", job.getKey(), e.toString());
      }

      statistics.activeRequests(activeRequests());
    }
  }
}
