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
package org.dcache.services.bulk.util;

import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides activity statistics since last restart via the CellInfo interface.
 */
public class BulkServiceStatistics implements CellInfoProvider {

  private static final String LAST_START = "Running since: %s";
  private static final String UP_TIME = "Uptime %s days, %s hours," + " %s minutes, %s seconds";
  private static final String LAST_SWEEP = "Last job sweep at %s";
  private static final String LAST_SWEEP_DURATION = "Last job sweep took %s seconds";
  private static final String STATS_FORMAT = "%-20s :    %10s";

  private final Date started = new Date();

  private final AtomicLong requestsCompleted = new AtomicLong(0);
  private final AtomicLong requestsCancelled = new AtomicLong(0);
  private final AtomicLong jobsAborted = new AtomicLong(0);
  private final AtomicLong jobsCompleted = new AtomicLong(0);
  private final AtomicLong jobsFailed = new AtomicLong(0);
  private final AtomicLong jobsCancelled = new AtomicLong(0);
  private final Map<String, AtomicLong> requestTypes = new TreeMap<>();
  private final Map<String, AtomicLong> userRequests = new TreeMap<>();

  private long lastSweep = started.getTime();
  private long lastSweepDuration = 0;
  private int runningJobs = 0;
  private int waitingJobs = 0;
  private int queuedJobs = 0;
  private int activeRequests = 0;

  public void activeRequests(int count) {
    activeRequests = count;
  }

  public void addUserRequest(String user) {
    AtomicLong counter = userRequests.get(user);
    if (counter == null) {
      counter = new AtomicLong(0);
      userRequests.put(user, counter);
    }
    counter.incrementAndGet();
  }

  public void currentlyQueuedJobs(int count) {
    queuedJobs = count;
  }

  public void currentlyRunningJobs(int count) {
    runningJobs = count;
  }

  public void currentlyWaitingJobs(int count) {
    waitingJobs = count;
  }

  public void getInfo(PrintWriter pw) {
    Duration duration = Duration.between(started.toInstant(), Instant.now());

    pw.println(String.format(LAST_START, started));
    pw.println(String.format(UP_TIME,
                             duration.toDays(),
                             duration.toHours() % 24,
                             duration.toMinutes() % 60,
                             duration.toSeconds() % 60));
    pw.println();

    pw.println(String.format(LAST_SWEEP, new Date(lastSweep)));
    pw.println(String.format(LAST_SWEEP_DURATION,
                             TimeUnit.MILLISECONDS.toSeconds(lastSweepDuration)));
    pw.println();

    long received = requestTypes.values().stream().mapToLong(AtomicLong::get).sum();

    pw.println("-------------------- REQUEST INFO --------------------");
    pw.println(String.format(STATS_FORMAT, "Requests received", received));
    pw.println(String.format(STATS_FORMAT, "Requests completed", requestsCompleted.get()));
    pw.println(String.format(STATS_FORMAT, "Requests cancelled", requestsCancelled.get()));
    pw.println();

    pw.println("------------------- REQUEST DETAILS ------------------");
    requestTypes.entrySet().stream()
        .forEach(entry ->
                pw.println(String.format(STATS_FORMAT, entry.getKey(), entry.getValue().get())));

    pw.println();

    pw.println("---------------------- JOB INFO ----------------------");
    pw.println(String.format(STATS_FORMAT, "Jobs completed", jobsCompleted.get()));
    pw.println(String.format(STATS_FORMAT, "Jobs failed", jobsFailed.get()));
    pw.println(String.format(STATS_FORMAT, "Jobs cancelled", jobsCancelled.get()));
    pw.println(String.format(STATS_FORMAT, "Jobs aborted", jobsAborted.get()));
    pw.println();

    pw.println("---------------------- USER INFO ---------------------");
    userRequests.entrySet().stream()
        .forEach(entry ->
                pw.println(String.format(STATS_FORMAT, entry.getKey(), entry.getValue().get())));
    pw.println();

    pw.println("--------------------- QUEUE  INFO --------------------");
    pw.println(String.format(STATS_FORMAT, "Running jobs", runningJobs));
    pw.println(String.format(STATS_FORMAT, "Waiting jobs", waitingJobs));
    pw.println(String.format(STATS_FORMAT, "Queued jobs", queuedJobs));
    pw.println(String.format(STATS_FORMAT, "Active requests", activeRequests));
  }

  public void incrementJobsAborted() {
    jobsAborted.incrementAndGet();
  }

  public void incrementJobsCancelled() {
    jobsCancelled.incrementAndGet();
  }

  public void incrementJobsCompleted() {
    jobsCompleted.incrementAndGet();
  }

  public void incrementJobsFailed() {
    jobsFailed.incrementAndGet();
  }

  public void incrementRequestsCancelled() {
    requestsCancelled.incrementAndGet();
  }

  public void incrementRequestsCompleted() {
    requestsCompleted.incrementAndGet();
  }

  public void incrementRequestsReceived(String activity) {
    requestTypes.computeIfAbsent(activity, v -> new AtomicLong(0)).incrementAndGet();
  }

  public void sweepFinished(long duration) {
    lastSweep = System.currentTimeMillis();
    lastSweepDuration = duration;
  }
}
