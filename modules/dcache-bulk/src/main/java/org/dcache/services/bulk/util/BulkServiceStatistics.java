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

import static org.dcache.services.bulk.util.BulkRequestTarget.State.CANCELLED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.COMPLETED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.FAILED;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.RUNNING;
import static org.dcache.services.bulk.util.BulkRequestTarget.State.SKIPPED;

import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides activity statistics via the CellInfo interface.
 */
public final class BulkServiceStatistics implements CellInfoProvider {

    private static final String LAST_START = "Running since: %s";
    private static final String UP_TIME = "Uptime %s days, %s hours, %s minutes, %s seconds";
    private static final String LAST_SWEEP = "Last job sweep at %s";
    private static final String LAST_SWEEP_DURATION = "Last job sweep took %s seconds";
    private static final String STATS_FORMAT = "%-20s :    %10s";

    private final Date started = new Date();

    private final AtomicInteger activeRequests = new AtomicInteger(0);
    private final AtomicLong requestsCompleted = new AtomicLong(0L);
    private final AtomicLong requestsCancelled = new AtomicLong(0L);
    private final Map<String, AtomicLong> requestTypes = new TreeMap<>();
    private final Map<String, AtomicLong> userRequests = new TreeMap<>();
    private final Map<String, AtomicLong> counts
          = Map.of(RUNNING.name(), new AtomicLong(0L),
          CANCELLED.name(), new AtomicLong(0L),
          COMPLETED.name(), new AtomicLong(0L),
          FAILED.name(), new AtomicLong(0L),
          SKIPPED.name(), new AtomicLong(0L));

    private long lastSweep = started.getTime();
    private long lastSweepDuration = 0;

    public void addUserRequest(String user) {
        AtomicLong counter = userRequests.get(user);
        if (counter == null) {
            counter = new AtomicLong(0);
            userRequests.put(user, counter);
        }
        counter.incrementAndGet();
    }

    @Override
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

        pw.println("------------------ TARGETS BY STATE ------------------");
        pw.println("         (cumulative from last service start)");
        counts.entrySet().stream().filter(e->!e.getKey().equals(RUNNING.name()))
              .forEach(e -> pw.println(String.format(STATS_FORMAT, e.getKey(), e.getValue().get())));
        pw.println();

        long received = requestTypes.values().stream().mapToLong(AtomicLong::get).sum();

        pw.println("------------ REQUEST TOTALS (since start) ------------");
        pw.println(String.format(STATS_FORMAT, "Requests received", received));
        pw.println(String.format(STATS_FORMAT, "Requests completed", requestsCompleted.get()));
        pw.println(String.format(STATS_FORMAT, "Requests cancelled", requestsCancelled.get()));
        pw.println();

        pw.println("--------------- REQUESTS (since start) ---------------");
        requestTypes.entrySet().stream()
              .forEach(entry ->
                    pw.println(
                          String.format(STATS_FORMAT, entry.getKey(), entry.getValue().get())));
        pw.println();

        pw.println("---------------- REQUESTS  (current) -----------------");
        pw.println(String.format(STATS_FORMAT, "Active", activeRequests.get()));
        pw.println();

        pw.println("----------------- TARGETS  (current) -----------------");
        pw.println(String.format(STATS_FORMAT, RUNNING.name(), counts.get(RUNNING.name())));
        pw.println();
    }

    public String getOwnerCounts() {
        StringBuilder builder = new StringBuilder();
        builder.append("----------------- USERS (since start) ----------------\n");
        userRequests.entrySet().stream()
              .forEach(entry ->
                    builder.append(
                                String.format(STATS_FORMAT, entry.getKey(), entry.getValue().get()))
                          .append("\n"));
        builder.append("\n");
        return builder.toString();
    }

    public void incrementRequestsCancelled() {
        requestsCancelled.incrementAndGet();
    }

    public void incrementRequestsCompleted() {
        requestsCompleted.incrementAndGet();
    }

    public void increment(String targetState) {
        AtomicLong counter = counts.get(targetState);
        if (counter != null) {
            counter.incrementAndGet();
        }
    }

    public void increment(String targetState, long by) {
        AtomicLong counter = counts.get(targetState);
        if (counter != null) {
            counter.addAndGet(by);
        }
    }

    public synchronized void decrement(String targetState) {
        AtomicLong counter = counts.get(targetState);
        if (counter != null) {
            counter.decrementAndGet();
        }
    }

    public synchronized void decrement(String targetState, long by) {
        AtomicLong counter = counts.get(targetState);
        if (counter != null) {
            counter.addAndGet(-by);
        }
    }

    public void incrementRequestsReceived(String activity) {
        requestTypes.computeIfAbsent(activity, v -> new AtomicLong(0)).incrementAndGet();
    }

    public void setActive(int count) {
        activeRequests.set(count);
    }

    public void sweepFinished(long duration) {
        lastSweep = System.currentTimeMillis();
        lastSweepDuration = duration;
    }
}
