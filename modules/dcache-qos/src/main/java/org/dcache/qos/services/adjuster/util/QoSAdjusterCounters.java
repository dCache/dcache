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
package org.dcache.qos.services.adjuster.util;

import static org.dcache.qos.data.QoSAction.CACHE_REPLICA;
import static org.dcache.qos.data.QoSAction.COPY_REPLICA;
import static org.dcache.qos.data.QoSAction.FLUSH;
import static org.dcache.qos.data.QoSAction.PERSIST_REPLICA;
import static org.dcache.qos.data.QoSAction.UNSET_PRECIOUS_REPLICA;
import static org.dcache.qos.data.QoSAction.WAIT_FOR_STAGE;

import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.util.QoSCounter;
import org.dcache.qos.util.QoSCounterGroup;
import org.dcache.qos.util.QoSCounters;

public final class QoSAdjusterCounters extends QoSCounters {

    private static final String ACTIONS = "ACTIONS";
    private static final String POOLS = "POOLS";
    private static final String FORMAT_ACTIONS = "%-26s %12s %12s\n";
    private static final String FORMAT_DETAILS =
          "%-28s | %12s %12s | %12s %12s %12s | %12s %12s %12s "
                + "| %12s %12s\n";
    private static final String FORMAT_STAT
          = "%-15s %37s | %15s | %25s %25s | %25s %25s | %9s seconds | %15s\n";
    private static final String[] ACTIONS_HEADER = {"ACTION", "TOTAL", "FAILED"};
    private static final String[] DETAILS_HEADER = {"POOL", "TGT", "FAILED", "CPSRC", "CPTGT",
          "CPBYT",
          "CACHE", "PERSIST", "UNSETP", "STAGE", "FLUSH"};
    private static final String[] STATS_HEADER = {"END", "ID", "ACTION", "START", "END",
          "SRC", "TGT", "DURATION", "FAILURE"};

    class QoSAdjustCounter extends QoSCounter {

        final AtomicLong copySrc = new AtomicLong(0L);
        final AtomicLong copyTgt = new AtomicLong(0L);
        final AtomicLong copyByt = new AtomicLong(0L);
        final AtomicLong cacheTgt = new AtomicLong(0L);
        final AtomicLong persistTgt = new AtomicLong(0L);
        final AtomicLong unsetTgt = new AtomicLong(0L);
        final AtomicLong stageTgt = new AtomicLong(0L);
        final AtomicLong flushTgt = new AtomicLong(0L);

        public QoSAdjustCounter(String name) {
            super(name);
        }
    }

    class QoSAdjustCounterGroup extends QoSCounterGroup<QoSCounter> {

        protected QoSAdjustCounterGroup(String name) {
            super(name);
        }

        @Override
        public void format(StringBuilder builder) {
            getKeys().stream()
                  .forEach(k -> {
                      QoSCounter c = getCounter(k);
                      builder.append(String.format(FORMAT_ACTIONS, k, c.getTotal(), c.getFailed()));
                  });
        }

        @Override
        protected QoSCounter createCounter(String key) {
            return new QoSCounter(key);
        }
    }

    class QoSAdjustDetailsCounterGroup extends QoSCounterGroup<QoSAdjustCounter> {

        protected QoSAdjustDetailsCounterGroup(String name) {
            super(name);
        }

        @Override
        public void format(StringBuilder builder) {
            getKeys().stream()
                  .forEach(k -> {
                      QoSAdjustCounter c = getCounter(k);
                      builder.append(String.format(FORMAT_DETAILS, k, c.getTotal(), c.getFailed(),
                            c.copySrc.get(), c.copyTgt.get(), formatWithPrefix(c.copyByt.get()),
                            c.cacheTgt.get(), c.persistTgt.get(), c.unsetTgt.get(),
                            c.stageTgt.get(),
                            c.flushTgt.get()));
                  });
        }

        @Override
        protected QoSAdjustCounter createCounter(String key) {
            return new QoSAdjustCounter(key);
        }
    }

    @Override
    public void initialize() {
        groupMap = new HashMap<>();
        QoSCounterGroup group = new QoSAdjustCounterGroup(ACTIONS);
        group.addCounter(COPY_REPLICA.name());
        group.addCounter(CACHE_REPLICA.name());
        group.addCounter(PERSIST_REPLICA.name());
        group.addCounter(UNSET_PRECIOUS_REPLICA.name());
        group.addCounter(WAIT_FOR_STAGE.name());
        group.addCounter(FLUSH.name());
        groupMap.put(ACTIONS, group);

        group = new QoSAdjustDetailsCounterGroup(POOLS);
        groupMap.put(POOLS, group);
    }

    @Override
    public void appendCounts(StringBuilder builder) {
        builder.append(String.format(FORMAT_ACTIONS, (Object[]) ACTIONS_HEADER));
        QoSCounterGroup group = groupMap.get(ACTIONS);
        group.format(builder);
    }

    @Override
    public void appendDetails(StringBuilder builder) {
        builder.append(String.format(FORMAT_DETAILS, (Object[]) DETAILS_HEADER));
        QoSCounterGroup group = groupMap.get(POOLS);
        group.format(builder);
    }

    public void recordTask(QoSAdjusterTask task) {
        if (task == null) {
            LOGGER.debug("recordTaskStatistics called with null task");
            return;
        }

        boolean failed = task.getException() != null;
        QoSAction action = task.getAction();
        QoSCounter actionCounter = groupMap.get(ACTIONS).getCounter(action.name());
        actionCounter.incrementTotal();
        if (failed) {
            actionCounter.incrementFailed();
        }

        if (toFile) {
            synchronized (statisticsBuffer) {
                statisticsBuffer.add(toFormattedString(task));
            }
        }

        String target = task.getTarget();
        if (target == null) {
            /*
             *  NB.  This is possible if this is a staging task which was cancelled.
             */
            LOGGER.debug("{}, no target", task);
            return;
        }

        checkPoolCounters(target);
        QoSAdjustCounter targetCounter = (QoSAdjustCounter) groupMap.get(POOLS).getCounter(target);

        /*
         *  Count total and failed only on target pools.
         */
        targetCounter.incrementTotal();
        if (failed) {
            targetCounter.incrementFailed();
        }

        switch (action) {
            case COPY_REPLICA:
                targetCounter.copyTgt.incrementAndGet();
                targetCounter.copyByt.addAndGet(task.getAttributes().getSize());
                String source = task.getSource();
                if (source == null) {
                    LOGGER.debug("{} no source ... could be a potential bug?", task);
                    return;
                }
                checkPoolCounters(source);
                QoSAdjustCounter srcCounter = (QoSAdjustCounter) groupMap.get(POOLS)
                      .getCounter(source);
                srcCounter.copySrc.incrementAndGet();
                break;
            case UNSET_PRECIOUS_REPLICA:
                targetCounter.unsetTgt.incrementAndGet();
                break;
            case CACHE_REPLICA:
                targetCounter.cacheTgt.incrementAndGet();
                break;
            case PERSIST_REPLICA:
                targetCounter.persistTgt.incrementAndGet();
                break;
            case FLUSH:
                targetCounter.flushTgt.incrementAndGet();
                break;
            case WAIT_FOR_STAGE:
                targetCounter.stageTgt.incrementAndGet();
                break;
            default:
                LOGGER.debug("{} UNKNOWN ACTION?", task);
                break;
        }
    }

    @Override
    protected String getStatisticsFormat() {
        return FORMAT_STAT;
    }

    @Override
    protected String[] getStatisticsHeader() {
        return STATS_HEADER;
    }

    private void checkPoolCounters(String pool) {
        QoSAdjustDetailsCounterGroup group = (QoSAdjustDetailsCounterGroup) groupMap.get(POOLS);
        if (!group.hasCounter(pool)) {
            group.addCounter(pool);
        }
    }

    private String toFormattedString(QoSAdjusterTask task) {
        String source = task.getSource();
        String target = task.getTarget();
        long startTime = task.getStartTime();
        long endTime = task.getEndTime();
        Exception exception = task.getException();
        return String.format(FORMAT_STAT,
              endTime,
              task.getPnfsId(),
              task.getAction(),
              DATE_FORMATTER.format(Instant.ofEpochMilli(startTime)),
              DATE_FORMATTER.format(Instant.ofEpochMilli(endTime)),
              source == null ? "-----" : source,
              target == null ? "-----" : target,
              getTimeInSeconds(endTime - startTime),
              exception == null ? task.getStatus()
                    : task.getStatus() + ": " + exception.toString());
    }

    private static String getTimeInSeconds(long elapsed) {
        if (elapsed < 0) {
            return "-----";
        }
        double delta = ((double) elapsed) / (1000.0);
        return String.format("%.3f", delta);
    }
}
