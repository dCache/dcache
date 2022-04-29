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
package org.dcache.qos.services.verifier.util;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.util.QoSCounter;
import org.dcache.qos.util.QoSCounterGroup;
import org.dcache.qos.util.QoSCounters;

public final class QoSVerifierCounters extends QoSCounters {

    public static final String ADJ_RESP_MESSAGE = "ADJUST RESPONSE";
    public static final String LOC_EXCL_MESSAGE = "LOCATION EXCLUDED";
    public static final String VRF_REQ_MESSAGE = "VERIFY REQUEST";
    public static final String BVRF_REQ_MESSAGE = "BATCH VERIFY REQUEST";
    public static final String VRF_CNCL_MESSAGE = "CANCEL REQUEST";
    public static final String BVRF_CNCL_MESSAGE = "BATCH CANCEL REQUEST";

    private static final String MESSAGES = "MESSAGES";
    private static final String OPS = "OPS";
    private static final String POOLS = "POOLS";
    private static final String ALL = "ALL";
    private static final String VOIDED = "VOIDED";

    private static final String FORMAT_MESSAGES = "%-30s %15s\n";
    private static final String FORMAT_OPS = "%-30s %15s %9s %12s\n";
    private static final String FORMAT_DETAILS = "%-40s | %12s %12s\n";
    private static final String FORMAT_STATS = "%-15s %28s | %15s %9s %9s %12s\n";
    private static final String[] MESSAGES_HEADER = {"MESSAGES", "RECEIVED"};
    private static final String[] OPS_HEADER = {"OPS", "COMPLETED", "OPS/SEC", "FAILED"};
    private static final String[] DETAILS_HEADER = {"POOL", "COMPLETED", "FAILED"};
    private static final String[] STATS_HEADER = {"EPOCH", "SWEEP", "OPS", "HZ", "CHNG", "FAILED"};

    class QoSVerifyOpsCounter extends QoSCounter {

        final AtomicLong current = new AtomicLong(0L);
        long last = 0L;

        public QoSVerifyOpsCounter(String name) {
            super(name);
        }
    }

    class QoSVerifyCounterGroup extends QoSCounterGroup<QoSCounter> {

        protected QoSVerifyCounterGroup(String name) {
            super(name);
        }

        @Override
        public void format(StringBuilder builder) {
            getKeys().stream()
                  .forEach(k -> {
                      QoSCounter c = getCounter(k);
                      builder.append(String.format(FORMAT_MESSAGES, k, c.getTotal()));
                  });
        }

        @Override
        protected QoSCounter createCounter(String key) {
            return new QoSCounter(key);
        }
    }

    class QoSVerifyRateCounterGroup extends QoSCounterGroup<QoSVerifyOpsCounter> {

        protected QoSVerifyRateCounterGroup(String name) {
            super(name);
        }

        @Override
        public void format(StringBuilder builder) {
            getKeys().stream()
                  .forEach(k -> {
                      QoSVerifyOpsCounter c = getCounter(k);
                      builder.append(String.format(FORMAT_OPS, k, c.getTotal(),
                            getRatePerSecond(c.current.get()), c.getFailed()));
                  });
        }

        @Override
        protected QoSVerifyOpsCounter createCounter(String key) {
            return new QoSVerifyOpsCounter(key);
        }
    }

    class QoSVerifyDetailsCounterGroup extends QoSCounterGroup<QoSCounter> {

        protected QoSVerifyDetailsCounterGroup(String name) {
            super(name);
        }

        @Override
        public void format(StringBuilder builder) {
            getKeys().stream()
                  .forEach(k -> {
                      QoSCounter c = getCounter(k);
                      builder.append(String.format(FORMAT_DETAILS, k, c.getTotal(), c.getFailed()));
                  });
        }

        @Override
        protected QoSCounter createCounter(String key) {
            return new QoSCounter(key);
        }
    }

    @Override
    public void initialize() {
        groupMap = new HashMap<>();

        QoSVerifyCounterGroup group = new QoSVerifyCounterGroup(MESSAGES);
        group.addCounter(ADJ_RESP_MESSAGE);
        group.addCounter(LOC_EXCL_MESSAGE);
        group.addCounter(VRF_REQ_MESSAGE);
        group.addCounter(VRF_CNCL_MESSAGE);
        group.addCounter(BVRF_REQ_MESSAGE);
        group.addCounter(BVRF_CNCL_MESSAGE);
        groupMap.put(MESSAGES, group);

        QoSVerifyRateCounterGroup rateGroup = new QoSVerifyRateCounterGroup(OPS);
        rateGroup.addCounter(ALL);
        rateGroup.addCounter(VOIDED);
        groupMap.put(OPS, rateGroup);

        QoSVerifyDetailsCounterGroup detailsGroup = new QoSVerifyDetailsCounterGroup(POOLS);
        groupMap.put(POOLS, detailsGroup);
    }

    @Override
    public void appendCounts(StringBuilder builder) {
        builder.append(String.format(FORMAT_MESSAGES, (Object[]) MESSAGES_HEADER));
        QoSCounterGroup group = groupMap.get(MESSAGES);
        group.format(builder);
        builder.append("\n");
        appendSweep(builder);
        builder.append(String.format(FORMAT_OPS, (Object[]) OPS_HEADER));
        group = groupMap.get(OPS);
        group.format(builder);
    }

    @Override
    public void appendDetails(StringBuilder builder) {
        builder.append(String.format(FORMAT_DETAILS, (Object[]) DETAILS_HEADER));
        QoSCounterGroup group = groupMap.get(POOLS);
        group.format(builder);
    }

    public void appendSweep(StringBuilder builder) {
        builder.append(String.format(LASTSWP, new Date(lastSweep))).append("\n");
    }

    public void increment(String source, String target, QoSAction type) {
        QoSVerifyOpsCounter counter = (QoSVerifyOpsCounter) groupMap.get(OPS).getCounter(ALL);
        counter.incrementTotal();
        counter.current.incrementAndGet();

        if (type == QoSAction.VOID) {
            counter = (QoSVerifyOpsCounter) groupMap.get(OPS).getCounter(VOIDED);
            counter.incrementTotal();
            counter.current.incrementAndGet();
        }

        if (source != null) {
            checkPoolCounters(source);
            groupMap.get(POOLS).getCounter(source).incrementTotal();
        }

        if (target != null) {
            checkPoolCounters(target);
            groupMap.get(POOLS).getCounter(target).incrementTotal();
        }

        if (toFile) {
            synchronized (statisticsBuffer) {
                statisticsBuffer.add(getFormattedStatistics());
            }
        }
    }

    public void incrementFailed(String pool) {
        QoSVerifyOpsCounter counter = (QoSVerifyOpsCounter) groupMap.get(OPS).getCounter(ALL);
        counter.incrementTotal();
        counter.incrementFailed();
        counter.current.incrementAndGet();
        if (pool != null) {
            checkPoolCounters(pool);
            groupMap.get(POOLS).getCounter(pool).incrementFailed();
        }
        if (toFile) {
            synchronized (statisticsBuffer) {
                statisticsBuffer.add(getFormattedStatistics());
            }
        }

    }

    public void incrementReceived(String type) {
        QoSCounter counter = groupMap.get(MESSAGES).getCounter(type);
        if (counter != null) {
            counter.incrementTotal();
        }
    }

    @Override
    public void recordSweep(long ended, long duration) {
        lastSweep = ended;
        lastSweepDuration = duration;
        writeStatistics();
        resetLatestCounts();
    }

    @Override
    protected String getStatisticsFormat() {
        return FORMAT_STATS;
    }

    @Override
    protected String[] getStatisticsHeader() {
        return STATS_HEADER;
    }

    private void checkPoolCounters(String pool) {
        QoSVerifyDetailsCounterGroup group = (QoSVerifyDetailsCounterGroup) groupMap.get(POOLS);
        if (!group.hasCounter(pool)) {
            group.addCounter(pool);
        }
    }

    private String getFormattedStatistics() {
        QoSVerifyOpsCounter counter = (QoSVerifyOpsCounter) groupMap.get(OPS).getCounter(ALL);
        long ops = counter.getTotal();
        long failed = counter.getFailed();
        long current = counter.current.get();
        long last = counter.last;
        double hz = getRatePerSecond(current);
        String delta = getRateChangeSinceLast(current, last);
        return String.format(FORMAT_STATS, lastSweep, new Date(lastSweep), ops, hz, delta, failed);
    }

    private long getRatePerSecond(long value) {
        long elapsed = System.currentTimeMillis() - lastSweep;
        elapsed = TimeUnit.MILLISECONDS.toSeconds(elapsed);
        if (elapsed == 0) {
            return 0L;
        }
        return value / elapsed;
    }

    private void resetLatestCounts() {
        QoSVerifyOpsCounter counter = (QoSVerifyOpsCounter) groupMap.get(OPS).getCounter(ALL);
        counter.last = counter.current.get();
        counter.current.set(0L);
        counter = (QoSVerifyOpsCounter) groupMap.get(OPS).getCounter(VOIDED);
        counter.last = counter.current.get();
        counter.current.set(0L);
    }
}
