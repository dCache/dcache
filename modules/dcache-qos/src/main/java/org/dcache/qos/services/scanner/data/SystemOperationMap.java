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
package org.dcache.qos.services.scanner.data;

import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.qos.services.scanner.data.PoolScanOperation.State;
import org.dcache.qos.services.scanner.data.ScanOperation.ScanLabel;
import org.dcache.qos.services.scanner.handlers.SysOpHandler;
import org.dcache.qos.services.scanner.util.QoSScannerCounters;
import org.dcache.qos.services.scanner.util.SystemScanTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains two maps corresponding to ONLINE and QOS_NEARLINE operations. The runnable method checks
 * for the period window expiration and launches the background operations when appropriate.
 * <p/>
 * The ONLINE operations look at ONLINE REPLICA and CUSTODIAL files.  The QOS_NEARLINE operations
 * look at files for which a policy is defined but whose current AL/RP is NEARLINE CUSTODIAL.
 * <p/>
 * If ONLINE scans are not activated, an IDLE-ENABLED pool scan is scheduled instead (as formerly
 * with resilience).  The online window and unit then apply to the pool scans.
 * <p/>
 * Provides methods for cancellation of running scans, and for ad hoc submission of a scan.
 * <p/>
 * Class is not marked final for stubbing/mocking purposes.
 */
public class SystemOperationMap extends ScanOperationMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemOperationMap.class);

    private static final String SCAN_DURATION
          = "\n\t%s days, %s hours, %s minutes, %s seconds\n\n";

    private static final byte ONLINE = 0x2;
    private static final byte QOS_NEARLINE = 0x4;

    private static final PoolFilter ALL_IDLE_ENABLED_POOLS;

    static {
        PoolFilter filter = new PoolFilter();
        filter.setState(Set.of(State.IDLE.name()));
        filter.setPoolStatus("ENABLED");
        ALL_IDLE_ENABLED_POOLS = filter;
    }

    private final Map<String, SystemScanOperation> online = new HashMap<>();
    private final Map<String, SystemScanOperation> qosNearline = new HashMap<>();
    private final EvictingQueue<String> history = EvictingQueue.create(100);

    private volatile int state;

    private volatile Integer onlineRescanWindow;
    private volatile TimeUnit onlineRescanWindowUnit;
    private volatile boolean onlineScanEnabled = false;

    private volatile Integer qosNearlineRescanWindow;
    private volatile TimeUnit qosNearlineRescanWindowUnit;

    private volatile int onlineBatchSize = 200000;
    private volatile int qosNearlineBatchSize = 500000;

    private long lastOnlineScanStart;
    private long lastOnlineScanEnd;
    private long lastQosNearlineScanStart;
    private long lastQosNearlineScanEnd;
    private long lastPoolScanStart;
    private long nextPoolScanStart;

    private SysOpHandler handler;
    private QoSScannerCounters counters;

    private PoolOperationMap poolOperationMap;

    public SystemOperationMap() {
        long now = System.currentTimeMillis();
        lastOnlineScanStart = now;
        lastOnlineScanEnd = now;
        lastQosNearlineScanStart = now;
        lastQosNearlineScanEnd = now;
        lastPoolScanStart = now;
        nextPoolScanStart = now;
    }

    public void cancelSystemScan(String id) {
        SystemScanOperation operation;

        lock.lock();
        try {
            operation = remove(id);
            if (operation != null) {
                cancel(operation);
            }
        } finally {
            lock.unlock();
        }
    }

    public void cancelAll(boolean qos) {
        lock.lock();
        try {
            if (qos) {
                this.qosNearline.values().forEach(this::cancel);
                this.qosNearline.clear();
            } else {
                this.online.values().forEach(this::cancel);
                this.online.clear();
            }
        } finally {
            lock.unlock();
        }

        if (qos) {
            state &= (~QOS_NEARLINE);
            lastQosNearlineScanEnd = System.currentTimeMillis();
        } else {
            state &= (~ONLINE);
            lastOnlineScanEnd = System.currentTimeMillis();
        }
    }

    public String configSettings() {
        return String.format("system online scan window %s %s\n"
                    + "system online scan is %s\n"
                    + "system qosNearline scan window %s %s\n"
                    + "max concurrent operations %s\n"
                    + "period set to %s %s\n\n",
              onlineRescanWindow,
              onlineRescanWindowUnit,
              onlineScanEnabled ? "on" : "off",
              qosNearlineRescanWindow,
              qosNearlineRescanWindowUnit,
              maxConcurrentRunning,
              timeout,
              timeoutUnit);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        StringBuilder builder = new StringBuilder();
        builder.append(configSettings());
        counters.appendRunning(builder);
        counters.appendSweep(builder);
        builder.append("\n")
              .append(String.format("last online scan start %s\nlast online scan end %s\n",
                    new Date(lastOnlineScanStart),
                    new Date(lastOnlineScanEnd)));
        long seconds = TimeUnit.MILLISECONDS.toSeconds(lastOnlineScanEnd - lastOnlineScanStart);
        if (seconds < 0L) {
            seconds = 0L;
        }
        counters.appendDHMSElapsedTime(seconds, SCAN_DURATION, builder);
        builder.append(String.format("last qosNearline (nearline) scan start %s\n"
                    + "last qosNearline (nearline) scan end %s\n",
              new Date(lastQosNearlineScanStart),
              new Date(lastQosNearlineScanEnd)));
        seconds = TimeUnit.MILLISECONDS.toSeconds(lastQosNearlineScanEnd - lastQosNearlineScanStart);
        if (seconds < 0L) {
            seconds = 0L;
        }
        counters.appendDHMSElapsedTime(seconds, SCAN_DURATION, builder);
        builder.append("\n")
              .append(String.format("last pool scan start %s\nnext pool scan start %s\n",
                    new Date(lastPoolScanStart),
                    new Date(nextPoolScanStart)));
        builder.append("\n");
        pw.print(builder);
    }

    public String getSystemScanStatus() {
        lock.lock();
        StringBuilder builder = new StringBuilder();
        try {
            online.entrySet().forEach(e ->
                  builder.append(e.getValue()).append("\n"));
            if (!online.isEmpty() && !qosNearline.isEmpty()) {
                builder.append("------------------------------------------------\n");
            }
            qosNearline.entrySet().forEach(e ->
                  builder.append(e.getValue()).append("\n"));
        } finally {
            lock.unlock();
        }
        return builder.toString();
    }

    public String historyAscending() {
        synchronized (history) {
            return history.stream().collect(Collectors.joining("\n"));
        }
    }

    public String historyDescending() {
        StringBuilder stringBuilder = new StringBuilder();
        synchronized (history) {
            history.stream().forEach(e -> stringBuilder.insert(0, e).append("\n"));
        }
        return stringBuilder.toString();
    }

    public void runScans() {
        lock.lock();
        try {
            if (!isQosNearlineRunning() && isQosNearlinePastExpiration()) {
                LOGGER.info("runScans: starting qosNearline system scans");
                start(true);
            }

            if (!isOnlineRunning()) {
                /*
                 *  If online is enabled, do the direct namespace scan;
                 *  otherwise, schedule a pool scan.
                 */
                if (isOnlinePastExpiration()) {
                    if (onlineScanEnabled) {
                        LOGGER.info("runScans: starting ONLINE system scans");
                        start(false);
                    } else {
                        LOGGER.info("runScans: starting IDLE POOL scans");
                        startPoolScans();
                    }
                }
            }
        } catch (CacheException e) {
            LOGGER.error("runScans failed: {}, cause {}.", e.getMessage(),
                  String.valueOf(Throwables.getRootCause(e)));
        } finally {
            lock.unlock();
        }
    }

    public void setCounters(QoSScannerCounters counters) {
        this.counters = counters;
    }

    public void setHandler(SysOpHandler handler) {
        this.handler = handler;
    }

    public void setQosNearlineBatchSize(Integer qosNearlineBatchSize) {
        this.qosNearlineBatchSize = qosNearlineBatchSize;
    }

    public void setOnlineScanEnabled(boolean enabled) {
        onlineScanEnabled = enabled;
    }

    public void setQosNearlineRescanWindow(int qosNearlineRescanWindow) {
        this.qosNearlineRescanWindow = qosNearlineRescanWindow;
    }

    public void setQosNearlineRescanWindowUnit(TimeUnit qosNearlineRescanWindowUnit) {
        this.qosNearlineRescanWindowUnit = qosNearlineRescanWindowUnit;
    }

    public void setOnlineBatchSize(Integer onlineBatchSize) {
        this.onlineBatchSize = onlineBatchSize;
    }

    public void setOnlineRescanWindow(int onlineRescanWindow) {
        this.onlineRescanWindow = onlineRescanWindow;
    }

    public void setOnlineRescanWindowUnit(TimeUnit onlineRescanWindowUnit) {
        this.onlineRescanWindowUnit = onlineRescanWindowUnit;
    }

    public void setPoolOperationMap(PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    public void startScan(boolean qos) throws PermissionDeniedCacheException {
        lock.lock();
        try {
            if (qos) {
                if (isQosNearlineRunning()) {
                    throw new PermissionDeniedCacheException("qosNearline scans are already running; "
                          + "use cancel and then call start again.");
                }
                start(true);
            } else {
                if (isOnlineRunning()) {
                    throw new PermissionDeniedCacheException(onlineScanEnabled ? "online" : "pool"
                          + " scans are already running; "
                          + "use cancel and then call start again.");
                }

                if (onlineScanEnabled) {
                    start(false);
                } else {
                    startPoolScans();
                }
            }
        } catch (CacheException e) {
            LOGGER.info("trouble starting scan: {}.", e.toString());
        } finally {
            lock.unlock();
        }
    }

    public void update(String id, boolean failed) {
        lock.lock();
        try {
            SystemScanOperation operation = get(id);
            if (operation != null) {
                operation.incrementCompleted(failed);
                if (operation.isComplete()) {
                    handleDone(operation);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void update(String id, long runningTotal, CacheException exception) {
        lock.lock();
        try {
            SystemScanOperation operation = get(id);
            if (operation != null) {
                operation.incrementCurrent(runningTotal);
                operation.exception = exception;
                if (runningTotal == 0 || operation.isComplete()) {
                    handleDone(operation);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    protected void clear() {
        lock.lock();
        try {
            online.clear();
            qosNearline.clear();
            history.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void recordSweep(long start, long end) {
        // NOP
    }

    private void cancel(SystemScanOperation operation) {
        operation.cancel();
        if (operation.task != null) {
            operation.task.cancel("qosNearline admin command");
        }
        history.add(operation.toString());
        handler.handleScanCancelled(operation.id);
    }

    @GuardedBy("lock")
    private SystemScanOperation get(String id) {
        SystemScanOperation operation = online.get(id);
        if (operation == null) {
            operation = qosNearline.get(id);
        }
        return operation;
    }

    private int getBatchSize(boolean qosNearline) {
        return qosNearline ? qosNearlineBatchSize : onlineBatchSize;
    }

    @GuardedBy("lock")
    private void handleDone(SystemScanOperation operation) {
        operation.scanLabel = ScanLabel.FINISHED;
        remove(operation.id);
        history.add(operation.toString());

        boolean isQosPermanent = operation.qos;

        if (operation.isFinal()) {
            if (isQosPermanent && qosNearline.isEmpty()) {
                state &= (~QOS_NEARLINE);
                lastQosNearlineScanEnd = System.currentTimeMillis();
            } else if (online.isEmpty()) {
                state &= (~ONLINE);
                lastOnlineScanEnd = System.currentTimeMillis();
            }
        } else {
            int loopWidth = maxConcurrentRunning;
            int batchSize = getBatchSize(isQosPermanent);
            long fromIndex = (operation.from / batchSize) + loopWidth;
            long toIndex = (operation.to / batchSize) + loopWidth;
            submit(fromIndex, toIndex, operation.minMaxIndices, isQosPermanent);
        }
    }

    private boolean isOnlineRunning() {
        return (state & ONLINE) == ONLINE;
    }

    private boolean isQosNearlineRunning() {
        return (state & QOS_NEARLINE) == QOS_NEARLINE;
    }

    private boolean isQosNearlinePastExpiration() {
        return System.currentTimeMillis() - lastQosNearlineScanEnd
              >= qosNearlineRescanWindowUnit.toMillis(
              qosNearlineRescanWindow);
    }

    private boolean isOnlinePastExpiration() {
        return System.currentTimeMillis() - lastOnlineScanEnd >= onlineRescanWindowUnit.toMillis(
              onlineRescanWindow);
    }

    @GuardedBy("lock")
    private void put(SystemScanOperation operation) {
        if (operation.isQos()) {
            qosNearline.put(operation.id, operation);
        } else {
            online.put(operation.id, operation);
        }
    }

    @GuardedBy("lock")
    private SystemScanOperation remove(String id) {
        SystemScanOperation operation = online.remove(id);
        if (operation == null) {
            operation = qosNearline.remove(id);
        }
        return operation;
    }

    /*
     *  Launch the first tasks up to the max concurrent.
     */
    @GuardedBy("lock")
    private void start(boolean qos) throws CacheException {
        if (!onlineScanEnabled && !qos) {
            LOGGER.info("start: overriding disabled flag to run online scan");
        }

        long[] indices = handler.getMinMaxIndices(qos);
        int count = maxConcurrentRunning;

        if (indices[1] == 0) {
            LOGGER.info("start: no {} entries to scan.", qos ? "QOS_NEARLINE" : "ONLINE");
            return;
        }

        LOGGER.info("start: loop count {}.", count);
        for (int i = 0; i < count; ++i) {
            LOGGER.info("start: submitting {} scan {}.", qos ? "QOS_NEARLINE" : "ONLINE", i);
            if (submit(i, i + 1, indices, qos) > indices[1]) {
                break;
            }
        }

        if (qos) {
            lastQosNearlineScanStart = System.currentTimeMillis();
            state |= QOS_NEARLINE;
        } else {
            lastOnlineScanStart = System.currentTimeMillis();
            state |= ONLINE;
        }
    }

    @GuardedBy("lock")
    private void startPoolScans() {
        LOGGER.info("runScans: starting Pools scans");
        poolOperationMap.scan(ALL_IDLE_ENABLED_POOLS);
        lastPoolScanStart = System.currentTimeMillis();
        nextPoolScanStart = lastPoolScanStart + onlineRescanWindowUnit.toMillis(onlineRescanWindow);
    }

    @GuardedBy("lock")
    private long submit(long fromIndex, long toIndex, long[] minmax, boolean qos) {
        int batchSize = getBatchSize(qos);
        long start = minmax[0] + (fromIndex * batchSize);
        long end = Math.min(minmax[0] + (toIndex * batchSize), minmax[1]);
        if (start > end) {
            return end;
        }
        SystemScanOperation operation = new SystemScanOperation(start, end, qos);
        operation.minMaxIndices = minmax;
        operation.lastScan = System.currentTimeMillis();
        submit(operation);
        return end;
    }

    @GuardedBy("lock")
    private void submit(SystemScanOperation operation) {
        operation.task = new SystemScanTask(operation.id, operation.from, operation.to,
              operation.qos, handler);
        operation.task.setErrorHandler(
              e -> LOGGER.info("Error during system scan: {}.", e.toString()));
        LOGGER.info("Submitting system scan task for operation {}, start index {}, end index {}.",
              operation.id, operation.from, operation.to);
        put(operation);
        operation.task.submit();
    }
}