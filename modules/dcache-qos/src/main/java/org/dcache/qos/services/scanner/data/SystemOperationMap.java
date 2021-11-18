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

import com.google.common.collect.EvictingQueue;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.qos.services.scanner.data.ScanOperation.ScanLabel;
import org.dcache.qos.services.scanner.handlers.SysOpHandler;
import org.dcache.qos.services.scanner.util.QoSScannerCounters;
import org.dcache.qos.services.scanner.util.SystemScanTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains two maps corresponding to ONLINE and NEARLINE operations. The runnable method checks
 * for the period window expiration and launches the background operations when appropriate.
 * <p/>
 * The ONLINE operations look at ONLINE REPLICA and CUSTODIAL files.  The NEARLINE operations look
 * at NEARLINE CUSTODIAL files which currently have one or more cached disk copies.  NEARLINE
 * REPLICA files (currently interpreted as volatile) are ignored.
 * <p/>
 * Since the NEARLINE scan can take a very long time, it is turned off by default. Its default batch
 * size is also lower than the ONLINE scans, to give the latter priority when running concurrently.
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
    private static final byte NEARLINE = 0x4;

    private final Map<String, SystemScanOperation> online = new HashMap<>();
    private final Map<String, SystemScanOperation> nearline = new HashMap<>();
    private final EvictingQueue<String> history = EvictingQueue.create(100);

    private volatile int state;

    private volatile Integer onlineRescanWindow;
    private volatile TimeUnit onlineRescanWindowUnit;

    private volatile Integer nearlineRescanWindow;
    private volatile TimeUnit nearlineRescanWindowUnit;
    private volatile boolean nearlineScanEnabled = false;

    private volatile int nearlineBatchSize = 200000;
    private volatile int onlineBatchSize = 500000;

    private long lastOnlineScanStart;
    private long lastOnlineScanEnd;
    private long lastNearlineScanStart;
    private long lastNearlineScanEnd;

    private SysOpHandler handler;
    private QoSScannerCounters counters;

    public SystemOperationMap() {
        long now = System.currentTimeMillis();
        lastOnlineScanStart = now;
        lastOnlineScanEnd = now;
        lastNearlineScanStart = now;
        lastNearlineScanEnd = now;
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

    public void cancelAll(boolean nearline) {
        lock.lock();
        try {
            if (nearline) {
                this.nearline.values().forEach(this::cancel);
                this.nearline.clear();
            } else {
                this.online.values().forEach(this::cancel);
                this.online.clear();
            }
        } finally {
            lock.unlock();
        }

        if (nearline) {
            state &= (~NEARLINE);
            lastNearlineScanEnd = System.currentTimeMillis();
        } else {
            state &= (~ONLINE);
            lastOnlineScanEnd = System.currentTimeMillis();
        }
    }

    public String configSettings() {
        return String.format("system online scan window %s %s\n"
                    + "system nearline scan is %s\n"
                    + "system nearline scan window %s %s\n"
                    + "max concurrent operations %s\n"
                    + "period set to %s %s\n\n",
              onlineRescanWindow,
              onlineRescanWindowUnit,
              nearlineScanEnabled ? "on" : "off",
              nearlineRescanWindow,
              nearlineRescanWindowUnit,
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
              .append(String.format("last online scan start %s\n"
                          + "last online scan end %s\n",
                    new Date(lastOnlineScanStart),
                    new Date(lastOnlineScanEnd)));
        long seconds = TimeUnit.MILLISECONDS.toSeconds(lastOnlineScanEnd - lastOnlineScanStart);
        if (seconds < 0L) {
            seconds = 0L;
        }
        counters.appendDHMSElapsedTime(seconds, SCAN_DURATION, builder);
        builder.append(String.format("last nearline scan start %s\n"
                    + "last nearline scan end %s\n",
              new Date(lastNearlineScanStart),
              new Date(lastNearlineScanEnd)));
        seconds = TimeUnit.MILLISECONDS.toSeconds(lastNearlineScanEnd - lastNearlineScanStart);
        if (seconds < 0L) {
            seconds = 0L;
        }
        counters.appendDHMSElapsedTime(seconds, SCAN_DURATION, builder);
        builder.append("\n");
        pw.print(builder);
    }

    public String getSystemScanStatus() {
        lock.lock();
        StringBuilder builder = new StringBuilder();
        try {
            online.entrySet().forEach(e ->
                  builder.append(e.getValue()).append("\n"));
            if (!online.isEmpty() && !nearline.isEmpty()) {
                builder.append("------------------------------------------------\n");
            }
            nearline.entrySet().forEach(e ->
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
            long now = System.currentTimeMillis();

            if (nearlineScanEnabled && (state & NEARLINE) != NEARLINE &&
                  now - lastNearlineScanEnd >= nearlineRescanWindowUnit.toMillis(
                        nearlineRescanWindow)) {
                LOGGER.info("runScans: starting NEARLINE system scans");
                start(true);
            }

            if ((state & ONLINE) != ONLINE &&
                  now - lastOnlineScanEnd >= onlineRescanWindowUnit.toMillis(onlineRescanWindow)) {
                LOGGER.info("runScans: starting ONLINE system scans");
                start(false);
            }
        } catch (CacheException e) {

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

    public void setNearlineBatchSize(Integer nearlineBatchSize) {
        this.nearlineBatchSize = nearlineBatchSize;
    }

    public void setNearlineRescanEnabled(boolean enableFull) {
        nearlineScanEnabled = enableFull;
    }

    public void setNearlineRescanWindow(int nearlineRescanWindow) {
        this.nearlineRescanWindow = nearlineRescanWindow;
    }

    public void setNearlineRescanWindowUnit(TimeUnit nearlineRescanWindowUnit) {
        this.nearlineRescanWindowUnit = nearlineRescanWindowUnit;
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

    public void startScan(boolean nearline) throws PermissionDeniedCacheException {
        lock.lock();
        try {
            if (nearline) {
                if ((state & NEARLINE) == NEARLINE) {
                    throw new PermissionDeniedCacheException("nearline scans are already running; "
                          + "use cancel and then call start again.");
                }

                if (!nearlineScanEnabled) {
                    LOGGER.info("overriding disabled flag to run nearline scan");
                }
            } else {
                if ((state & ONLINE) == ONLINE) {
                    throw new PermissionDeniedCacheException("online scans are already running; "
                          + "use cancel and then call start again.");
                }
            }

            try {
                start(nearline);
            } catch (CacheException e) {
                LOGGER.debug("trouble starting scan: {}.", e.toString());
            }
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
            nearline.clear();
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
            operation.task.cancel("qos admin command");
        }
        history.add(operation.toString());
        handler.handleScanCancelled(operation.id);
    }

    @GuardedBy("lock")
    private SystemScanOperation get(String id) {
        SystemScanOperation operation = online.get(id);
        if (operation == null) {
            operation = nearline.get(id);
        }
        return operation;
    }

    private int getBatchSize(boolean nearline) {
        return nearline ? nearlineBatchSize : onlineBatchSize;
    }

    @GuardedBy("lock")
    private void handleDone(SystemScanOperation operation) {
        operation.scanLabel = ScanLabel.FINISHED;
        remove(operation.id);
        history.add(operation.toString());

        boolean isNearline = operation.nearline;

        if (operation.isFinal()) {
            if (isNearline && nearline.isEmpty()) {
                state &= (~NEARLINE);
                lastNearlineScanEnd = System.currentTimeMillis();
            } else if (online.isEmpty()) {
                state &= (~ONLINE);
                lastOnlineScanEnd = System.currentTimeMillis();
            }
        } else {
            int loopWidth = maxConcurrentRunning;
            int batchSize = getBatchSize(isNearline);
            long fromIndex = (operation.from / batchSize) + loopWidth;
            long toIndex = (operation.to / batchSize) + loopWidth;
            submit(fromIndex, toIndex, operation.minMaxIndices, isNearline);
        }
    }

    @GuardedBy("lock")
    private void put(SystemScanOperation operation) {
        if (operation.isNearline()) {
            nearline.put(operation.id, operation);
        } else {
            online.put(operation.id, operation);
        }
    }

    @GuardedBy("lock")
    private SystemScanOperation remove(String id) {
        SystemScanOperation operation = online.remove(id);
        if (operation == null) {
            operation = nearline.remove(id);
        }
        return operation;
    }

    /*
     *  Launch the first tasks up to the max concurrent.
     */
    @GuardedBy("lock")
    private void start(boolean nearline) throws CacheException {
        if (!nearlineScanEnabled && nearline) {
            LOGGER.info("start: overriding disabled flag to run nearline scan");
        }

        long[] indices = handler.getMinMaxIndices();
        int count = maxConcurrentRunning;

        LOGGER.info("start: loop count {}.", count);
        for (int i = 0; i < count; ++i) {
            LOGGER.info("start: submitting {} scan {}.", nearline ? "NEARLINE" : "ONLINE", i);
            submit(i, i + 1, indices, nearline);
        }

        if (nearline) {
            lastNearlineScanStart = System.currentTimeMillis();
            state |= NEARLINE;
        } else {
            lastOnlineScanStart = System.currentTimeMillis();
            state |= ONLINE;
        }
    }

    @GuardedBy("lock")
    private void submit(long fromIndex, long toIndex, long[] minmax, boolean nearline) {
        int batchSize = getBatchSize(nearline);
        SystemScanOperation operation
              = new SystemScanOperation(minmax[0] + (fromIndex * batchSize),
              minmax[0] + (toIndex * batchSize),
              nearline);
        operation.minMaxIndices = minmax;
        operation.lastScan = System.currentTimeMillis();
        submit(operation);
    }

    @GuardedBy("lock")
    private void submit(SystemScanOperation operation) {
        operation.task = new SystemScanTask(operation.id, operation.from, operation.to,
              operation.nearline, handler);
        operation.task.setErrorHandler(
              e -> LOGGER.info("Error during system scan: {}.", e.toString()));
        LOGGER.info("Submitting system scan task for operation {}, start index {}, end index {}.",
              operation.id, operation.from, operation.to);
        put(operation);
        operation.task.submit();
    }
}