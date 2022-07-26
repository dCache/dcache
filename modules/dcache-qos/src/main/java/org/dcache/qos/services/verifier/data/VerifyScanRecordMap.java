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
package org.dcache.qos.services.verifier.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import diskCacheV111.util.PnfsId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.listeners.QoSPoolScanResponseListener;
import org.dcache.qos.services.verifier.handlers.VerifyAndUpdateHandler;
import org.dcache.qos.util.ErrorAwareTask;
import org.dcache.qos.vehicles.QoSScannerVerificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state of batched scan requests. Provides ability to cancel scan requests and to
 * determine when to update the scanner as to progress.
 */
public class VerifyScanRecordMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyScanRecordMap.class);

    class ScanRecord {

        private final AtomicInteger done = new AtomicInteger(0);
        private final AtomicInteger failures = new AtomicInteger(0);
        private final AtomicInteger arrived = new AtomicInteger(0);
        private final AtomicInteger currentSucceeded = new AtomicInteger(0);
        private final AtomicInteger currentFailed = new AtomicInteger(0);

        private final QoSMessageType type;
        private final String pool;
        private final String group;
        private final String storageUnit;
        private final boolean forced;

        private boolean cancelled = false;

        ScanRecord(String pool, QoSMessageType type, String group, String storageUnit,
              boolean forced) {
            this.pool = pool;
            this.type = type;
            this.group = group;
            this.storageUnit = storageUnit;
            this.forced = forced;
        }

        void cancel() {
            cancelled = true;
        }

        void updateArrived(int count) {
            arrived.addAndGet(count);
            LOGGER.trace("ScanRecord: update arrived with count {}; arrived is now {}",
                  count, arrived.get());
        }

        void updateTerminated(boolean failed) {
            done.incrementAndGet();
            if (failed) {
                failures.incrementAndGet();
                currentFailed.incrementAndGet();
            } else {
                currentSucceeded.incrementAndGet();
            }
        }

        void reset() {
            currentSucceeded.set(0);
            currentFailed.set(0);
        }

        boolean shouldNotify() {
            LOGGER.trace("shouldNotify, succeeded {}, failed {}, batch size {}, complete {}.",
                  currentSucceeded.get(), currentFailed.get(), batchSize, isComplete());
            return (currentSucceeded.get() + currentFailed.get() >= batchSize) || isComplete();
        }

        boolean isCancelled() {
            return cancelled;
        }

        boolean isComplete() {
            return cancelled || arrived.get() == done.get();
        }
    }

    class ScanTask extends ErrorAwareTask {

        ScanRecord record;
        List<PnfsId> replicas;
        Future future;

        public void run() {
            for (PnfsId pnfsId : replicas) {
                if (record.isCancelled()) {
                    break;
                }

                FileQoSUpdate data = new FileQoSUpdate(pnfsId, record.pool, record.type,
                      record.group,
                      record.storageUnit, record.forced);
                LOGGER.trace("calling handleUpdate for {}.", data.getPnfsId());
                verifyHandler.handleUpdate(data);
            }

            tasks.remove(record.pool, this);
        }
    }

    private final Map<String, ScanRecord> scanRecords = new HashMap<>();
    private final Multimap<String, ScanTask> tasks = HashMultimap.create();

    private VerifyAndUpdateHandler verifyHandler;
    private ExecutorService executor;
    private QoSPoolScanResponseListener scanResponseListener;
    private int batchSize = 200;

    public synchronized void cancel(String pool) {
        ScanRecord record = scanRecords.get(pool);
        if (record != null) {
            LOGGER.trace("cancelled scan record for {}.", pool);
            record.cancel();
        }
        LOGGER.trace("cancelling {} futures for {}.", tasks.get(pool).size(), pool);
        tasks.removeAll(pool).stream().forEach(t -> t.future.cancel(true));
    }

    public synchronized void updateArrived(QoSScannerVerificationRequest request) {
        QoSMessageType type = request.getType();
        String pool = request.getId();
        String group = request.getGroup();
        String storageUnit = request.getStorageUnit();
        List<PnfsId> replicas = request.getReplicas();
        int count = replicas.size();
        boolean forced = request.isForced();

        LOGGER.trace("updateArrived, pool {}, count {}.", pool, replicas.size());

        ScanRecord record = scanRecords.computeIfAbsent(pool,
              p -> new ScanRecord(pool, type, group, storageUnit, forced));

        if (record.isCancelled()) {
            LOGGER.trace("updateArrived, scan of {} cancelled.", pool);
            return;
        }

        record.updateArrived(count);

        if (!record.isCancelled()) {
            ScanTask task = new ScanTask();
            task.record = record;
            task.replicas = replicas;
            tasks.put(pool, task);
            task.future = executor.submit(task);
        }
    }

    public synchronized void updateCompleted(String pool, boolean failed) {
        ScanRecord record = scanRecords.get(pool);
        if (record != null) {
            record.updateTerminated(failed);
        }
    }

    public synchronized void checkForNotify(String pool) {
        ScanRecord record = scanRecords.get(pool);
        if (record != null && record.shouldNotify()) {
            scanResponseListener.scanRequestUpdated(record.type,
                  pool,
                  record.currentSucceeded.get(),
                  record.currentFailed.get());
            record.reset();
        }
    }

    public synchronized void fetchAndRemoveIfCompleted(String id) {
        ScanRecord record = scanRecords.get(id);
        if (record == null || !record.isComplete()) {
            LOGGER.trace("fetchAndRemoveIfCompleted, no record for {}.", id);
            return;
        }
        int failed = record.failures.get();
        int succeeded = record.done.get() - failed;
        LOGGER.trace("fetchAndRemoveIfCompleted, record for {} complete, succeeded {}, failed {}.",
              id, succeeded, failed);
        scanRecords.remove(id);
        tasks.removeAll(id);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setScanResponseListener(QoSPoolScanResponseListener scanResponseListener) {
        this.scanResponseListener = scanResponseListener;
    }

    public void setVerifyHandler(VerifyAndUpdateHandler verifyHandler) {
        this.verifyHandler = verifyHandler;
    }
}
