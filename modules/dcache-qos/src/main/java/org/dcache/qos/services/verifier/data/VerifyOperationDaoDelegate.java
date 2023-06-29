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

import static org.dcache.qos.data.QoSAction.WAIT_FOR_STAGE;
import static org.dcache.qos.data.QoSMessageType.ADD_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CLEAR_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CORRUPT_FILE;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_DOWN;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_UP;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED_CANCELED;
import static org.dcache.qos.data.QoSMessageType.SYSTEM_SCAN;
import static org.dcache.qos.data.QoSMessageType.VALIDATE_ONLY;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.ABORTED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.CANCELED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.DONE;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.FAILED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.READY;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.RUNNING;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.WAITING;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao.VerifyOperationCriterion;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.util.SignalAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queues and running index which front for persistent storage implementing the dao interface.
 * <p/>
 * Some methods which count or list entries do so directly on the storage. The non-filtered cancel
 * methods are not supported, since they would cause bottlenecks (the filtered cancellation is
 * executed by the main consumer thread of the containing map implementation).
 * <p/>
 * Because the in-memory presence of the operation is coterminous with its execution lifecycle, this
 * is not a traditional cache which uses eviction or soft references.
 * <p/>
 * Notwithstanding, the max running and capacity settings are configurable.  It is advisable to keep
 * the latter >> the former, for reasons of efficiency (avoiding frequent fetches of READY
 * operations from persistent store).
 */
public class VerifyOperationDaoDelegate implements VerifyOperationDelegate {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyOperationDaoDelegate.class);

    private static final VerifyOperationState[] TERMINAL_STATES = {DONE, FAILED, ABORTED, CANCELED};

    /**
     * The READY queue types.
     */
    enum Queue {
        ADD(0,
              "from a location or cancellation request",
              new QoSMessageType[]{ADD_CACHE_LOCATION}),
        CLR(1, "from a clear location request",
              new QoSMessageType[]{CLEAR_CACHE_LOCATION, CORRUPT_FILE}),
        PLS(2,
              "from a pool scan request",
              new QoSMessageType[]{POOL_STATUS_UP, POOL_STATUS_DOWN}),
        MOD(3,
              "from a qos modification request",
              new QoSMessageType[]{QOS_MODIFIED, QOS_MODIFIED_CANCELED, VALIDATE_ONLY}),
        SYS(4,
              "from system scan request",
              new QoSMessageType[]{SYSTEM_SCAN});

        int index;
        String description;
        QoSMessageType[] messageTypes;

        Queue(int index, String description, QoSMessageType[] messageTypes) {
            this.index = index;
            this.description = description;
            this.messageTypes = messageTypes;
        }

        static Queue at(int i) {
            switch (i) {
                case 0:
                    return ADD;
                case 1:
                    return CLR;
                case 2:
                    return PLS;
                case 3:
                    return MOD;
                case 4:
                    return SYS;
                default:
                    throw new IllegalArgumentException("No such ready queue with index " + i);
            }
        }

        static int indexFromMessageType(QoSMessageType type) {
            switch (type) {
                case CORRUPT_FILE:
                    return CLR.index;
                case CLEAR_CACHE_LOCATION:
                    return CLR.index;
                case ADD_CACHE_LOCATION:
                    return ADD.index;
                case POOL_STATUS_DOWN:
                    return PLS.index;
                case POOL_STATUS_UP:
                    return PLS.index;
                case QOS_MODIFIED:
                    return MOD.index;
                case QOS_MODIFIED_CANCELED:
                    return MOD.index;
                case VALIDATE_ONLY:
                    return MOD.index;
                case SYSTEM_SCAN:
                    return SYS.index;
                default:
                    throw new IllegalArgumentException(
                          "No such ready queue serving message of type " + type);
            }
        }
    }

    /**
     * The current set of operations in the RUNNING state. The handlers access this map first.
     * <p/>
     * A miss here means the operation most likely is in the WAITING state; given that staging
     * requests can be long lived and numerous, these are not held in memory.
     */
    private final Map<PnfsId, VerifyOperation> running = new HashMap<>();

    /**
     * The order for election to run is FIFO.  The operation is removed from these waiting queues
     * when it is submitted to run.
     */
    private final Deque<VerifyOperation>[] queues = new Deque[]{
          new LinkedList<VerifyOperation>(), // ADD
          new LinkedList<VerifyOperation>(), // CLR
          new LinkedList<VerifyOperation>(), // PLS
          new LinkedList<VerifyOperation>(), // MOD
          new LinkedList<VerifyOperation>()  // SYS
    };

    /*
     *  For consistency between queues and database.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock write = lock.writeLock();
    private final Lock read = lock.readLock();

    /*
     *  When a query finishes, the callback is notified.
     */
    private SignalAware callback;

    /*
     *  Data persistence for the operations.
     */
    private VerifyOperationDao dao;

    /*
     *  Executor service for refreshing the queues.
     */
    private ExecutorService queueSupplier;

    /*
     *  Maximum elements in the queues.  This should be significantly larger than max running.
     */
    private int capacity = 10000;

    /*
     *  This should be set close to the maxRunning value for the adjuster service map.
     */
    private int maxRunning = 200;

    /*
     *  Used in connection with the clock algorithm governing next().
     */
    private int lastReadyIndex = 0;

    @Override
    public Map<String, Long> aggregateCounts(String classifier) {
        read.lock();
        try {
            return dao.counts(dao.where().classifier(classifier));
        } finally {
            read.unlock();
        }
    }

    /*
     *  Used when electing the next READY operations (how many free slots there are).
     */
    public int available() {
        return maxRunning - running();
    }

    /*
     *  This only marks the operations as canceled in
     *  the database because they need to be post-processed first.
     */
    public void cancel(VerifyOperationCancelFilter filter) {
        write.lock();
        try {
            dao.update(filter.getCriterion(dao), filter.getUpdate(dao));
        } finally {
            write.unlock();
        }
    }

    @Override
    public void cancel(PnfsId pnfsId, boolean remove) {
        throw new UnsupportedOperationException(
              "cancel(PnfsId, boolean) is not supported by the delegate; "
                    + "use the delegating map instead.");
    }

    @Override
    public void cancelFileOpForPool(String pool, boolean onlyParent) {
        throw new UnsupportedOperationException(
              "cancelFileOpForPool is not supported by the delegate; "
                    + "use the delegating map instead.");
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public VerifyOperationDelegate callback(SignalAware callback) {
        this.callback = callback;
        return this;
    }

    public int count(VerifyOperationFilter filter) {
        read.lock();
        try {
            return dao.count(filter.toCriterion(dao));
        } finally {
            read.unlock();
        }
    }

    /*
     *  Creates the operation or updates it if already present.
     *  Operation is written to the store but not to the queues, to preserve
     *  the prioritization by arrival time (since gaps may be created when
     *  the queues are close to max capacity).
     *  If a duplicate request arrives with a second subject, the original subject
     *  is not changed.  REVISIT
     */
    public boolean createOrUpdateOperation(FileQoSUpdate data) {
        PnfsId pnfsId = data.getPnfsId();
        String storageUnit = data.getStorageUnit();
        QoSMessageType type = data.getMessageType();
        boolean isParent =
              type == POOL_STATUS_DOWN || type == POOL_STATUS_UP || type == SYSTEM_SCAN;
        String pool = data.getPool();
        String parent = null;
        String source = null;

        if (isParent) {
            parent = pool;
        } else {
            source = pool;
        }

        long now = System.currentTimeMillis();

        VerifyOperation operation = new VerifyOperation(pnfsId);
        operation.setArrived(now);
        operation.setLastUpdate(now);
        operation.setMessageType(type);
        operation.setStorageUnit(storageUnit);
        operation.setParent(parent);
        operation.setSource(source);
        operation.setRetried(0);
        operation.setNeeded(0);
        operation.setState(READY);
        operation.setSubject(data.getSubject());

        write.lock();
        try {
            if (dao.store(operation)) {
                callback.signal();
                LOGGER.debug("createOrUpdateOperation, stored operation for {}.", pnfsId);
                return true;
            } else {
                /*
                 *  If the message type is now a pool status type
                 *  and the current type is a system scan, promote it.
                 */
                if (operation.getMessageType() == SYSTEM_SCAN &&
                      (type == POOL_STATUS_DOWN || type == POOL_STATUS_UP)) {
                    operation.setMessageType(type);
                }
                operation.setStorageUnit(storageUnit);
                dao.update(dao.whereUnique().pnfsId(pnfsId), dao.set().storageUnit(storageUnit));
                LOGGER.debug("createOrUpdateOperation, updated operation for {}, sunit {}.",
                      pnfsId, storageUnit);
                return false;
            }
        } catch (QoSException e) {
            LOGGER.error("createOrUpdateOperation, could not store operation for {}: {}.", pnfsId,
                  e.toString());
            return false;
        } finally {
            write.unlock();
        }
    }

    public VerifyOperation getRunning(PnfsId pnfsId) {
        read.lock();
        try {
            return running.get(pnfsId);
        } finally {
            read.unlock();
        }
    }

    /*
     *  We don't really care if the reads here reflect the absolute state of both memory
     *  and disk, so we don't bother to lock.
     */
    public String list(VerifyOperationFilter filter, int limit) {
        List<String> list = dao.get(filter.toCriterion(dao), limit).stream()
              .map(VerifyOperation::toString)
              .collect(Collectors.toList());
        if (list.isEmpty()) {
            return "NO (MATCHING) OPERATIONS.\n";
        }

        StringBuilder builder
              = new StringBuilder("MATCHED OPERATIONS:\t\t").append(list.size()).append("\n\n");
        list.forEach(op -> builder.append(op).append("\n"));

        return builder.toString();
    }

    @Override
    public int maxRunning() {
        return maxRunning;
    }

    /*
     *  Dequeues the next READY operation, if it exists.  Uses a simple clock algorithm
     *  to alternate among the queues.
     *  <p/>
     */
    public VerifyOperation next() {
        VerifyOperation operation = null;
        write.lock();
        try {
            for (int i = 0; i < queues.length; ++i) {
                operation = queues[lastReadyIndex].poll();

                lastReadyIndex = (lastReadyIndex + 1) % queues.length;

                if (operation != null) {
                    break;
                }
            }
        } finally {
            write.unlock();
        }

        return operation;
    }

    /*
     *  Checks each queue to see if it is empty.  If it is, its message types are
     *  added to a query to see if there are READY operations.  The entire set is
     *  passed off to the queueSupplier thread which will repopulate the empty queues
     *  from the persistent store.
     */
    public void refresh() {
        List<QoSMessageType> toRefresh = new LinkedList<>();
        read.lock();
        try {
            for (int i = 0; i < queues.length; ++i) {
                if (queues[i].peek() == null) {
                    toRefresh.addAll(Arrays.asList(Queue.at(i).messageTypes));
                }
            }

            if (!toRefresh.isEmpty()) {
                QoSMessageType[] messageTypes = toRefresh.toArray(QoSMessageType[]::new);
                int ready = dao.count(dao.where().state(READY).messageType(messageTypes));
                if (ready > 0) {
                    LOGGER.trace("next, toRefresh {}, ready {}.", toRefresh, ready);
                    queueSupplier.submit(this::doRefresh);
                }
            }
        } finally {
            read.unlock();
        }
    }

    /*
     *  Resets to READY operations that were in the RUNNING or WAITING state when the service
     *  last stopped so they can be reprocessed.  The queues are then refreshed.
     */
    public void reload() {
        dao.update(dao.where().state(RUNNING, WAITING), dao.set().state(READY));
        if (dao.count(dao.where().state(READY)) > 0) {
            queueSupplier.submit(this::doRefresh);
        }
    }

    /*
     *  Operation has completed its work or has failed fatally.
     */
    public void remove(PnfsId pnfsId) {
        write.lock();
        try {
            running.remove(pnfsId);
            dao.delete(dao.whereUnique().pnfsId(pnfsId));
        } finally {
            write.unlock();
        }
    }

    public void remove(VerifyOperationCancelFilter filter) {
        write.lock();
        try {
            List.copyOf(running.values()).stream()
                  .filter(filter.getPredicate()).forEach(o -> running.remove(o.getPnfsId()));

            for (int i = 0; i < queues.length; ++i) {
                int index = i;
                List.copyOf(queues[index]).stream()
                      .filter(filter.getPredicate()).forEach(o -> queues[index].remove(o));
            }

            if (filter.shouldRemove()) {
                dao.delete(filter.getCriterion(dao));
            }
        } finally {
            write.unlock();
        }
    }

    /*
     *  Update done when there is more work, or a failure which can be retried occurs.
     *  The operation has already been removed from the running running by the time of this call.
     *  Unless it is to be put back to the front of the queue, only the stored value
     *  is updated.
     */
    @Override
    public void resetOperation(VerifyOperation operation, boolean retry) {
        LOGGER.debug("resetOperation {}, {}.", operation, retry);

        /*
         * For fairness, resets the updated timestamp
         * to arrival time if this is a retry or needed < 2.
         */
        operation.resetOperation(retry);

        LOGGER.debug("resetOperation, after reset {}.", operation);
        write.lock();
        try {
            dao.update(dao.whereUnique().pnfsId(operation.getPnfsId()),
                  dao.fromOperation(operation));
            if (retry || operation.getNeededAdjustments() < 2) {
                enqueueFirst(operation);
            }
        } catch (QoSException e) {
            LOGGER.error("resetOperation, could not update reset {}: {}", operation, e.toString());
        } finally {
            write.unlock();
        }
    }

    public int running() {
        read.lock();
        try {
            return running.size();
        } finally {
            read.unlock();
        }
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public void setDao(VerifyOperationDao dao) {
        this.dao = dao;
    }

    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }

    public void setQueueSupplier(ExecutorService queueSupplier) {
        this.queueSupplier = queueSupplier;
    }

    @Override
    public int size() {
        read.lock();
        try {
            return dao.count(dao.where());
        } finally {
            read.unlock();
        }
    }

    @Override
    public Collection<VerifyOperation> terminated() {
        write.lock();
        try {
            Collection<VerifyOperation> toProcess = dao.get(dao.where().state(TERMINAL_STATES),
                  capacity);
            toProcess.stream().map(VerifyOperation::getPnfsId).forEach(running::remove);
            LOGGER.debug("terminated, returning {}, running is now {}.", toProcess.size(),
                  running.size());
            return toProcess;
        } finally {
            write.unlock();
        }
    }

    /*
     * Terminal state update.
     */
    @Override
    public void updateOperation(PnfsId pnfsId, CacheException error) {
        write.lock();
        try {
            VerifyOperation operation = running.get(pnfsId);
            if (operation == null) {
                /*
                 *  operation is likely in the WAITING state.
                 */
                operation = dao.get(dao.whereUnique().pnfsId(pnfsId));
            }

            LOGGER.trace("updateOperation {} ({}) {}.", pnfsId, error, operation);

            if (operation == null) {
                /*
                 *  It is possible that a cancellation could remove an operation which is
                 *  in return flight to the verifier, so we just treat this benignly.
                 */
                LOGGER.info("{} was no longer present when updated.", pnfsId);
                return;
            }

            /*
             *  Operation is updated in memory, and left in the running map (if it is there)
             *  until the terminal processing thread removes (#terminated).
             */
            if (operation.updateOperation(error)) {
                LOGGER.debug("updated operation {}.", operation);
                dao.update(dao.whereUnique().pnfsId(pnfsId), dao.fromOperation(operation));
                callback.signal();
            }
        } catch (QoSException e) {
            LOGGER.debug("could not update operation for ({}, {}): {}.", pnfsId, error, e.toString());
        } finally {
            write.unlock();
        }
    }

    /*
     * Called after the operation held in memory has been updated.
     */
    @Override
    public void updateOperation(VerifyOperation operation, VerifyOperationState state) {
        PnfsId pnfsId = operation.getPnfsId();

        write.lock();
        try {
            if (state == RUNNING) {
                running.put(pnfsId, operation);
            }

            dao.update(dao.whereUnique().pnfsId(pnfsId), dao.set().state(state));
        } finally {
            write.unlock();
        }
    }

    /*
     * Update done by handler when it has finished determining the adjustment.
     * Called after the operation held in memory has been updated.
     */
    @Override
    public void updateOperation(QoSAdjustmentRequest request) {
        VerifyOperationState state = null;
        PnfsId pnfsId = request.getPnfsId();

        write.lock();
        try {
            if (request.getAction() == WAIT_FOR_STAGE) {
                running.remove(pnfsId);
                state = WAITING;
            }

            dao.update(dao.whereUnique().pnfsId(pnfsId),
                  dao.set()
                        .action(request.getAction())
                        .state(state)
                        .source(request.getSource())
                        .target(request.getTarget())
                        .poolGroup(request.getPoolGroup()));
        } finally {
            write.unlock();
        }
    }

    /**
     * Called after the handler has determined nothing needs to be done.
     */
    @Override
    public void voidOperation(VerifyOperation operation) {
        operation.voidOperation();
        PnfsId pnfsId = operation.getPnfsId();
        write.lock();
        try {
            dao.update(dao.whereUnique().pnfsId(pnfsId), dao.fromOperation(operation));
            callback.signal();
        } catch (QoSException e) {
            LOGGER.error("could not void operation {}: {}.", operation, e.toString());
        } finally {
            write.unlock();
        }
    }

    /*
     *  Triggered if any queue is empty and if there are ready operations in the store.
     *
     *  For each queue it will attempt to pull into memory up to cache capacity operations
     *  corresponding to queue message types.
     *
     *  Run on a separate thread.
     */
    private void doRefresh() {
        LOGGER.debug("doRefresh starting.");

        write.lock();
        try {
            for (int i = 0; i < queues.length; ++i) {
                if (queues[i].isEmpty()) {
                    populateQueue(i);
                }
            }
            callback.signal();
        } finally {
            write.unlock();
        }

        LOGGER.debug("signalled callback; refresh finished.");
    }

    /*
     *  ORDER BY default is "updated".  Prioritization sets it back
     *  to arrival time (done on resetOperation).
     *
     *  For fairness, we want to load the earliest of each of the separate types.
     */
    @GuardedBy("lock")
    private void populateQueue(int index) {
        Queue queue = Queue.at(index);
        LOGGER.debug("populateQueue {} ({}), message types {}.",
              queue.name(), queue.description, Arrays.asList(queue.messageTypes));
        VerifyOperationCriterion criterion = dao.where().state(READY)
              .messageType(queue.messageTypes);
        List<VerifyOperation> results = dao.get(criterion, capacity);
        results.forEach(queues[index]::addLast);
        LOGGER.debug("populateQueue {}, loaded {} operations.", queue.name(), queues[index].size());
    }

    @GuardedBy("lock")
    private void enqueueFirst(VerifyOperation operation) {
        int index = Queue.indexFromMessageType(operation.getMessageType());
        LOGGER.debug("enqueue; add first {} to {}.", operation.getPnfsId(), index);
        queues[index].addFirst(operation);
    }
}
