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

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.util.PnfsId;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.qos.services.verifier.data.VerifyOperationQueueIndex.QueueType;
import org.dcache.util.SignalAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Responsible for the management of a verify operation of a given message type.
 *  Uses three internal queues to track the state of the operation (running, ready, waiting).
 *  Calls back the manager when an operation is ready for post-processing.
 */
public class VerifyOperationQueue implements SignalAware, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyOperationQueue.class);

    private final Deque<PnfsId> running = new LinkedList<PnfsId>();
    private final Deque<PnfsId> ready = new LinkedList<PnfsId>();
    private final Deque<PnfsId> waiting = new LinkedList<PnfsId>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock write = lock.writeLock();
    private final Lock read = lock.readLock();

    /**
     * For reporting operations terminated or canceled while the consumer thread is doing work
     * outside the wait monitor.  Supports the SignalAware interface.
     */
    private final AtomicInteger signalled = new AtomicInteger(0);
    private final QueueType queueType;

    /**
     *  Callback to the manager is necessary for submission and termination.
     */
    private final VerifyOperationManager operationManager;

    /**
     *  Extracts the operation from the manager's map and checks to see if it is terminated.
     */
    private final Predicate<PnfsId> terminated;

    public VerifyOperationQueue(QueueType queueType, VerifyOperationManager operationManager) {
        this.queueType = queueType;
        this.operationManager = operationManager;
        terminated = id -> {
            VerifyOperation op = this.operationManager.get(id);
            return op == null || op.isInTerminalState();
        };
    }

    public boolean addFirst(PnfsId pnfsId) {
        write.lock();
        try {
            return ready.offerFirst(pnfsId);
        } finally {
            write.unlock();
            signal();
        }
    }

    public boolean addLast(PnfsId pnfsId) {
        write.lock();
        try {
            return ready.offerLast(pnfsId);
        } finally {
            write.unlock();
            signal();
        }
    }

    @Override
    public int countSignals() {
        return signalled.get();
    }

    public QueueType getQueueType() {
        return queueType;
    }

    public boolean isRunning(PnfsId pnfsId) {
        read.lock();
        try {
            return running.contains(pnfsId);
        } finally {
            read.unlock();
        }
    }

    public boolean isWaiting(PnfsId pnfsId) {
        read.lock();
        try {
            return waiting.contains(pnfsId);
        } finally {
            read.unlock();
        }
    }

    public boolean isReady(PnfsId pnfsId) {
        read.lock();
        try {
            return ready.contains(pnfsId);
        } finally {
            read.unlock();
        }
    }

    public void run() {
        try {
            while (!Thread.interrupted()) {
                LOGGER.trace("{} queue calling scan.", queueType);

                signalled.set(0);

                processTerminated();
                processReady();

                if (signalled.get() > 0) {
                    /*
                     *  Give the operations completed during the scan a chance
                     *  to free slots immediately, if possible, by rescanning now.
                     */
                    LOGGER.trace("{} queue processing complete, received {} signals; "
                                + "rechecking for requeued operations ...",
                          queueType, signalled.get());
                    continue;
                }

                if (Thread.interrupted()) {
                    break;
                }

                LOGGER.trace("{} queue processing complete, waiting ...", queueType);
                await();
            }
        } catch (InterruptedException e) {
            LOGGER.trace("{} queue  processing was interrupted.", queueType);
        }

        LOGGER.info("Exiting {} queue processing.", queueType);
    }

    @Override
    public synchronized void signal() {
        signalled.incrementAndGet();
        notifyAll();
    }

    public void updateToWaiting(PnfsId pnfsId) {
        write.lock();
        try {
            running.remove(pnfsId);
            waiting.addLast(pnfsId);
        } finally {
            write.unlock();
        }
    }

    @VisibleForTesting
    void processReady() {
        List<PnfsId> next = new ArrayList<>();

        write.lock();
        try {
            int available = operationManager.getMaxRunning() - running.size();
            LOGGER.debug("{} queue, available to run: {}", queueType, available);
            while (available > 0 && !ready.isEmpty()) {
                PnfsId pnfsId = ready.removeFirst();
                running.addLast(pnfsId);
                next.add(pnfsId);
                --available;
            }
        } finally {
            write.unlock();
        }

        for (PnfsId pnfsId : next) {
            operationManager.submitToRun(pnfsId, queueType.executorService);
        }
    }

    @VisibleForTesting
    void processTerminated() {
        /*
         *  Will also gather the canceled operations for purposes of finalization/
         *  notification.
         */
        List<PnfsId> toProcess = terminated();
        LOGGER.debug("{} queue found {} terminated operations.", queueType, toProcess.size());
        toProcess.stream().forEach(operationManager::submitForPostProcessing);
        toProcess.clear();
    }

    private synchronized void await() throws InterruptedException {
        wait(operationManager.getTimeoutUnit().toMillis(operationManager.getTimeout()));
    }

    @GuardedBy("lock")
    private List<PnfsId> removeFrom(String name, Deque<PnfsId> deque) {
        LOGGER.debug("{} queue, {} before remove {}.", queueType, name, deque.size());
        List<PnfsId> from = deque.stream().filter(terminated).collect(Collectors.toList());
        from.forEach(deque::remove);
        LOGGER.debug("{} queue, {} after remove {}.", queueType, name, deque.size());
        return from;
    }

    private List<PnfsId> terminated() {
        write.lock();
        try {
            List<PnfsId> terminated = new ArrayList<>();
            terminated.addAll(removeFrom("running", running));
            terminated.addAll(removeFrom("waiting", waiting));
            terminated.addAll(removeFrom("ready", ready));
            LOGGER.debug("{} queue, terminated {}.", queueType, terminated.size());
            return terminated;
        } finally {
            write.unlock();
        }
    }
}
