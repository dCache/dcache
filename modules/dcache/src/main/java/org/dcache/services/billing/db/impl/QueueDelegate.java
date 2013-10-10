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
package org.dcache.services.billing.db.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.histograms.data.IHistogramData;

/**
 * Abstraction for handling insert logic. Each type of object is given a
 * separate queue and mover. Allows the put() method to do special processing
 * (for instance, to front-end the database insert with a temporary file).
 * Provides hooks for monitoring.
 *
 * @author arossi
 */
public abstract class QueueDelegate {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected int maxBatchSize;
    protected int maxQueueSize;
    protected boolean dropMessagesAtLimit;
    protected AtomicLong dropped = new AtomicLong(0);
    protected AtomicLong committed = new AtomicLong(0);

    protected BlockingQueue moverQueue;
    protected BlockingQueue doorQueue;
    protected BlockingQueue storageQueue;
    protected BlockingQueue hitQueue;

    private Thread moverConsumer;
    private Thread doorConsumer;
    private Thread storageConsumer;
    private Thread hitConsumer;

    private BaseBillingInfoAccess callback;

    private boolean running;

    private class Consumer extends Thread {
        private BlockingQueue queue;

        private Consumer(String name, BlockingQueue queue) {
            super(name);
            this.queue = queue;
        }

        public void run() {
            try {
                while (isRunning()) {
                    Collection<IHistogramData> data = new ArrayList<IHistogramData>();

                    /*
                     * blocks until non-empty
                     */
                    logger.trace("calling queue.take()");
                    data.add((IHistogramData) queue.take());

                    /*
                     * add to data and remove from queue any accumulated entries
                     */
                    logger.trace("calling queue.drainTo(), queue size {}",
                                    queue.size());
                    queue.drainTo(data, maxBatchSize);

                    try {
                        logger.trace("calling commit");
                        callback.commit(data);
                        committed.addAndGet(data.size());
                    } catch (BillingQueryException t) {
                        logger.warn("commit failed; retrying once ...");
                        try {
                            callback.commit(data);
                        } catch (BillingQueryException t1) {
                            logger.error("commit retry failed, "
                                            + "{} inserts have been lost",
                                            data.size());
                            logger.debug("exception in run(), commit", t1);
                        }
                    }
                }
            } catch (InterruptedException t) {
                logger.warn("queue take() was interrupted; "
                                + "this is probably due to cell shutdown; "
                                + "exiting thread ...");
            } finally {
                setRunning(false);
            }
        }
    }

    public void close() {
        setRunning(false);

        if (moverConsumer != null) {
            moverConsumer.interrupt();
            try {
                moverConsumer.join();
            } catch (InterruptedException e) {
                logger.trace("join on moverConsumer interrupted");
            }
        }
        if (doorConsumer != null) {
            doorConsumer.interrupt();
            try {
                doorConsumer.join();
            } catch (InterruptedException e) {
                logger.trace("join on doorConsumer interrupted");
            }
        }
        if (storageConsumer != null) {
            storageConsumer.interrupt();
            try {
                storageConsumer.join();
            } catch (InterruptedException e) {
                logger.trace("join on storageConsumer interrupted");
            }
        }
        if (hitConsumer != null) {
            hitConsumer.interrupt();
            try {
                hitConsumer.join();
            } catch (InterruptedException e) {
                logger.trace("join on hitConsumer interrupted");
            }
        }

        logger.trace("{} close exiting", this);
    }

    public long getCommitted() {
        return committed.get();
    }

    public long getDropped() {
        return dropped.get();
    }

    public long getQueueSize() {
        return moverQueue.size() + doorQueue.size() + storageQueue.size()
                        + hitQueue.size();
    }

    public void handlePut(IHistogramData data) throws BillingQueryException {
        if (data instanceof MoverData) {
            handlePut((MoverData) data);
        } else if (data instanceof StorageData) {
            handlePut((StorageData) data);
        } else if (data instanceof DoorRequestData) {
            handlePut((DoorRequestData) data);
        } else if (data instanceof PoolHitData) {
            handlePut((PoolHitData) data);
        }
    }

    public void initialize() throws BillingInitializationException {
        initializeInternal();

        moverQueue = new LinkedBlockingQueue(maxQueueSize);
        doorQueue = new LinkedBlockingQueue(maxQueueSize);
        storageQueue = new LinkedBlockingQueue(maxQueueSize);
        hitQueue = new LinkedBlockingQueue(maxQueueSize);

        setRunning(true);

        moverConsumer = new Consumer("mover data consumer", moverQueue);
        doorConsumer = new Consumer("door requeust data consumer", doorQueue);
        storageConsumer = new Consumer("storage data consumer", storageQueue);
        hitConsumer = new Consumer("cache hit data consumer", hitQueue);

        moverConsumer.start();
        doorConsumer.start();
        storageConsumer.start();
        hitConsumer.start();
    }

    public void setCallback(BaseBillingInfoAccess callback) {
        this.callback = callback;
    }

    public void setDropMessagesAtLimit(boolean dropMessagesAtLimit) {
        this.dropMessagesAtLimit = dropMessagesAtLimit;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    protected abstract void handlePut(DoorRequestData data)
                    throws BillingQueryException;

    protected abstract void handlePut(MoverData data)
                    throws BillingQueryException;

    protected abstract void handlePut(PoolHitData data)
                    throws BillingQueryException;

    protected abstract void handlePut(StorageData data)
                    throws BillingQueryException;

    protected abstract void initializeInternal()
                    throws BillingInitializationException;

    protected synchronized boolean isRunning() {
        return running;
    }

    protected synchronized void setRunning(boolean running) {
        this.running = running;
    }
}
