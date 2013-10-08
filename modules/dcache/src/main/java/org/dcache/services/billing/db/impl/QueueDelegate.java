package org.dcache.services.billing.db.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;

/**
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
                    Collection<IPlotData> data = new ArrayList<IPlotData>();

                    /*
                     * blocks until non-empty
                     */
                    logger.trace("calling queue.take()");
                    data.add((IPlotData) queue.take());

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
                    } catch (BillingStorageException t) {
                        logger.warn("commit failed; retrying once ...");
                        try {
                            callback.commit(data);
                        } catch (BillingStorageException t1) {
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

        logger.debug("{} close exiting", this);
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

    public void handlePut(IPlotData data) throws BillingStorageException {
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
        doorConsumer = new Consumer("door request data consumer", doorQueue);
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

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    protected abstract void handlePut(DoorRequestData data)
                    throws BillingStorageException;

    protected abstract void handlePut(MoverData data)
                    throws BillingStorageException;

    protected abstract void handlePut(PoolHitData data)
                    throws BillingStorageException;

    protected abstract void handlePut(StorageData data)
                    throws BillingStorageException;

    protected abstract void initializeInternal()
                    throws BillingInitializationException;

    protected synchronized boolean isRunning() {
        return running;
    }

    protected synchronized void setRunning(boolean running) {
        this.running = running;
    }
}
