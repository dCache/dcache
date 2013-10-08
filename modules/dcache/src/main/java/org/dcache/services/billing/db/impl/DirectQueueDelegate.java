package org.dcache.services.billing.db.impl;

import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.IPlotData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;

public class DirectQueueDelegate extends QueueDelegate {

    @Override
    protected void handlePut(DoorRequestData data)
                    throws BillingStorageException {
        if (!dropMessagesAtLimit) {
            try {
                doorQueue.put(data);
            } catch (InterruptedException t) {
                throw new BillingStorageException(t.getMessage(), t.getCause());
            }
        } else if (!doorQueue.offer(data)) {
            processDroppedData(data);
        }
    }

    @Override
    protected void handlePut(MoverData data) throws BillingStorageException {
        if (!dropMessagesAtLimit) {
            try {
                moverQueue.put(data);
            } catch (InterruptedException t) {
                throw new BillingStorageException(t.getMessage(), t.getCause());
            }
        } else if (!moverQueue.offer(data)) {
            processDroppedData(data);
        }
    }

    @Override
    protected void handlePut(PoolHitData data) throws BillingStorageException {
        if (!dropMessagesAtLimit) {
            try {
                hitQueue.put(data);
            } catch (InterruptedException t) {
                throw new BillingStorageException(t.getMessage(), t.getCause());
            }
        } else if (!hitQueue.offer(data)) {
            processDroppedData(data);
        }
    }

    @Override
    protected void handlePut(StorageData data) throws BillingStorageException {
        if (!dropMessagesAtLimit) {
            try {
                storageQueue.put(data);
            } catch (InterruptedException t) {
                throw new BillingStorageException(t.getMessage(), t.getCause());
            }
        } else if (!storageQueue.offer(data)) {
            processDroppedData(data);
        }
    }

    @Override
    protected void initializeInternal() throws BillingInitializationException {
    }

    private void processDroppedData(IPlotData data) {
        dropped.incrementAndGet();
        logger.info("encountered max queue limit; "
                        + "{} entries have been dropped", dropped.get());
        logger.debug("queue limit prevented storage of {}", data);
    }
}
