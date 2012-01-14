package org.dcache.services.billing.cells;

import diskCacheV111.vehicles.*;

import org.dcache.cells.CellMessageReceiver;
import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PnfsBaseInfo;
import org.dcache.services.billing.db.data.PoolCostData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingStorageException;
import org.dcache.services.billing.db.exceptions.BillingInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Required;

/**
 * This class is responsible for the processing of messages from
 * other domains regarding transfers and pool usage. It calls out to a
 * IBillingInfoAccess implementation to handle persistence of the
 * data.
 *
 * @see IBillingInfoAccess
 */
public final class BillingDatabase
    implements CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(BillingDatabase.class);

    private IBillingInfoAccess _access;

    public void messageArrived(InfoMessage info)
    {
        try {
            _access.put(convert(info));
        } catch (BillingStorageException e) {
            _log.error("Can't log billing via BillingInfoAccess: " +
                       e.getMessage(), e);
            _log.info("Trying to reconnect");

            try {
                _access.close();
                _access.initialize();
            } catch (BillingInitializationException ex) {
                _log.error("Could not restart BillingInfoAccess: {}",
                            ex.getMessage());
            }
        }
    }

    /**
     * Converts from the InfoMessage type to the storage type.
     *
     * @param info
     * @return storage object
     */
    private PnfsBaseInfo convert(InfoMessage info) {
        if (info instanceof MoverInfoMessage) {
            return new MoverData((MoverInfoMessage) info);
        }
        if (info instanceof DoorRequestInfoMessage) {
            return new DoorRequestData((DoorRequestInfoMessage) info);
        }
        if (info instanceof StorageInfoMessage) {
            return new StorageData((StorageInfoMessage) info);
        }
        if (info instanceof PoolCostInfoMessage) {
            return new PoolCostData((PoolCostInfoMessage) info);
        }
        if (info instanceof PoolHitInfoMessage) {
            return new PoolHitData((PoolHitInfoMessage) info);
        }
        return null;
    }

    @Required
    public void setAccess(IBillingInfoAccess access) {
        _access = access;
    }
}
