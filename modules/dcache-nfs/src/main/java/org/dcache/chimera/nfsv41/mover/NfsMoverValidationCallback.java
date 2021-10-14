package org.dcache.chimera.nfsv41.mover;

import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellPath;
import java.lang.ref.WeakReference;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.chimera.nfs.v4.xdr.stateid4;
import org.dcache.vehicles.DoorValidateMoverMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code DoorValidateMoverMessage} callback handler for NFS transfer service.
 */
public class NfsMoverValidationCallback extends
      AbstractMessageCallback<DoorValidateMoverMessage<org.dcache.chimera.nfs.v4.xdr.stateid4>> {

    private final Logger LOGGER = LoggerFactory.getLogger(NfsMoverValidationCallback.class);
    /**
     * A weak reference to the mover. If mover will be killed by some other path (admin command or
     * from the door), then we can simply forget about it.
     */
    private final WeakReference<NfsMover> moverRef;

    public NfsMoverValidationCallback(NfsMover mover) {
        moverRef = new WeakReference<>(mover);
    }

    @Override
    public void success(DoorValidateMoverMessage<stateid4> message) {
        if (!message.isIsValid()) {
            kill();
        }
    }

    @Override
    public void failure(int rc, Object error) {
        // effective NOP, as we can't safely decide to kill the mover
        LOGGER.info("Failed to send validation requests: {} ({})", error, rc);
    }

    @Override
    public void noroute(CellPath path) {
        // door is dead. All states (movers) are invalid.
        kill();
    }

    private void kill() {
        NfsMover mover = moverRef.get();
        if (mover != null) {
            LOGGER.warn("Killing abandoned mover: {}", mover);
            mover.disable(new CacheException(CacheException.THIRD_PARTY_TRANSFER_FAILED,
                  "Abandoned mover"));
        }
    }
}
