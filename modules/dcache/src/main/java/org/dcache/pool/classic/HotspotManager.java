package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellSetupProvider;
import java.util.HashSet;
import java.util.Set;

public class HotspotManager implements CellMessageReceiver, CellCommandListener, CellSetupProvider {

    private static final long defaultNumReplicas = 10;
    private static final long defaultHotspotThreshold = 5;

    private long _numReplicas = defaultNumReplicas;
    private long _hotspotThreshold = defaultHotspotThreshold;
    private Set<PnfsId> _inFlightMigrations;

    public HotspotManager() {
        _inFlightMigrations = new HashSet<PnfsId>();
    }

    void messageArrived(PoolIoFileMessage message) {
        _inFlightMigrations.remove(message.getPnfsId());
    }

    void maybeReplicate(PoolIoFileMessage message, long numPendingRequests) {
        if  (! (numPendingRequests < _hotspotThreshold || _inFlightMigrations.contains(message.getPnfsId())) {
            // TODO: Compose and send migration message.

            // Blacklist
            _inFlightMigrations.add(pnfsId);
        }
    }
}
