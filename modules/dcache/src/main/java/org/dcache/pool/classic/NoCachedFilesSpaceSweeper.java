package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.PoolDataBeanProvider;
import org.dcache.pool.classic.json.SweeperData;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.SpaceSweeperPolicy;
import org.dcache.pool.repository.StateChangeEvent;

/**
 *
 * SpaceSweeper which removes files as soon as they become 'CACHED'
 * and not 'STICKY'.
 *
 * @since 1.8.0-16
 *
 */
public class NoCachedFilesSpaceSweeper
    extends AbstractStateChangeListener
    implements SpaceSweeperPolicy, PoolDataBeanProvider<SweeperData>
{
    private static final Logger _log =
        LoggerFactory.getLogger(NoCachedFilesSpaceSweeper.class);

    private Repository _repository;

    public NoCachedFilesSpaceSweeper()
    {
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
        _repository.addListener(this);
    }

    /**
     * Returns true if this file is removable. This is the case if the
     * file is not sticky and is cached (which under normal
     * circumstances implies that it is ready and not precious).
     */
    @Override
    public boolean isRemovable(CacheEntry entry)
    {
        return false;
    }

    @Override
    public long getLru()
    {
        return 0;
    }

    @Override
    public double getMargin() { return 0.0; }

    @Override
    public SweeperData getDataObject() {
        SweeperData info = new SweeperData();
        info.setLabel("No Cached Files Space Sweeper");
        info.setLruQueueSize(0);
        info.setLruTimestamp(0L);
        info.setMargin(0.0);
        return info;
    }

    @Override
    public void stateChanged(StateChangeEvent event)
    {
        try {
            if (event.getNewState() == ReplicaState.CACHED) {
                PnfsId id = event.getPnfsId();
                CacheEntry entry = event.getNewEntry();
                if (!entry.isSticky()) {
                    _repository.setState(id, ReplicaState.REMOVED,
                            "Replica is now cache-only on pool with no-cache policy");
                    _log.debug(entry.getPnfsId() + " removed: {}", event.getWhy());
                }
            }
        } catch (InterruptedException | CacheException e) {
            _log.warn("Failed to remove entry from repository ({}): {}",
                    event.getWhy(), e.getMessage() );
        }
    }
}
