package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
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
    implements SpaceSweeperPolicy
{
    private final static Logger _log =
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
    public void stateChanged(StateChangeEvent event)
    {
        try {
            if (event.getNewState() == EntryState.CACHED) {
                PnfsId id = event.getPnfsId();
                CacheEntry entry = event.getNewEntry();
                if (!entry.isSticky()) {
                    _repository.setState(id, EntryState.REMOVED);
                    _log.debug(entry.getPnfsId() + " : removed.");
                }
            }
        } catch (InterruptedException | IllegalTransitionException | CacheException e) {
            _log.warn("Failed to remove entry from repository: " + e.getMessage() );
        }
    }
}
