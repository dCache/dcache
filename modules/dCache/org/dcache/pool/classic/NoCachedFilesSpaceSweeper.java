package org.dcache.pool.classic;

import org.apache.log4j.Logger;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.AbstractCacheRepositoryListener;

/**
 *
 * SpaceSweeped which removes files as soon as they become 'CACHED' and not 'STICKY'.
 *
 * @since 1.8.0-16
 *
 */
public class NoCachedFilesSpaceSweeper
    extends AbstractCacheRepositoryListener
{
    private final static Logger _log = 
        Logger.getLogger(NoCachedFilesSpaceSweeper.class);

    private final CacheRepository _repository;

    public NoCachedFilesSpaceSweeper(CacheRepository repository)
    {
        _repository = repository;
        _repository.addCacheRepositoryListener(this);
    }

    public void cached(CacheRepositoryEvent event) {

        CacheRepositoryEntry entry = event.getRepositoryEntry() ;
        try {
            _repository.removeEntry(entry);
            _log.debug(entry.getPnfsId() + " : removed.");
        } catch (CacheException e) {
            _log.error("Failed to remove entry from repository: " + e.getMessage() );
        }
    }
}
