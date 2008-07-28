package diskCacheV111.pools;

import java.io.PrintWriter;

import org.apache.log4j.Logger;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.event.CacheEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
import diskCacheV111.util.event.CacheRepositoryEvent;

/**
 *
 * SpaceSweeped which removes files as soon as they become 'CACHED' and not 'STICKY'.
 *
 * @since 1.8.0-16
 *
 */
public class NoCachedFilesSpaceSweeper
    extends AbstractSpaceSweeper
{
    private final static Logger _log = Logger.getLogger(NoCachedFilesSpaceSweeper.class);

    private final CacheRepository _repository;

    public NoCachedFilesSpaceSweeper( PnfsHandler pnfs,
                         CacheRepository repository)
    {
        _repository = repository;
        _repository.addCacheRepositoryListener(this);
    }

    public long getLRUSeconds() {
        // forced by interface
        return 0;
    }

    public long getRemovableSpace() {
        // forced by interface
        return 0;
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
