package org.dcache.pool.repository;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.MissingResourceException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.CacheRepositoryListener;

/**
 * Abstract implementation of the CacheRepository interface.
 *
 * This class implements the parts of the interface, which are
 * believed to be used by all implementations. This covers the event
 * listener registration, event propagation, space monitor delegation,
 * and accounting of precious and reserved space.
 *
 * It does not implement repository entry handling.
 */
public abstract class AbstractCacheRepository
    implements CacheRepository, EventProcessor
{
    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    /**
     * Registered event listeners.
     */
    private final List <CacheRepositoryListener> _repositoryListners =
        new CopyOnWriteArrayList<CacheRepositoryListener>();

    /**
     * Keeps track of how space is used.
     */
    protected Account _account;

    /**
     * Set of precious entries.
     */
    private final Set<PnfsId> _precious = new HashSet<PnfsId>();

    public AbstractCacheRepository()
    {
    }

    public void setAccount(Account account)
    {
        _account = account;
    }

    /**
     * Add repository listener.
     */
    public void addCacheRepositoryListener(CacheRepositoryListener listener)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("adding listener: " + listener);
        }
        _repositoryListners.add(listener);
    }

    /**
     * Remove repository listener.
     */
    public void removeCacheRepositoryListener(CacheRepositoryListener listener)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("removing listener: " + listener);
        }

        _repositoryListners.remove(listener);
    }

    /**
     * Adds an entry to the list of precious files. If the entry is
     * already in the list, then nothin happens.
     */
    private void addPrecious(CacheRepositoryEntry entry)
    {
        long size = entry.getSize();
        synchronized (_precious) {
            if (_precious.add(entry.getPnfsId())) {
                _account.adjustPrecious(size);
            }
        }
    }

    /**
     * Removes an entry from the list of precious files. If the entry
     * is not in the list, then nothing happens.
     */
    private void removePrecious(CacheRepositoryEntry entry)
    {
        long size = entry.getSize();
        synchronized (_precious) {
            if (_precious.remove(entry.getPnfsId())) {
                _account.adjustPrecious(-size);
            }
        }
    }

    /**
     * Triggers listener notification.
     */
    public void processEvent(EventType type, CacheRepositoryEvent event)
    {
        CacheRepositoryEntry entry;

        if (_log.isDebugEnabled()) {
            _log.debug("Broadcasting event: " + event + " type " + type);
        }

        switch (type) {
        case CACHED:
            removePrecious(event.getRepositoryEntry());
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.cached(event);
            }
            break;

        case PRECIOUS:
            addPrecious(event.getRepositoryEntry());
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.precious(event);
            }
            break;

        case CREATE:
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.created(event);
            }
            break;

        case REMOVE:
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.removed(event);
            }
            break;

        case TOUCH:
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.touched(event);
            }
            break;

        case DESTROY:
            entry = event.getRepositoryEntry();
            _account.free(entry.getSize());
            removePrecious(entry);
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.destroyed(event);
            }
            break;

        case SCAN:
            try {
                entry = event.getRepositoryEntry();
                if (!_account.allocateNow(entry.getSize())) {
                    throw new RuntimeException("File registration failed: Pool is out of space.");
                }
                if (entry.isPrecious()) {
                    addPrecious(entry);
                }
            } catch (CacheException ignored) {
            }

            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.scanned(event);
            }
            break;

        case AVAILABLE:
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.available(event);
            }
            break;

        case STICKY:
            for (CacheRepositoryListener listener : _repositoryListners) {
                listener.sticky(event);
            }
            break;
	}
    }
}
