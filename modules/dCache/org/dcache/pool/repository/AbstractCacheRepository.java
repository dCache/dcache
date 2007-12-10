package org.dcache.pool.repository;

import java.util.List;
import java.util.ArrayList;
import java.util.MissingResourceException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import dmg.util.Logable;

import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.repository.SpaceRequestable;
import diskCacheV111.repository.CacheRepository;
import diskCacheV111.repository.FairQueueAllocation;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
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
        new ArrayList<CacheRepositoryListener>();

    /**
     * Space monitor for bookkeeping.
     */
    private final SpaceMonitor _spaceMonitor;

    /**
     * Amount of precious space in the repository.
     */
    protected final AtomicLong _preciousSpace = new AtomicLong(0L);

    /**
     * Space reservation.
     */
    private final Object _spaceReservationLock = new Object();

    /**
     * Current amount of reserved space. Protected against concurrent
     * access by the monitor of the space monitor.
     */
    private long _reservedSpace = 0L;

    /**
     * Utility class to bridge between space monitor and repository
     * event system.
     */
    private class NeedSpace implements SpaceRequestable
    {
        public void spaceNeeded(long space) {
            processEvent(EventType.SPACE, new CacheNeedSpaceEvent(this, space));
        }
    }

    public AbstractCacheRepository()
    {
        _spaceMonitor  = new FairQueueAllocation(0);
        _spaceMonitor.addSpaceRequestListener(new NeedSpace());
    }

    /**
     * Add repository listener.
     */
    public void addCacheRepositoryListener(CacheRepositoryListener listener)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("adding listener: " + listener);
        }
        synchronized(_repositoryListners) {
            _repositoryListners.add(listener);
        }
    }

    /**
     * Remove repository listener.
     */
    public void removeCacheRepositoryListener(CacheRepositoryListener listener)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("removing listener: " + listener);
        }

        synchronized(_repositoryListners) {
            _repositoryListners.remove(listener);
        }
    }

    /**
     * @deprecated
     */
    public void setLogable(Logable logable)
    {

    }

    public void addSpaceRequestListener(SpaceRequestable listener)
    {
        throw new IllegalArgumentException("Not supported");
    }

    /**
     * Triggers listener notification.
     */
    public void processEvent(EventType type, CacheRepositoryEvent event)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("Broadcasting event: " + event + " type " + type);
        }

        synchronized (_repositoryListners) {
            switch (type) {
            case CACHED:
                try {
                    if (event.getRepositoryEntry().isPrecious()) {
                        _preciousSpace.addAndGet(-event.getRepositoryEntry().getSize());
                    }
                } catch (CacheException ignored) {
                }
                for (CacheRepositoryListener listener : _repositoryListners) {
                    listener.cached(event);
                }
                break;

            case PRECIOUS:
                try {
                    _preciousSpace.addAndGet(event.getRepositoryEntry().getSize());
                } catch (CacheException e) {
                    _log.error("failed to get entry size : " + e.getMessage() );
                }
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
                try {
                    CacheRepositoryEntry entry = event.getRepositoryEntry();
                    long size = entry.getSize();
                    freeSpace(size);
                    if (entry.isPrecious()) {
                        _preciousSpace.addAndGet(-size);
                    }
                } catch (CacheException ignored) {
                }
                for (CacheRepositoryListener listener : _repositoryListners) {
                    listener.destroyed(event);
                }
                break;

            case SCAN:
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

            case SPACE:
                if( event instanceof  CacheNeedSpaceEvent ) {
                    for (CacheRepositoryListener listener : _repositoryListners) {
                        listener.needSpace((CacheNeedSpaceEvent)event);
                    }
                } // else .... do we need to panic here?
                break;
            }
	}
    }

    /**
     * Move <i>space</i> bytes from free space into used space.
     * Remove some entries if space is needed. Blocks as long as space
     * is not available.
     */
    public void allocateSpace(long space) throws InterruptedException
    {
        _spaceMonitor.allocateSpace(space);
    }

    /**
     * Move <i>space</i> bytes from free space into used space.
     * Remove some entries if space is needed. Blocks as long as space
     * is not available, but at most <i>millis</i>.
     */
    public void allocateSpace(long space, long millis)
        throws InterruptedException, MissingResourceException
    {
        if (millis == SpaceMonitor.NONBLOCKING) {
            synchronized (_spaceMonitor) {
                if ((_spaceMonitor.getTotalSpace() -
                     _preciousSpace.get() -
                     _reservedSpace ) < ( 3 * space ) )
                    throw new
	                MissingResourceException("Not enough Space Left",
                                                 this.getClass().getName(),
                                                 "Space");
                _spaceMonitor.allocateSpace(space);
            }
        } else if (millis == SpaceMonitor.BLOCKING) {
            _spaceMonitor.allocateSpace(space);
        } else {
            _spaceMonitor.allocateSpace(space, millis);
        }
    }

    /**
     * Move <i>space</i> bytes from used into free space.
     */
    public void freeSpace(long space)
    {
        _spaceMonitor.freeSpace(space);
    }

    public long getFreeSpace()
    {
        return _spaceMonitor.getFreeSpace();
    }

    public long getTotalSpace()
    {
        return _spaceMonitor.getTotalSpace();
    }

    public void setTotalSpace(long space)
    {
        _spaceMonitor.setTotalSpace(space);
    }

    protected abstract void storeReservedSpace() throws CacheException;

    public void reserveSpace(long space, boolean blocking)
        throws CacheException, InterruptedException
    {
        if (space < 0L) {
            throw new
                IllegalArgumentException("Space to reserve must be > 0");
        }

        allocateSpace(space,
                      blocking
                      ? SpaceMonitor.BLOCKING
                      : SpaceMonitor.NONBLOCKING);

        synchronized (_spaceReservationLock) {
            _reservedSpace += space;
            try {
                storeReservedSpace();
            } catch (CacheException e) {
                _reservedSpace -= space;
                throw e;
            }
        }
    }

    public void freeReservedSpace(long space) throws CacheException
    {
        modifyReservedSpace(space, true);
    }

    public void modifyReservedSpace(long space, boolean freeSpace)
        throws CacheException
    {
        if (space < 0L) {
            throw new IllegalArgumentException("Space to free must be > 0");
        }

        if ((_reservedSpace - space) < 0L) {
            throw new IllegalArgumentException("Inconsistent space request (result<0)");
        }

        if (freeSpace) {
            freeSpace(space);
        }

        synchronized (_spaceReservationLock) {
            _reservedSpace -= space;
            try {
                storeReservedSpace();
            } catch (CacheException ee) {
                _reservedSpace += space;
                throw ee;
            }
        }
    }

    public void applyReservedSpace(long space) throws CacheException
    {
        modifyReservedSpace(space, false);
    }

    public long getPreciousSpace()
    {
        return _preciousSpace.get();
    }

    public long getReservedSpace()
    {
        synchronized (_spaceReservationLock) {
            return _reservedSpace;
        }
    }
}
