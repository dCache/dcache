package org.dcache.pool.repository.v5;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;
import diskCacheV111.pools.SpaceSweeper;
import diskCacheV111.repository.RepositoryInterpreter;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.event.CacheRepositoryListener;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
import diskCacheV111.util.event.CacheEvent;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.repository.v4.CacheRepositoryV4;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.ReadHandle;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.SpaceRecord;
import static org.dcache.pool.repository.EntryState.*;


import com.sleepycat.je.DatabaseException;

import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.lang.reflect.Constructor;

public class CacheRepositoryV5// extends CellCompanion implements CacheRepository
    implements CacheRepositoryListener
{
    private final List<StateChangeListener> _listeners =
        new CopyOnWriteArrayList<StateChangeListener>();

    /**
     * Map to keep track of states. Temporary hack because we do not
     * have enough information in the actual repository.
     */
    private final Map<PnfsId, EntryState> _states =
        new HashMap<PnfsId, EntryState>();

    /** Cell used for communication. */
    private final CellAdapter _cell;

    /** Classic repository used for tracking entries. */
    private final CacheRepositoryV4 _repository;

    /** Cell command interpreter for the repository. */
    private final RepositoryInterpreter _interpreter;

    /** Handler for talking to the PNFS manager. */
    private final PnfsHandler _pnfs;

    /** Sweeper for GC cached files. */
    private final SpaceSweeper _sweeper;

    /**
     * Reader-writer lock used for most accesses.
     */
    private final ReadWriteLock _operationLock =
        new ReentrantReadWriteLock();

    private static final String DEFAULT_SWEEPER =
        "diskCacheV111.pools.SpaceSweeper0";
    private static final String DUMMY_SWEEPER =
        "diskCacheV111.pools.DummySpaceSweeper";

    /** Instantiates a new sweeper.
     */
    protected SpaceSweeper createSweeper(Args args)
        throws IllegalArgumentException
    {
        String sweeperClass = args.getOpt("sweeper");
        if (args.getOpt("permanent") != null)
            sweeperClass = DUMMY_SWEEPER;
        else if (sweeperClass == null)
            sweeperClass = DEFAULT_SWEEPER;

        try {
            Class<?>[] argClass = { dmg.cells.nucleus.CellAdapter.class,
                                    diskCacheV111.util.PnfsHandler.class,
                                    diskCacheV111.repository.CacheRepository.class,
                                    diskCacheV111.pools.HsmStorageHandler2.class };
            Class<?> c = Class.forName(sweeperClass);
            Constructor<?> con = c.getConstructor(argClass);
            return (SpaceSweeper)con.newInstance(_cell, _pnfs, _repository, null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not instantiate "
                                               + sweeperClass + ": "
                                               + e.getMessage());
        }
    }

    public CacheRepositoryV5(CellAdapter cell, PnfsHandler pnfs,
                             File base, Args args)
        throws IOException, RepositoryException
    {
        try {
            _cell = cell;
            _pnfs = pnfs;
            _repository = new CacheRepositoryV4(base, args);

            _sweeper = createSweeper(args);
            _interpreter = new RepositoryInterpreter(_cell, _repository);

            _cell.addCommandListener(_interpreter);
            _cell.addCommandListener(_sweeper);

            _repository.setTotalSpace(Long.MAX_VALUE);
            _repository.addCacheRepositoryListener(this);
        } catch (DatabaseException e) {
            throw new RepositoryException("Failed to initialise repository: " + e.getMessage());
        }
    }

    public void runInventory()
        throws IOException, RepositoryException
    {
        try {
            _repository.runInventory(null, _pnfs, 0);
        } catch (CacheException e) {
            throw new RepositoryException("Failed to initialise repository: " + e.getMessage());
        }
    }

    /**
     * Creates entry in the repository.
     *
     * @param pnfsid
     * @param initialState
     * @return IO descriptor
     */
    public WriteHandle createEntry(PnfsId pnfsid,
                                   StorageInfo info,
                                   EntryState initialState,
                                   EntryState targetState,
                                   StickyRecord sticky)
        throws FileInCacheException
    {
        _operationLock.writeLock().lock();
        try {
            return new WriteHandleImpl(this,
                                       _repository,
                                       _pnfs,
                                       _repository.createEntry(pnfsid),
                                       info,
                                       initialState,
                                       targetState,
                                       sticky);
        } catch (FileInCacheException e) {
            throw e;
        } catch (CacheException e) {
            // FIXME: Shut down repository
            throw new RuntimeException("Internal repository error: " + e.getMessage());
        } finally {
            _operationLock.writeLock().unlock();
        }
    }


    /**
     * Opens existing entry for reading.
     *
     * @param pnfsid
     * @return IO descriptor
     * @throws FileNotInCacheException if file not found or scheduled
     * for removal.
     */
    public ReadHandle openEntry(PnfsId pnfsId)
        throws FileNotInCacheException
    {
        _operationLock.readLock().lock();
        try {
            return new ReadHandleImpl(this, _repository.getEntry(pnfsId));
        } catch (FileNotInCacheException e) {
            throw e;
        } catch (CacheException e) {
            // FIXME: Shut down repository
            throw new RuntimeException("Internal repository error: " + e.getMessage());
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    /**
     *
     * @return snapshot of current space usage record
     */
    public SpaceRecord getSpaceRecord()
    {
        _operationLock.readLock().lock();
        try {
            // REVISIT: This is not atomic!
            return new SpaceRecord(_repository.getTotalSpace(),
                                   _repository.getFreeSpace(),
                                   _repository.getPreciousSpace(),
                                   _sweeper.getRemovableSpace());
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    /**
     * Set repository size.
     *
     * @param size in bytes
     * @throws IllegalArgumentException if new size is smaller than
     * current non removable space.
     */
    public void setSize(long size) throws IllegalArgumentException
    {
        _operationLock.writeLock().lock();
        try {
            _repository.setTotalSpace(size);
        } finally {
            _operationLock.writeLock().unlock();
        }
    }

    public void setState(PnfsId id, EntryState state)
    {
        _operationLock.readLock().lock();
        try {
            CacheRepositoryEntry entry = _repository.getEntry(id);

            switch (state) {
            case NEW:
                throw new IllegalArgumentException("Cannot mark entry new");
            case FROM_CLIENT:
                entry.setReceivingFromClient();
                break;
            case FROM_STORE:
                entry.setReceivingFromStore();
                break;
            case FROM_POOL:
                entry.setReceivingFromClient();
                break;
            case BROKEN:
                entry.setBad(true);
                break;
            case CACHED:
                entry.setCached();
                break;
            case PRECIOUS:
                entry.setPrecious(true);
                break;
            case REMOVED:
                entry.setRemoved();
                break;
            case DESTROYED:
            }
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: " +
                                       e.getMessage());
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    protected void notify(PnfsId id, EntryState oldState, EntryState newState)
    {
        StateChangeEvent event = new StateChangeEvent(id, oldState, newState);
        for (StateChangeListener listener : _listeners)
            listener.stateChanged(event);
    }

    public void addListener(StateChangeListener listener)
    {
        _listeners.add(listener);
    }

    public void removeListener(StateChangeListener listener)
    {
        _listeners.remove(listener);
    }

    EntryState getState(PnfsId id)
    {
        EntryState oldState = _states.get(id);
        if (oldState == null)
            oldState = NEW;
        return oldState;
    }

    protected void updateState(PnfsId id, EntryState newState)
    {
        EntryState oldState = getState(id);
        if (newState == DESTROYED)
            _states.remove(id);
        else
            _states.put(id, newState);
        notify(id, oldState, newState);
    }

    public void precious(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), PRECIOUS);
    }

    public void cached(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), CACHED);
    }

    public void created(CacheRepositoryEvent event)
    {
        //        updateState(event.getPnfsId(), EntryState.);
    }

    public void touched(CacheRepositoryEvent event)
    {

    }

    public void removed(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), REMOVED);
    }

    public void destroyed(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), DESTROYED);
    }

    public void scanned(CacheRepositoryEvent event)
    {
        try {
            EntryState state;
            CacheRepositoryEntry entry = event.getRepositoryEntry();

            if (entry.isBad())
                state = BROKEN;
            else if (entry.isPrecious())
                state = PRECIOUS;
            else if (entry.isCached())
                state = CACHED;
            else if (entry.isReceivingFromClient())
                state = FROM_CLIENT;
            else if (entry.isReceivingFromStore())
                state = FROM_STORE;
            else if (entry.isRemoved())
                state = REMOVED;
            else
                state = NEW;

            updateState(entry.getPnfsId(), state);
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
    }

    public void available(CacheRepositoryEvent event)
    {

    }

    public void sticky(CacheRepositoryEvent event)
    {

    }

    public void needSpace(CacheNeedSpaceEvent event)
    {

    }

    public void actionPerformed(CacheEvent event)
    {

    }
}