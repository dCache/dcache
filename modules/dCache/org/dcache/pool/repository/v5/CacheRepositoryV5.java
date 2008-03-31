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

    /**
     * True if inventory has been build, otherwise false.
     */
    private boolean _initialised;

    private static final String DEFAULT_SWEEPER =
        "diskCacheV111.pools.SpaceSweeper0";
    private static final String DUMMY_SWEEPER =
        "diskCacheV111.pools.DummySpaceSweeper";

    /**
     * Instantiates a new sweeper. The sweeper to create is determined
     * by the -sweeper option.
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

    // REVISIT: Consider taking base from the args. Consider taking
    // args from the cell. Consider creating the pnfs handler based on
    // the args.
    public CacheRepositoryV5(CellAdapter cell, PnfsHandler pnfs,
                             File base, Args args)
        throws IOException, RepositoryException
    {
        try {
            _initialised = false;
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

    /**
     * Loads the repository from the on disk state. Must be done
     * exactly once before any other operation can be performed.
     */
    public void runInventory()
        throws IOException, RepositoryException, IllegalStateException
    {
        _operationLock.writeLock().lock();
        try {
            if (_initialised)
                throw new IllegalStateException("Can only load repository once.");
            _initialised = true;
            _repository.runInventory(null, _pnfs, 0);
        } catch (CacheException e) {
            throw new RepositoryException("Failed to initialise repository: " + e.getMessage());
        } finally {
            _operationLock.writeLock().unlock();
        }
    }

    /**
     * Creates entry in the repository. Returns a write handle for the
     * entry. The write handle must be explicitly closed. As long as
     * the write handle is not closed, reads are not allowed on the
     * entry.
     *
     * While the handle is open, the entry is in the transfer
     * state. Once the handle is closed, the entry is automatically
     * moved to the target state, unless the handle is cancelled
     * first.
     *
     * @param id the PNFS ID of the new entry
     * @param info the storage info of the new entry
     * @param transferState the transfer state
     * @param targetState the target state
     * @param sticky sticky record to apply to entry; can be null
     * @return A write handle for the entry.
     * @throws FileInCacheException if an entry with the same ID
     * already exists.
     */
    public WriteHandle createEntry(PnfsId id,
                                   StorageInfo info,
                                   EntryState transferState,
                                   EntryState targetState,
                                   StickyRecord sticky)
        throws FileInCacheException
    {
        _operationLock.writeLock().lock();
        try {
            return new WriteHandleImpl(this,
                                       _repository,
                                       _pnfs,
                                       _repository.createEntry(id),
                                       info,
                                       transferState,
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
     * Opens an entry for reading.
     *
     * A read handle is returned which is used to access the file. The
     * read handle must be explicitly closed after use. While the read
     * handle is open, it acts as a shared lock, which prevents the
     * entry from being deleted. Notice that an open read handle does
     * not prevent state changes.
     *
     * @param id the PNFS ID of the entry to open
     * @return IO descriptor
     * @throws FileNotInCacheException if file not found or scheduled
     * for removal.
     */
    public ReadHandle openEntry(PnfsId id)
        throws FileNotInCacheException
    {
        _operationLock.readLock().lock();
        try {
            return new ReadHandleImpl(this, _repository.getEntry(id));
        } catch (FileNotInCacheException e) {
            throw e;
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    /**
     *
     */
    public CacheEntry getEntry(PnfsId id)
        throws FileNotInCacheException
    {
        _operationLock.readLock().lock();
        try {
            return new CacheEntryImpl(_repository.getEntry(id), getState(id));
        } catch (FileNotInCacheException e) {
            throw e;
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        } finally {
            _operationLock.readLock().unlock();
        }
    }


    /**
     * Returns information about the size and space usage of the
     * repository.
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
     * Sets the size of the repository.
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

    /**
     * Sets the state of an entry.
     *
     * @param id a PNFS ID
     * @param state an entry state
     * @throws NullPointerException if <code>id</code> is null
     * @throws FileNotInCacheException if file not found or scheduled
     * for removal.
     * @throws IllegalArgumentException is <code>state</code> is NEW
     * or DESTROYED.
     */
    public void setState(PnfsId id, EntryState state)
        throws FileNotInCacheException
    {
        _operationLock.readLock().lock();
        try {
            CacheRepositoryEntry entry = _repository.getEntry(id);

            if (entry.isBad()) {
                entry.setBad(false);
            }

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
                entry.setReceivingFromStore();
                break;
            case BROKEN:
                entry.setBad(true);
                updateState(id, BROKEN);
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
                throw new IllegalArgumentException("Cannot mark entry destroyed");
            }
        } catch (FileNotInCacheException e) {
            throw e;
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: " +
                                       e.getMessage());
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    /**
     * Notify listeners about a state change.
     */
    protected void notify(PnfsId id, EntryState oldState, EntryState newState)
    {
        StateChangeEvent event = new StateChangeEvent(id, oldState, newState);
        for (StateChangeListener listener : _listeners)
            listener.stateChanged(event);
    }

    /**
     * Adds a state change listener.
     */
    public void addListener(StateChangeListener listener)
    {
        _listeners.add(listener);
    }

    /**
     * Removes a state change listener.
     */
    public void removeListener(StateChangeListener listener)
    {
        _listeners.remove(listener);
    }

    /**
     * Returns the state of an entry.
     *
     * @param id the PNFS ID of an entry
     */
    EntryState getState(PnfsId id)
    {
        synchronized (_states) {
            EntryState oldState = _states.get(id);
            if (oldState == null)
                oldState = NEW;
            return oldState;
        }
    }

    /**
     * Updates the state information about an entry and notifies
     * listeners about the state change.
     *
     * Since we currently wrap all the old repository components, we
     * have to keep track of the state manually in the
     * <code>_states</code> hash table. This method updates an entry
     * in that table.
     */
    protected void updateState(PnfsId id, EntryState newState)
    {
        EntryState oldState;
        synchronized (_states) {
            oldState = getState(id);
            if (newState == DESTROYED)
                _states.remove(id);
            else
                _states.put(id, newState);
        }
        notify(id, oldState, newState);
    }

    /** Callback. */
    public void precious(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), PRECIOUS);
    }

    /** Callback. */
    public void cached(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), CACHED);
    }

    /** Callback. */
    public void created(CacheRepositoryEvent event)
    {
        try {
            CacheRepositoryEntry entry = event.getRepositoryEntry();
            if (entry.isReceivingFromClient())
                updateState(entry.getPnfsId(), FROM_CLIENT);
            else if (entry.isReceivingFromStore())
                updateState(entry.getPnfsId(), FROM_STORE);
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository failure: "
                                       + e.getMessage());
        }
    }

    /** Callback. */
    public void touched(CacheRepositoryEvent event)
    {

    }

    /** Callback. */
    public void removed(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), REMOVED);
        // TODO Unregister in PNFS
    }

    /** Callback. */
    public void destroyed(CacheRepositoryEvent event)
    {
        updateState(event.getRepositoryEntry().getPnfsId(), DESTROYED);
    }

    /** Callback. */
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

    /** Callback. */
    public void available(CacheRepositoryEvent event)
    {
        // TODO Register in PNFS
    }

    /** Callback. */
    public void sticky(CacheRepositoryEvent event)
    {

    }

    /** Callback. */
    public void needSpace(CacheNeedSpaceEvent event)
    {

    }

    /** Callback. */
    public void actionPerformed(CacheEvent event)
    {

    }
}