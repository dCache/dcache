package org.dcache.pool.repository.v5;

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
import diskCacheV111.util.UnitInteger;
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
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.FaultAction;
import org.dcache.cell.CellInfoProvider;
import org.dcache.cell.AbstractCellComponent;
import static org.dcache.pool.repository.EntryState.*;

import java.io.PrintWriter;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

public class CacheRepositoryV5
    extends AbstractCellComponent
    implements CacheRepositoryListener,
               Iterable<PnfsId>
{
    private final List<StateChangeListener> _stateChangeListeners =
        new CopyOnWriteArrayList<StateChangeListener>();

    private final List<FaultListener> _faultListeners =
        new CopyOnWriteArrayList<FaultListener>();

    /**
     * Map to keep track of states. Temporary hack because we do not
     * have enough information in the actual repository.
     */
    private final Map<PnfsId, EntryState> _states =
        new HashMap<PnfsId, EntryState>();

    /** Classic repository used for tracking entries. */
    private CacheRepositoryV4 _repository;

    /** Executor for periodic tasks. */
    private final ScheduledExecutorService _executor;

    /**
     * True if inventory has been build, otherwise false.
     */
    private boolean _initialised = false;

    private long _size = Long.MAX_VALUE;
    private SpaceSweeper _sweeper;
    private PnfsHandler _pnfs;
    private boolean _checkRepository = true;
    private boolean _volatile = false;

    public CacheRepositoryV5()
    {
        _executor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Throws an IllegalStateException if the object has been initialised.
     */
    private void assertNotInitialised()
    {
        if (_initialised)
            throw new IllegalStateException("Cannot be changed after initialisation");
    }

    /**
     * Sets the handler for talking to the PNFS manager.
     */
    public synchronized void setPnfsHandler(PnfsHandler pnfs)
    {
        assertNotInitialised();
        _pnfs = pnfs;
    }

    /**
     * Enables or disables periodic consistency checks.
     */
    public synchronized void setPeriodicChecks(boolean enable)
    {
        assertNotInitialised();
        _checkRepository = enable;
    }

    public synchronized void setSweeper(SpaceSweeper sweeper)
    {
        assertNotInitialised();
        _sweeper = sweeper;
    }

    public synchronized boolean getVolatile()
    {
        return _volatile;
    }

    /**
     * Sets whether pool is volatile. On volatile pools, target states
     * of PRECIOUS are silently changed to CACHED, and
     * ClearCacheLocation messages are flagged to trigger deletion of
     * the namespace entry when the last known replica is deleted.
     */
    public synchronized void setVolatile(boolean value)
    {
        _volatile = value;
    }

    /**
     * Sets the size of the repository.
     *
     * @param size in bytes
     * @throws IllegalArgumentException if new size is smaller than
     * current non removable space.
     */
    public synchronized void setSize(long size) throws IllegalArgumentException
    {
        if (size < 0)
            throw new IllegalArgumentException("Size must not be negative");
        _size = size;
        if (_repository != null)
            _repository.setTotalSpace(_size);
    }

    /**
     * Sets the cache repository used internally to track repository
     * entries.
     */
    public synchronized void setLegacyRepository(CacheRepositoryV4 repository)
    {
        _repository = repository;
        _repository.setTotalSpace(_size);
        _repository.addCacheRepositoryListener(this);
    }

    /**
     * Loads the repository from the on disk state. Must be done
     * exactly once before any other operation can be performed.
     *
     * @throws IllegalStateException if called multiple times
     * @throws IOException if an io error occurs
     * @throws RepositoryException in case of other internal errors
     */
    public synchronized void init(int flags)
        throws IOException, RepositoryException, IllegalStateException
    {
        assert _pnfs != null : "Pnfs handler must be set";
        assert _repository != null : "Repository must be set";

        try {
            if (_initialised)
                throw new IllegalStateException("Can only load repository once.");
            _initialised = true;

            _repository.runInventory(_pnfs, flags);

            if (_checkRepository) {
                _executor.scheduleWithFixedDelay(new CheckHealthTask(this),
                                                 30, 30, TimeUnit.SECONDS);
            }
        } catch (CacheException e) {
            throw new RepositoryException("Failed to initialise repository: " + e.getMessage());
        }
    }

    /**
     * Returns the list of PNFS IDs of entries in the repository.
     */
    public synchronized Iterator<PnfsId> iterator()
    {
        if (!_initialised)
            throw new IllegalStateException("Repository has not been initialized");
        return _repository.pnfsids();
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
    public synchronized WriteHandle createEntry(PnfsId id,
                                                StorageInfo info,
                                                EntryState transferState,
                                                EntryState targetState,
                                                StickyRecord sticky)
        throws FileInCacheException
    {
        if (!_initialised)
            throw new IllegalStateException("Repository has not been initialized");
        try {
            CacheRepositoryEntry entry = _repository.createEntry(id);
            try {
                if (_volatile && targetState == PRECIOUS) {
                    targetState = CACHED;
                }

                WriteHandle handle = new WriteHandleImpl(this,
                                                         _repository,
                                                         _pnfs,
                                                         entry,
                                                         info,
                                                         transferState,
                                                         targetState,
                                                         sticky);
                entry = null;
                return handle;
            } finally {
                if (entry != null) {
                    entry.lock(false);
                    _repository.removeEntry(entry);
                }
            }
        } catch (FileInCacheException e) {
            throw e;
        } catch (IOException e) {
            fail(FaultAction.DISABLED, "Failed to create file", e);
            throw new RuntimeException("Failed to create file", e);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
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
     * TODO: Refine the exceptions. Throwing FileNotInCacheException
     * implies that one could create the entry, however this is not
     * the case for broken or incomplet files.
     *
     * @param id the PNFS ID of the entry to open
     * @return IO descriptor
     * @throws FileNotInCacheException if file not found or in a state
     * in which it cannot be opened
     */
    public synchronized ReadHandle openEntry(PnfsId id)
        throws FileNotInCacheException
    {
        if (!_initialised)
            throw new IllegalStateException("Repository has not been initialized");

        switch (getState(id)) {
        case FROM_CLIENT:
        case FROM_STORE:
        case FROM_POOL:
            throw new FileNotInCacheException("File is incomplete");
        case BROKEN:
            throw new FileNotInCacheException("Replica marked broken. Cannot open broken replica.");
        default:
            break;
        }

        try {
            return new ReadHandleImpl(this, _repository.getEntry(id));
        } catch (FileNotInCacheException e) {
            /* Somebody got the idea that we have the file, so we make
             * sure to remove any stray pointers.
             */
            _pnfs.clearCacheLocation(id);
            throw e;
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Returns information about an entry. Equivalent to calling
     * <code>getEntry</code> on a read handle, but avoid the cost of
     * creating a read handle.
     */
    public synchronized CacheEntry getEntry(PnfsId id)
        throws FileNotInCacheException
    {
        if (!_initialised)
            throw new IllegalStateException("Repository has not been initialized");
        try {
            return new CacheEntryImpl(_repository.getEntry(id), getState(id));
        } catch (FileNotInCacheException e) {
            throw e;
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Sets the lifetime of a named sticky flag. If expiration time is
     * -1, then the sticky flag never expires. If is is 0, the flag
     * expires immediately.
     *
     * @param id the PNFS ID of the entry for which to change the flag
     * @param owner the owner of the sticky flag
     * @param expire expiration time in milliseconds since the epoch
     * @throws FileNotInCacheException when an entry with the given id
     * is not found in the repository
     * @throws IllegalArgumentException when <code>id</code> or
     * <code>owner</code> are null or when <code>lifetime</code> is
     * smaller than -1.
     */
    public synchronized void setSticky(PnfsId id, String owner, long expire)
        throws IllegalArgumentException,
               FileNotInCacheException
    {
        try {
            if (expire < -1)
                throw new IllegalArgumentException("Expiration time must be -1 or non-negative");
            if (id == null || owner == null)
                throw new IllegalArgumentException("Null argument not allowed");

            _repository.getEntry(id).setSticky(expire != 0, owner, expire);
        } catch (FileNotInCacheException e) {
            throw e;
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Returns information about the size and space usage of the
     * repository.
     *
     * @return snapshot of current space usage record
     */
    public synchronized SpaceRecord getSpaceRecord()
    {
        // REVISIT: This is not atomic!
        return new SpaceRecord(_repository.getTotalSpace(),
                               _repository.getFreeSpace(),
                               _repository.getPreciousSpace(),
                               _sweeper.getRemovableSpace(),
                               _sweeper.getLRUSeconds());
    }

    /**
     * Internal method for setting the state of an entry.
     *
     * @param entry a repository entry
     * @param state an entry state
     * @throws IllegalArgumentException is <code>state</code> is NEW
     * or DESTROYED.
     */
    synchronized void setState(CacheRepositoryEntry entry, EntryState state)
    {
        try {
            switch (state) {
            case NEW:
                throw new IllegalArgumentException("Cannot mark entry new");
            case FROM_CLIENT:
                entry.setBad(false);
                entry.setReceivingFromClient();
                break;
            case FROM_STORE:
                entry.setBad(false);
                entry.setReceivingFromStore();
                break;
            case FROM_POOL:
                entry.setBad(false);
                entry.setReceivingFromStore();
                break;
            case BROKEN:
                entry.setBad(true);
                updateState(entry.getPnfsId(), BROKEN);
                break;
            case CACHED:
                entry.setBad(false);
                entry.setCached();
                break;
            case PRECIOUS:
                entry.setBad(false);
                entry.setPrecious(true);
                break;
            case REMOVED:
                _repository.removeEntry(entry);
                break;
            case DESTROYED:
                throw new IllegalArgumentException("Cannot mark entry destroyed");
            }
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Sets the state of an entry. Only the following transitions are
     * allowed:
     *
     * <ul>
     * <li>{NEW, REMOVED, DESTROYED} to REMOVED.
     * <li>{PRECIOUS, CACHED, BROKEN} to {PRECIOUS, CACHED, BROKEN, REMOVED}.
     * </ul>
     *
     * @param id a PNFS ID
     * @param state an entry state
     * @throws IllegalTransitionException if the transition is illegal.
     * @throws IllegalArgumentException if <code>id</code> is null.
     */
    public synchronized void setState(PnfsId id, EntryState state)
        throws IllegalTransitionException, IllegalArgumentException
    {
        if (id == null)
            throw new IllegalArgumentException("id is null");

        try {
            EntryState source = getState(id);
            switch (source) {
            case NEW:
            case REMOVED:
            case DESTROYED:
                if (state == EntryState.REMOVED) {
                    /* File doesn't exist or is already
                     * deleted. That's all we care about.
                     */
                    return;
                }
                break;
            case PRECIOUS:
            case CACHED:
            case BROKEN:
                switch (state) {
                case REMOVED:
                case CACHED:
                case PRECIOUS:
                case BROKEN:
                    setState(_repository.getEntry(id), state);
                    return;
                default:
                    break;
                }
            default:
                break;
            }
            throw new IllegalTransitionException(id, source, state);
        } catch (FileNotInCacheException e) {
            /* File disappeared before we could change the
             * state. That's okay if we wanted to remove it, otherwise
             * not.
             */
            if (state != REMOVED) {
                throw new IllegalTransitionException(id, NEW, state);
            }
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Notify listeners about a state change.
     */
    protected void notify(PnfsId id, EntryState oldState, EntryState newState)
    {
        StateChangeEvent event = new StateChangeEvent(id, oldState, newState);
        for (StateChangeListener listener : _stateChangeListeners)
            listener.stateChanged(event);
    }

    /**
     * Reports a fault to all fault listeners.
     */
    void fail(FaultAction action, String message, Throwable cause)
    {
        FaultEvent event =
            new FaultEvent("repository", action, message, cause);
        for (FaultListener listener : _faultListeners)
            listener.faultOccurred(event);
    }

    /**
     * Reports a fault to all fault listeners.
     */
    void fail(FaultAction action, String message)
    {
        fail(action, message, null);
    }

    /**
     * Adds a state change listener.
     */
    public void addListener(StateChangeListener listener)
    {
        _stateChangeListeners.add(listener);
    }

    /**
     * Removes a state change listener.
     */
    public void removeListener(StateChangeListener listener)
    {
        _stateChangeListeners.remove(listener);
    }

    /**
     * Adds a state change listener.
     */
    public void addFaultListener(FaultListener listener)
    {
        _faultListeners.add(listener);
    }

    /**
     * Removes a fault change listener.
     */
    public void removeFaultListener(FaultListener listener)
    {
        _faultListeners.remove(listener);
    }

    /**
     * Returns the state of an entry.
     *
     * @param id the PNFS ID of an entry
     */
    public EntryState getState(PnfsId id)
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
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /** Callback. */
    public void touched(CacheRepositoryEvent event)
    {

    }

    /** Callback. */
    public void removed(CacheRepositoryEvent event)
    {
        PnfsId id = event.getRepositoryEntry().getPnfsId();
        updateState(id, REMOVED);
        _pnfs.clearCacheLocation(id, _volatile);
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
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /** Callback. */
    public void available(CacheRepositoryEvent event)
    {
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

    public synchronized void getInfo(PrintWriter pw)
    {
//         pw.println("Storage Mode      : "
//                    + (DUMMY_SWEEPER.isInstance(_sweeper) ? "Static" : "Dynamic"));
        pw.println("Check Repository  : " + _checkRepository);

        SpaceRecord space = getSpaceRecord();
        pw.println("Diskspace usage   : ");
        long total = space.getTotalSpace();
        long used = total - space.getFreeSpace();
        long precious = space.getPreciousSpace();

        pw.println("    Total    : " + UnitInteger.toUnitString(total));
        pw.println("    Used     : " + used + "    ["
                   + (((float) used) / ((float) total)) + "]");
        pw.println("    Free     : " + (total - used));
        pw.println("    Precious : " + precious + "    ["
                   + (((float) precious) / ((float) total)) + "]");
        pw.println("    Removable: "
                   + space.getRemovableSpace()
                   + "    ["
                   + (((float) space.getRemovableSpace()) / ((float) total))
                   + "]");
    }

    public void shutdown()
    {
        _executor.shutdown();
    }

    /**
     * Performs basic health check of the repository. Called by
     * CheckHealthTask.
     */
    boolean isRepositoryOk()
    {
        return _repository.isRepositoryOk();
    }

}