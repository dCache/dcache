package org.dcache.pool.repository.v5;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.UnitInteger;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;

import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.MetaDataCache;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.SpaceSweeperPolicy;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Args;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.pool.repository.EntryState.*;


/**
 * Implementation of Repository interface.
 *
 * The class is thread-safe.
 *
 * Allows openEntry, getEntry, getState and setSticky to be called
 * before the load method finishes. Other methods of the Repository
 * interface will fail until load has completed.
 */
public class CacheRepositoryV5
    extends AbstractCellComponent
    implements Repository, CellCommandListener
{
    /* Implementation note
     * -------------------
     *
     * The following order must be observed when synchronizing:
     *
     *  - this
     *  - entries (only one)
     *  - _account
     *
     */

    private static final Logger _log =
            LoggerFactory.getLogger(CacheRepositoryV5.class);

    /**
     * Time in millisecs added to each sticky expiration task.  We
     * schedule the task later than the expiration time to account for
     * small clock shifts.
     */
    public static final long EXPIRATION_CLOCKSHIFT_EXTRA_TIME = 1000L;

    public static final long DEFAULT_GAP =  4L << 30;

    private final List<FaultListener> _faultListeners =
        new CopyOnWriteArrayList<>();

    private final StateChangeListeners _stateChangeListeners =
        new StateChangeListeners();

    /**
     * Sticky bit expiration tasks.
     */
    private final Map<PnfsId,ScheduledFuture<?>> _tasks =
        new ConcurrentHashMap<>();

    /**
     * Collection of removable entries.
     */
    private final Set<PnfsId> _removable =
        Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Executor for periodic tasks. */
    private ScheduledExecutorService _executor;

    /**
     * Meta data about files in the pool.
     */
    private MetaDataStore _store;

    /**
     * Current state of the repository.
     */
    enum State {
        UNINITIALIZED,
        INITIALIZING,
        INITIALIZED,
        LOADING,
        OPEN,
        FAILED,
        CLOSED
    }

    private State _state = State.UNINITIALIZED;

    /**
     * Shared repository account object for tracking space.
     */
    private Account _account;

    /**
     * Allocator used for when allocating space for new entries.
     */
    private Allocator _allocator;

    /**
     * Policy defining which files may be garbage collected.
     */
    private SpaceSweeperPolicy _sweeper;

    private PnfsHandler _pnfs;
    private boolean _volatile;

    /**
     * Pool size configured through the 'max disk space' command.
     */
    private long _runtimeMaxSize = Long.MAX_VALUE;

    /**
     * Pool size configured in the configuration files.
     */
    private long _staticMaxSize = Long.MAX_VALUE;

    /**
     * Pool size gap to report to pool manager.
     */
    private Optional<Long> _gap = Optional.empty();

    public CacheRepositoryV5()
    {
    }

    /**
     * Throws an IllegalStateException if the repository has been
     * initialized.
     */
    private synchronized void assertUninitialized()
    {
        if (_state != State.UNINITIALIZED) {
            throw new IllegalStateException("Operation not allowed after initialization");
        }
    }

    /**
     * Throws an IllegalStateException if the repository is not open.
     */
    private synchronized void assertOpen()
    {
        if (_state != State.OPEN) {
            throw new IllegalStateException("Operation not allowed while repository is in state " + _state);
        }
    }

    /**
     * Throws an IllegalStateException if the repository is not in
     * either INITIALIZED, LOADING or OPEN.
     */
    private synchronized void assertInitialized()
    {
        if (_state != State.INITIALIZED && _state != State.LOADING &&
            _state != State.OPEN) {
            throw new IllegalStateException("Operation not allowed while repository is in state " + _state);
        }
    }

    /**
     * The executor is used for periodic background checks and sticky
     * flag expiration.
     */
    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        assertUninitialized();
        _executor = executor;
    }

    /**
     * Sets the handler for talking to the PNFS manager.
     */
    public synchronized void setPnfsHandler(PnfsHandler pnfs)
    {
        assertUninitialized();
        _pnfs = pnfs;
    }

    public synchronized boolean getVolatile()
    {
        return _volatile;
    }

    /**
     * Sets whether pool is volatile. On volatile pools
     * ClearCacheLocation messages are flagged to trigger deletion of
     * the namespace entry when the last known replica is deleted.
     */
    public synchronized void setVolatile(boolean value)
    {
        assertUninitialized();
        _volatile = value;
    }

    /**
     * The account keeps track of available space.
     */
    public synchronized void setAccount(Account account)
    {
        assertUninitialized();
        _account = account;
    }

    /**
     * The allocator implements an allocation policy.
     */
    public synchronized void setAllocator(Allocator allocator)
    {
        assertUninitialized();
        _allocator = allocator;
    }

    public synchronized void setMetaDataStore(MetaDataStore store)
    {
        assertUninitialized();
        _store = store;
    }

    public synchronized void setSpaceSweeperPolicy(SpaceSweeperPolicy sweeper)
    {
        assertUninitialized();
        _sweeper = sweeper;
    }

    public synchronized void setMaxDiskSpaceString(String size)
    {
        setMaxDiskSpace(UnitInteger.parseUnitLong(size));
    }

    public synchronized void setMaxDiskSpace(long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Negative value is not allowed");
        }
        _staticMaxSize = size;
        if (_state == State.OPEN) {
            updateAccountSize();
        }
    }

    public synchronized State getState()
    {
        return _state;
    }

    @Override
    public void init()
            throws IllegalStateException, CacheException
    {
        synchronized (this) {
            assert _pnfs != null : "Pnfs handler must be set";
            assert _account != null : "Account must be set";
            assert _allocator != null : "Account must be set";

            if (_state != State.UNINITIALIZED) {
                throw new IllegalStateException("Can only initialize repository once.");
            }

            _state = State.INITIALIZING;
        }

        /* Instantiating the cache causes the listing to be
         * generated to prepopulate the cache. That may take some
         * time. Therefore we do this outside the synchronization.
         */
        _log.warn("Reading inventory from " + _store);
        MetaDataCache cache = new MetaDataCache(_store);

        synchronized (this) {
            _store = cache;
            _state = State.INITIALIZED;
        }
    }

    @Override
    public void load()
        throws CacheException, IllegalStateException,
               InterruptedException
    {
        try {
            synchronized (this) {
                if (_state != State.INITIALIZED) {
                    throw new IllegalStateException("Can only load repository after initialization and only once.");
                }
                _state = State.LOADING;
            }

            List<PnfsId> ids = new ArrayList<>(_store.list());
            _log.info("Found {} data files", ids.size());

            /* On some file systems (e.g. GPFS) stat'ing files in
             * lexicographic order seems to trigger the pre-fetch
             * mechanism of the file system.
             */
            Collections.sort(ids);

            /* Collect all entries.
             */
            _log.info("Checking meta data for {} files", ids.size());
            long usedDataSpace = 0L;
            List<MetaDataRecord> entries = new ArrayList<>();
            for (PnfsId id: ids) {
                MetaDataRecord entry = readMetaDataRecord(id);
                if (entry != null)  {
                    usedDataSpace += entry.getSize();
                    _log.debug("{} {}", id, entry.getState());
                    entries.add(entry);
                }
            }

            /* Allocate space.
             */
            _log.info("Pool contains {} bytes of data", usedDataSpace);
            Account account = _account;
            synchronized (account) {
                account.setTotal(usedDataSpace);
                account.allocateNow(usedDataSpace);
            }

            updateAccountSize();

            /* State change notifications are suppressed while the
             * repository is loading. We synchronize to ensure a clean
             * switch from the LOADING state to the OPEN state.
             */
            synchronized (this) {
                /* Register with event listeners in LRU order. The
                 * sweeper relies on the LRU order.
                 */
                _log.info("Registering files in sweeper");
                for (MetaDataRecord entry: entries) {
                    synchronized (entry) {
                        CacheEntry cacheEntry = new CacheEntryImpl(entry);
                        stateChanged(cacheEntry, cacheEntry, NEW, entry.getState());
                    }
                }

                _log.info("Inventory contains {} files; total size is {}; used space is {}; free space is {}.",
                          entries.size(), _account.getTotal(),
                          usedDataSpace, _account.getFree());

                _state = State.OPEN;
            }

            /* Register sticky timeouts.
             */
            _log.info("Registering sticky bits");
            for (MetaDataRecord entry: entries) {
                synchronized (entry) {
                    if (entry.isSticky()) {
                        scheduleExpirationTask(entry);
                    }
                }
            }
        } finally {
            synchronized (this) {
                if (_state != State.OPEN) {
                    _state = State.FAILED;
                }
            }
        }

        _log.info("Done generating inventory");
    }

    @Override
    public Iterator<PnfsId> iterator()
    {
        assertOpen();
        try {
            return Collections.unmodifiableCollection(_store.list()).iterator();
        } catch (DiskErrorCacheException | RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw Throwables.propagate(e);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ReplicaDescriptor createEntry(FileAttributes fileAttributes,
                                   EntryState transferState,
                                   EntryState targetState,
                                   List<StickyRecord> stickyRecords,
                                   Set<OpenFlags> flags)
        throws FileInCacheException
    {
        if (!fileAttributes.isDefined(EnumSet.of(PNFSID, STORAGEINFO))) {
            throw new IllegalArgumentException("PNFSID and STORAGEINFO are required, only got " + fileAttributes.getDefinedAttributes());
        }
        if (stickyRecords == null) {
            throw new IllegalArgumentException("List of sticky records must not be null");
        }
        PnfsId id = fileAttributes.getPnfsId();
        try {
            assertOpen();

            switch (transferState) {
            case FROM_CLIENT:
            case FROM_STORE:
            case FROM_POOL:
                break;
            default:
                throw new IllegalArgumentException("Invalid initial state");
            }

            switch (targetState) {
            case PRECIOUS:
            case CACHED:
                break;
            default:
                throw new IllegalArgumentException("Invalid target state");
            }

            _log.info("Creating new entry for {}", id);

            MetaDataRecord entry = _store.create(id);
            synchronized (entry) {
                entry.setFileAttributes(fileAttributes);
                setState(entry, transferState);

                return new WriteHandleImpl(
                        this, _allocator, _pnfs, entry, fileAttributes, targetState, stickyRecords, flags);
            }
        } catch (DuplicateEntryException e) {
            /* Somebody got the idea that we don't have the file, so we make
             * sure to register it.
             */
            _pnfs.notify(new PnfsAddCacheLocationMessage(id, getPoolName()));
            throw new FileInCacheException("Entry already exists: " + id);
        } catch (RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        } catch (DiskErrorCacheException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    @Override
    public ReplicaDescriptor openEntry(PnfsId id, Set<OpenFlags> flags)
        throws CacheException, InterruptedException
    {
        assertInitialized();

        try {
            ReplicaDescriptor handle;

            MetaDataRecord entry = getMetaDataRecord(id);
            synchronized (entry) {
                switch (entry.getState()) {
                case NEW:
                case FROM_CLIENT:
                case FROM_STORE:
                case FROM_POOL:
                    throw new LockedCacheException("File is incomplete");
                case BROKEN:
                    throw new LockedCacheException("File is broken");
                case REMOVED:
                case DESTROYED:
                    throw new LockedCacheException("File has been removed");
                case PRECIOUS:
                case CACHED:
                    break;
                }
                handle = new ReadHandleImpl(this, _pnfs, entry);
            }

            if (!flags.contains(OpenFlags.NOATIME)) {
                synchronized (this) {
                    /* Don't notify listeners until we are done
                     * loading; at the end of the load method
                     * listeners are informed about all entries.
                     */
                    synchronized (entry) {
                        CacheEntry oldEntry = new CacheEntryImpl(entry);
                        entry.touch();
                        if (_state == State.OPEN) {
                            CacheEntryImpl newEntry = new CacheEntryImpl(entry);
                            accessTimeChanged(oldEntry, newEntry);
                        }
                    }
                }
            }

            return handle;
        } catch (FileNotInCacheException e) {
            /* Somebody got the idea that we have the file, so we make
             * sure to remove any stray pointers.
             */
            try {
                MetaDataRecord entry = _store.create(id);
                setState(entry, REMOVED);
            } catch (DuplicateEntryException concurrentCreation) {
                return openEntry(id, flags);
            } catch (DiskErrorCacheException | RuntimeException f) {
                fail(FaultAction.DEAD, "Internal repository error", f);
                e.addSuppressed(f);
            }
            throw e;
        } catch (DiskErrorCacheException | RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }
    }

    @Override
    public CacheEntry getEntry(PnfsId id)
        throws CacheException, InterruptedException
    {
        assertInitialized();

        try {
            MetaDataRecord entry = getMetaDataRecord(id);
            synchronized (entry) {
                if (entry.getState() == NEW) {
                    throw new FileNotInCacheException("File is incomplete");
                }
                return new CacheEntryImpl(entry);
            }
        } catch (RuntimeException | DiskErrorCacheException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }
    }

    @Override
    public void setSticky(PnfsId id, String owner,
                          long expire, boolean overwrite)
        throws IllegalArgumentException,
               CacheException,
               InterruptedException
    {
        checkNotNull(id);
        checkNotNull(owner);
        checkArgument(expire >= -1, "Expiration time must be -1 or non-negative");

        assertInitialized();

        MetaDataRecord entry;
        try {
            entry = getMetaDataRecord(id);
        } catch (FileNotInCacheException e) {
            /* Attempts to set a sticky bit on a missing file may
             * indicate a stale registration in the name space.
             */
            try {
                entry = _store.create(id);
                setState(entry, REMOVED);
            } catch (DuplicateEntryException concurrentCreation) {
                setSticky(id, owner, expire, overwrite);
                return;
            } catch (DiskErrorCacheException | RuntimeException f) {
                fail(FaultAction.DEAD, "Internal repository error", f);
                e.addSuppressed(f);
            }
            throw e;
        } catch (DiskErrorCacheException | RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }

        /* We synchronize on 'this' because setSticky below is
         * synchronized; and 'this' has to be locked before entry.
         */
        synchronized (this) {
            synchronized (entry) {
                switch (entry.getState()) {
                case NEW:
                case FROM_CLIENT:
                case FROM_STORE:
                case FROM_POOL:
                    throw new FileNotInCacheException("File is incomplete");
                case REMOVED:
                case DESTROYED:
                    throw new FileNotInCacheException("File has been removed");
                case BROKEN:
                case PRECIOUS:
                case CACHED:
                    break;
                }

                setSticky(entry, owner, expire, overwrite);
            }
        }
    }

    @Override
    public SpaceRecord getSpaceRecord()
    {
        SpaceRecord space = _account.getSpaceRecord();
        long lru = (System.currentTimeMillis() - _sweeper.getLru()) / 1000L;
        long gap = _gap.orElse(Math.min(space.getTotalSpace() / 4, DEFAULT_GAP));
        return new SpaceRecord(space.getTotalSpace(),
                               space.getFreeSpace(),
                               space.getPreciousSpace(),
                               space.getRemovableSpace(),
                               lru,
                               gap);
    }

    @Override
    public void setState(PnfsId id, EntryState state)
        throws IllegalTransitionException, IllegalArgumentException,
               InterruptedException, CacheException
    {
        if (id == null) {
            throw new IllegalArgumentException("id is null");
        }

        assertOpen();

        try {
            MetaDataRecord entry = getMetaDataRecord(id);
            synchronized (entry) {
                EntryState source = entry.getState();
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
                        setState(entry, state);
                        return;
                    default:
                        break;
                    }
                default:
                    break;
                }
                throw new IllegalTransitionException(id, source, state);
            }
        } catch (FileNotInCacheException e) {
            /* File disappeared before we could change the
             * state. That's okay if we wanted to remove it, otherwise
             * not.
             */
            if (state != REMOVED) {
                throw new IllegalTransitionException(id, NEW, state);
            }
        } catch (RuntimeException | DiskErrorCacheException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }
    }

    /**
     * If set to true, then state change listeners are notified
     * synchronously. In this case listeners must not acquire any
     * locks or call back into the repository, as there is otherwise a
     * risk that the component will deadlock. Synchronous notification
     * is mainly provided for testing purposes.
     */
    public void setSynchronousNotification(boolean value)
    {
        _stateChangeListeners.setSynchronousNotification(value);
    }

    @Override
    public void addListener(StateChangeListener listener)
    {
        _stateChangeListeners.add(listener);
    }

    @Override
    public void removeListener(StateChangeListener listener)
    {
        _stateChangeListeners.remove(listener);
    }

    @Override
    public void addFaultListener(FaultListener listener)
    {
        _faultListeners.add(listener);
    }

    @Override
    public void removeFaultListener(FaultListener listener)
    {
        _faultListeners.remove(listener);
    }

    @Override
    public EntryState getState(PnfsId id)
        throws CacheException, InterruptedException
    {
        assertInitialized();
        try {
            return getMetaDataRecord(id).getState();
        } catch (FileNotInCacheException e) {
            return NEW;
        } catch (DiskErrorCacheException | RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("State : " + _state);
        try {
            pw.println("Files : " + (_state == State.OPEN ?_store.list().size() : ""));
        } catch (CacheException e) {
            pw.println("Files : " + e.getMessage());
        }

        SpaceRecord space = getSpaceRecord();
        long total = space.getTotalSpace();
        long used = total - space.getFreeSpace();
        long precious = space.getPreciousSpace();
        long fsFree = _store.getFreeSpace();
        long fsTotal = _store.getTotalSpace();
        long gap = space.getGap();

        pw.println("Disk space");
        pw.println("    Total    : " + UnitInteger.toUnitString(total));
        pw.println("    Used     : " + used + "    ["
                   + (((float) used) / ((float) total)) + "]");
        pw.println("    Free     : " + (total - used) + "    Gap : " + gap);
        pw.println("    Precious : " + precious + "    ["
                   + (((float) precious) / ((float) total)) + "]");
        pw.println("    Removable: "
                   + space.getRemovableSpace()
                   + "    ["
                   + (((float) space.getRemovableSpace()) / ((float) total))
                   + "]");
        pw.println("File system");
        pw.println("    Size : " + fsTotal);
        pw.println("    Free : " + fsFree +
                   "    [" + (((float) fsFree) / fsTotal) + "]");
        pw.println("Limits for maximum disk space");
        pw.println("    File system          : " + (fsFree + used));
        pw.println("    Statically configured: " + UnitInteger.toUnitString(_staticMaxSize));
        pw.println("    Runtime configured   : " + UnitInteger.toUnitString(_runtimeMaxSize));
    }

    public synchronized void shutdown()
    {
        _stateChangeListeners.stop();
        _state = State.CLOSED;
    }

    // Operations on MetaDataRecord ///////////////////////////////////////

    @GuardedBy("getMetaDataRecord(entry.getPnfsid())")
    protected void updateRemovable(CacheEntry entry)
    {
            PnfsId id = entry.getPnfsId();
            if (_sweeper.isRemovable(entry)) {
                if (_removable.add(id)) {
                    _account.adjustRemovable(entry.getReplicaSize());
                }
            } else {
                if (_removable.remove(id)) {
                    _account.adjustRemovable(-entry.getReplicaSize());
                }
            }
    }

    /**
     * Asynchronously notify listeners about a state change.
     */
    @GuardedBy("getMetaDataRecord(newEntry.getPnfsid())")
    protected void stateChanged(CacheEntry oldEntry, CacheEntry newEntry,
                                EntryState oldState, EntryState newState)
    {
        updateRemovable(newEntry);
        StateChangeEvent event =
            new StateChangeEvent(oldEntry, newEntry, oldState, newState);
        _stateChangeListeners.stateChanged(event);

        if (oldState != PRECIOUS && newState == PRECIOUS) {
            _account.adjustPrecious(newEntry.getReplicaSize());
        } else if (oldState == PRECIOUS && newState != PRECIOUS) {
            _account.adjustPrecious(-newEntry.getReplicaSize());
        }
    }

    /**
     * Asynchronously notify listeners about an access time change.
     */
    @GuardedBy("getMetaDataRecord(newEntry.getPnfsid())")
    protected void accessTimeChanged(CacheEntry oldEntry, CacheEntry newEntry)
    {
        updateRemovable(newEntry);
        EntryChangeEvent event = new EntryChangeEvent(oldEntry, newEntry);
        _stateChangeListeners.accessTimeChanged(event);
    }

    /**
     * Asynchronously notify listeners about a change of a sticky
     * record.
     */
    @GuardedBy("getMetaDataRecord(newEntry.getPnfsid())")
    protected void stickyChanged(CacheEntry oldEntry, CacheEntry newEntry, StickyRecord record)
    {
        updateRemovable(newEntry);
        StickyChangeEvent event = new StickyChangeEvent(oldEntry, newEntry, record);
        _stateChangeListeners.stickyChanged(event);
    }

    /**
     * Package local method for setting the state of an entry.
     *
     * @param entry a repository entry
     * @param state an entry state
     */
    void setState(MetaDataRecord entry, EntryState state)
    {
        try {
            synchronized (entry) {
                EntryState oldState = entry.getState();
                if (oldState == state) {
                    return;
                }

                CacheEntry oldEntry = new CacheEntryImpl(entry);

                entry.setState(state);

                CacheEntryImpl newEntry = new CacheEntryImpl(entry);

                if (!(oldState == NEW && state == REMOVED)) {
                    stateChanged(oldEntry, newEntry, oldState, state);
                }

                if (state == REMOVED) {
                    if (oldState != NEW) {
                        _log.info("remove entry for: {}", entry.getPnfsId());
                    }

                    PnfsId id = entry.getPnfsId();
                    _pnfs.clearCacheLocation(id, _volatile);

                    ScheduledFuture<?> oldTask = _tasks.remove(id);
                    if (oldTask != null) {
                        oldTask.cancel(false);
                    }

                    destroyWhenRemovedAndUnused(entry);
                }
            }
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        } catch (RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }
    }

    /**
     * Package local method for changing sticky records of an entry.
     */
    synchronized void setSticky(MetaDataRecord entry, String owner,
                   long expire, boolean overwrite)
        throws IllegalArgumentException
    {
        /* This method is synchronized to avoid conflicts with the
         * load method; at the end of load method expiration tasks are
         * scheduled. For that reason this method does not generate
         * sticky changed notification and does not schedule
         * expiration tasks if the repository is not OPEN.
         */
        try {
            synchronized (entry) {
                CacheEntry oldEntry = new CacheEntryImpl(entry);
                if (entry.setSticky(owner, expire, overwrite) && _state == State.OPEN) {
                    CacheEntryImpl newEntry = new CacheEntryImpl(entry);
                    stickyChanged(oldEntry, newEntry, new StickyRecord(owner, expire));
                    scheduleExpirationTask(entry);
                }
            }
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        } catch (RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
            throw e;
        }
    }

    /**
     * @throw FileNotInCacheException in case file is not in
     *        repository
     */
    private MetaDataRecord getMetaDataRecord(PnfsId pnfsId)
        throws CacheException, InterruptedException
    {
        MetaDataRecord entry = _store.get(pnfsId);
        if (entry == null) {
            throw new FileNotInCacheException("Entry not in repository : "
                                              + pnfsId);
        }
        return entry;
    }

    /**
     * Reads an entry from the meta data store. Retries indefinitely
     * in case of timeouts.
     */
    private MetaDataRecord readMetaDataRecord(PnfsId id)
        throws CacheException, InterruptedException
    {
        /* In case of communication problems with the pool, there is
         * no point in failing - the pool would be dead if we did. It
         * is reasonable to expect that the PNFS manager is started at
         * some point and hence we just keep trying.
         */
        while (!Thread.interrupted()) {
            try {
                return _store.get(id);
            } catch (CacheException e) {
                if (e.getRc() != CacheException.TIMEOUT) {
                    throw CacheExceptionFactory.exceptionOf(e.getRc(),
                            "Failed to read meta data for " + id + ": " + e.getMessage(), e);
                }
            }
            Thread.sleep(1000);
        }

        throw new InterruptedException();
    }

    /**
     * Removes an entry from the in-memory cache and erases the data
     * file if it is REMOVED and the link count is zero. Package local
     * method since it is called by the handles.
     */
    void destroyWhenRemovedAndUnused(MetaDataRecord entry)
    {
        try {
            synchronized (entry) {
                EntryState state = entry.getState();
                PnfsId id = entry.getPnfsId();
                if (entry.getLinkCount() == 0 && state == EntryState.REMOVED) {
                    /* Setting the entry to DESTROYED ensures that we only deallocate it once.
                     */
                    setState(entry, DESTROYED);
                    _store.remove(id);

                    /* It is essential to free after we removed the file: This is the opposite
                     * of what happens during allocation, in which we allocate before writing
                     * to disk. We rely on never having anything on disk that we haven't accounted
                     * for in the Account object.
                     */
                    _account.free(entry.getSize());
                }
            }
        } catch (DiskErrorCacheException | RuntimeException e) {
            fail(FaultAction.DEAD, "Internal repository error", e);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
        }
    }

    /**
     * Removes all expired sticky flags of entry.
     */
    private void removeExpiredStickyFlags(MetaDataRecord entry) throws CacheException
    {
        synchronized (entry) {
            CacheEntry oldEntry = new CacheEntryImpl(entry);
            Collection<StickyRecord> removed = entry.removeExpiredStickyFlags();
            for (StickyRecord record: removed) {
                CacheEntryImpl newEntry = new CacheEntryImpl(entry);
                stickyChanged(oldEntry, newEntry, record);
            }
            scheduleExpirationTask(entry);
        }
    }

    /**
     * Schedules an expiration task for a sticky entry.
     */
    private void scheduleExpirationTask(MetaDataRecord entry)
    {
        synchronized (entry) {
            /* Cancel previous task.
             */
            PnfsId pnfsId = entry.getPnfsId();
            ScheduledFuture<?> future = _tasks.remove(pnfsId);
            if (future != null) {
                future.cancel(false);
            }

            /* Find next sticky flag to expire.
             */
            long expire = Long.MAX_VALUE;
            for (StickyRecord record: entry.stickyRecords()) {
                if (record.expire() > -1) {
                    expire = Math.min(expire, record.expire());
                }
            }

            /* Schedule a new task. Notice that we schedule an expiration
             * task even if expire is in the past. This guarantees that we
             * also remove records that already have expired.
             */
            if (expire != Long.MAX_VALUE) {
                ExpirationTask task = new ExpirationTask(entry);
                future = _executor.schedule(task, expire - System.currentTimeMillis()
                                            + EXPIRATION_CLOCKSHIFT_EXTRA_TIME, TimeUnit.MILLISECONDS);
                _tasks.put(pnfsId, future);
            }
        }
    }

    // Callbacks for fault notification ////////////////////////////////////

    /**
     * Reports a fault to all fault listeners.
     */
    void fail(FaultAction action, String message, Throwable cause)
    {
        FaultEvent event =
            new FaultEvent("repository", action, message, cause);
        for (FaultListener listener : _faultListeners) {
            listener.faultOccurred(event);
        }
    }

    /**
     * Reports a fault to all fault listeners.
     */
    void fail(FaultAction action, String message)
    {
        fail(action, message, null);
    }

    /**
     * Runnable for removing expired sticky flags.
     */
    class ExpirationTask implements Runnable
    {
        private final MetaDataRecord _entry;

        public ExpirationTask(MetaDataRecord entry)
        {
            _entry = entry;
        }

        @Override
        public void run()
        {
            try {
                _tasks.remove(_entry.getPnfsId());
                removeExpiredStickyFlags(_entry);
            } catch (CacheException | RuntimeException e) {
                fail(FaultAction.DEAD, "Internal repository error", e);
            }
        }
    }

    /**
     * Get pool name to which repository belongs.
     * @return pool name.
     */
    public String getPoolName()
    {
         return getCellName();
    }

    public final static String hh_set_max_diskspace =
        "<bytes>[<unit>]|Infinity # unit = k|m|g|t";
    public final static String fh_set_max_diskspace =
        "Sets the maximum disk space to be used by this pool. Overrides\n" +
        "whatever maximum was defined in the configuration file. The value\n" +
        "will be saved to the pool setup file if the save command is\n" +
        "executed. If inf is specified, then the pool will return to the\n" +
        "size configured in the configuration file, or no maximum if such a\n" +
        "size is not defined.";
    public synchronized String ac_set_max_diskspace_$_1(Args args)
    {
        long size = UnitInteger.parseUnitLong(args.argv(0));
        if (size < 0) {
            throw new IllegalArgumentException("Negative value is not allowed");
        }
        _runtimeMaxSize = size;
        if (_state == State.OPEN) {
            updateAccountSize();
        }
        return "";
    }

    public static final String hh_set_gap = "<always removable gap>/size[<unit>] # unit = k|m|g";
    public String ac_set_gap_$_1(Args args)
    {
        long gap = UnitInteger.parseUnitLong(args.argv(0));
        _gap = Optional.of(gap);
        return "Gap set to " + gap;
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        if (_runtimeMaxSize < Long.MAX_VALUE) {
            pw.println("set max diskspace " + _runtimeMaxSize);
        }
        if (_gap.isPresent()) {
            pw.println("set gap " + _gap.get());
        }
    }

    private synchronized long getConfiguredMaxSize()
    {
        if (_runtimeMaxSize < Long.MAX_VALUE) {
            return _runtimeMaxSize;
        } else {
            return _staticMaxSize;
        }
    }

    private long getFileSystemMaxSize()
    {
        return _store.getFreeSpace() + _account.getUsed();
    }

    private boolean isTotalSpaceReported()
    {
        return _store.getTotalSpace() > 0;
    }

    /**
     * Updates the total size of the Account based on the configured
     * limits and the available disk space.
     *
     * Notice that if the configured limits are larger than the file
     * system or if there are no configured limits, then the size is
     * going to be an overapproximation based on the current amount of
     * used space and the amount of free space on disk. This is so
     * because the Account object does not accurately track space that
     * has been reserved but not yet written to disk. In this case the
     * periodic health check will adjust the pool size when a more
     * accurate limit can be determined.
     */
    private synchronized void updateAccountSize()
    {
        Account account = _account;
        synchronized (account) {
            long configuredMaxSize = getConfiguredMaxSize();
            long fileSystemMaxSize = getFileSystemMaxSize();
            boolean hasConfiguredMaxSize = (configuredMaxSize < Long.MAX_VALUE);
            long used = account.getUsed();

            if (!isTotalSpaceReported()) {
                _log.warn("Java reported the file system size as 0. This typically happens on Solaris with a 32-bit JVM. Please use a 64-bit JVM.");
                if (!hasConfiguredMaxSize) {
                    throw new IllegalStateException("Failed to determine file system size. A pool size must be configured.");
                }
            }

            if (hasConfiguredMaxSize && fileSystemMaxSize < configuredMaxSize) {
                _log.warn("Configured pool size ({}) is larger than what is available on disk ({})", configuredMaxSize,
                          fileSystemMaxSize);
            }

            if (configuredMaxSize < used) {
                _log.warn("Configured pool size ({}) is smaller than what is used already ({})", configuredMaxSize,
                          used);
            }

            long newSize =
                Math.max(used, Math.min(configuredMaxSize, fileSystemMaxSize));
            if (newSize != account.getTotal()) {
                _log.info("Adjusting pool size to {}", newSize);
                account.setTotal(newSize);
            }
        }
    }
}
