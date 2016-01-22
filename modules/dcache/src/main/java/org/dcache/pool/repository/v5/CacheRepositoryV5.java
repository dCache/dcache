package org.dcache.pool.repository.v5;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.PrintWriter;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * The class is thread-safe after initialization.
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
     *  - _stateLock
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
    private volatile MetaDataStore _store;

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

    private volatile State _state = State.UNINITIALIZED;

    /**
     * Lock for the _state field.
     *
     * The _state field itself is volatile and may be accessed without locking
     * by threads that are only interested in the current state, however updates
     * to _state must obtain a write lock and threads that want to block state
     * changes in a critical region must obtain a read lock.
     */
    private final ReadWriteLock _stateLock = new ReentrantReadWriteLock();

    /**
     * Initialization progress between 0 and 1.
     */
    private float _initializationProgress;

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
    private void assertUninitialized()
    {
        if (_state != State.UNINITIALIZED) {
            throw new IllegalStateException("Operation not allowed after initialization");
        }
    }

    /**
     * Throws an IllegalStateException if the repository is not open.
     */
    private void assertOpen()
    {
        State state = _state;
        if (state != State.OPEN) {
            throw new IllegalStateException("Operation not allowed while repository is in state " + state);
        }
    }

    /**
     * Throws an IllegalStateException if the repository is not in
     * either INITIALIZED, LOADING or OPEN.
     */
    private void assertInitialized()
    {
        State state = _state;
        if (state != State.INITIALIZED && state != State.LOADING && state != State.OPEN) {
            throw new IllegalStateException("Operation not allowed while repository is in state " + state);
        }
    }

    /**
     * The executor is used for periodic background checks and sticky
     * flag expiration.
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        assertUninitialized();
        _executor = executor;
    }

    /**
     * Sets the handler for talking to the PNFS manager.
     */
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        assertUninitialized();
        _pnfs = pnfs;
    }

    public boolean getVolatile()
    {
        return _volatile;
    }

    /**
     * Sets whether pool is volatile. On volatile pools
     * ClearCacheLocation messages are flagged to trigger deletion of
     * the namespace entry when the last known replica is deleted.
     */
    public void setVolatile(boolean value)
    {
        assertUninitialized();
        _volatile = value;
    }

    /**
     * The account keeps track of available space.
     */
    public void setAccount(Account account)
    {
        assertUninitialized();
        _account = account;
    }

    /**
     * The allocator implements an allocation policy.
     */
    public void setAllocator(Allocator allocator)
    {
        assertUninitialized();
        _allocator = allocator;
    }

    public void setMetaDataStore(MetaDataStore store)
    {
        assertUninitialized();
        _store = store;
    }

    public void setSpaceSweeperPolicy(SpaceSweeperPolicy sweeper)
    {
        assertUninitialized();
        _sweeper = sweeper;
    }

    public void setMaxDiskSpaceString(String size)
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

    public State getState()
    {
        return _state;
    }

    @Override
    public void init()
            throws IllegalStateException, CacheException
    {
        assert _pnfs != null : "Pnfs handler must be set";
        assert _account != null : "Account must be set";
        assert _allocator != null : "Account must be set";

        _stateLock.writeLock().lock();
        try {
            if (_state != State.UNINITIALIZED) {
                throw new IllegalStateException("Can only initialize repository once.");
            }
            _state = State.INITIALIZING;
        } finally {
            _stateLock.writeLock().unlock();
        }

        /* Instantiating the cache causes the listing to be generated to
         * populate the cache. This may take some time and therefore we
         * do this outside the synchronization.
         */
        _log.warn("Reading inventory from {}", _store);
        _store = new MetaDataCache(_store, new StateChangeListener()
        {
            @Override
            public void stateChanged(StateChangeEvent event)
            {
                if (event.getOldState() != NEW || event.getNewState() != REMOVED) {
                    if (event.getOldState() == NEW) {
                        long size = event.getNewEntry().getReplicaSize();
                        /* Usually space has to be allocated before writing the
                         * data to disk, however during pool startup we are notified
                         * about "new" files that already consume space, so we
                         * adjust the allocation here.
                         */
                        if (size > 0) {
                            _account.growTotalAndUsed(size);
                        }
                        scheduleExpirationTask(event.getNewEntry());
                    }

                    updateRemovable(event.getNewEntry());

                    if (event.getOldState() != PRECIOUS && event.getNewState() == PRECIOUS) {
                        _account.adjustPrecious(event.getNewEntry().getReplicaSize());
                    } else if (event.getOldState() == PRECIOUS && event.getNewState() != PRECIOUS) {
                        _account.adjustPrecious(-event.getOldEntry().getReplicaSize());
                    }

                    _stateChangeListeners.stateChanged(event);
                }
                PnfsId id = event.getPnfsId();
                switch (event.getNewState()) {
                case REMOVED:
                    if (event.getOldState() != NEW) {
                        _log.info("remove entry for: {}", id);
                    }

                    _pnfs.clearCacheLocation(id, _volatile);

                    ScheduledFuture<?> oldTask = _tasks.remove(id);
                    if (oldTask != null) {
                        oldTask.cancel(false);
                    }
                    break;
                case DESTROYED:
                    /* It is essential to free after we removed the file: This is the opposite
                     * of what happens during allocation, in which we allocate before writing
                     * to disk. We rely on never having anything on disk that we haven't accounted
                     * for in the Account object.
                     */
                    _account.free(event.getOldEntry().getReplicaSize());
                    break;
                }
            }

            @Override
            public void accessTimeChanged(EntryChangeEvent event)
            {
                updateRemovable(event.getNewEntry());
                _stateChangeListeners.accessTimeChanged(event);
            }

            @Override
            public void stickyChanged(StickyChangeEvent event)
            {
                updateRemovable(event.getNewEntry());
                _stateChangeListeners.stickyChanged(event);
                scheduleExpirationTask(event.getNewEntry());
            }
        }, new FaultListener()
        {
            @Override
            public void faultOccurred(FaultEvent event)
            {
                for (FaultListener listener : _faultListeners) {
                    listener.faultOccurred(event);
                }
            }
        });

        _stateLock.writeLock().lock();
        try {
            _state = State.INITIALIZED;
        } finally {
            _stateLock.writeLock().unlock();
        }
    }

    @Override
    public void load()
        throws CacheException, IllegalStateException,
               InterruptedException
    {
        try {
            _stateLock.writeLock().lock();
            try {
                if (_state != State.INITIALIZED) {
                    throw new IllegalStateException("Can only load repository after initialization and only once.");
                }
                _state = State.LOADING;
            } finally {
                _stateLock.writeLock().unlock();
            }

            Collection<PnfsId> ids = _store.index();

            int fileCount = ids.size();
            _log.info("Checking meta data for {} files", fileCount);
            int cnt = 0;
            for (PnfsId id: ids) {
                MetaDataRecord entry = readMetaDataRecord(id);
                if (entry != null)  {
                    EntryState state = entry.getState();
                    _log.debug("{} {}", id, state);
                }
                _initializationProgress = ((float) cnt) / fileCount;
                cnt++;
            }

            updateAccountSize();

            _stateLock.writeLock().lock();
            try {
                if (_state != State.LOADING) {
                    throw new IllegalStateException("Repository was closed during loading.");
                }
                _state = State.OPEN;
            } finally {
                _stateLock.writeLock().unlock();
            }
        } finally {
            _stateLock.writeLock().lock();
            try {
                if (_state != State.OPEN) {
                    _state = State.FAILED;
                }
            } finally {
                _stateLock.writeLock().unlock();
            }
        }

        _log.info("Done generating inventory");
    }

    @Override
    public Iterator<PnfsId> iterator()
    {
        assertOpen();
        try {
            return Collections.unmodifiableCollection(_store.index()).iterator();
        } catch (CacheException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ReplicaDescriptor createEntry(FileAttributes fileAttributes,
                                   EntryState transferState,
                                   EntryState targetState,
                                   List<StickyRecord> stickyRecords,
                                   Set<OpenFlags> flags)
        throws CacheException
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
                entry.setState(transferState);

                try {
                    return new WriteHandleImpl(
                            this, _allocator, _pnfs, entry, fileAttributes, targetState, stickyRecords, flags);
                } catch (IOException e) {
                    throw new DiskErrorCacheException("Failed to create file: " + entry.getDataFile(), e);
                }
            }
        } catch (DuplicateEntryException e) {
            /* Somebody got the idea that we don't have the file, so we make
             * sure to register it.
             */
            _pnfs.notify(new PnfsAddCacheLocationMessage(id, getPoolName()));
            throw new FileInCacheException("Entry already exists: " + id);
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
                entry.touch();
            }

            return handle;
        } catch (FileNotInCacheException e) {
            /* Somebody got the idea that we have the file, so we make
             * sure to remove any stray pointers.
             */
            try {
                MetaDataRecord entry = _store.create(id);
                entry.setState(REMOVED);
            } catch (DuplicateEntryException concurrentCreation) {
                return openEntry(id, flags);
            } catch (CacheException | RuntimeException f) {
                fail(FaultAction.READONLY, "Internal repository error", f);
                e.addSuppressed(f);
            }
            throw e;
        }
    }

    @Override
    public CacheEntry getEntry(PnfsId id)
        throws CacheException, InterruptedException
    {
        assertInitialized();

        MetaDataRecord entry = getMetaDataRecord(id);
        synchronized (entry) {
            if (entry.getState() == NEW) {
                throw new FileNotInCacheException("File is incomplete");
            }
            return new CacheEntryImpl(entry);
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
                entry.setState(REMOVED);
            } catch (DuplicateEntryException concurrentCreation) {
                setSticky(id, owner, expire, overwrite);
                return;
            } catch (CacheException | RuntimeException f) {
                fail(FaultAction.READONLY, "Internal repository error", f);
                e.addSuppressed(f);
            }
            throw e;
        }

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

            entry.setSticky(owner, expire, overwrite);
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
                        entry.setState(state);
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
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        State state = _state;
        pw.append("State : ").append(state.toString());
        if (state == State.LOADING) {
            pw.append(" (").append(String.valueOf((int) (_initializationProgress * 100))).append("% done)");
        }
        pw.println();
        try {
            pw.println("Files : " + (state == State.OPEN || state == State.LOADING || state == State.INITIALIZED ? _store.index().size() : ""));
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

    public void shutdown()
    {
        _stateLock.writeLock().lock();
        try {
            _stateChangeListeners.stop();
            _state = State.CLOSED;
            _store.close();
        } finally {
            _stateLock.writeLock().unlock();
        }
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
     * Package local method for setting the state of an entry.
     *
     * @param entry a repository entry
     * @param state an entry state
     */
    void setState(MetaDataRecord entry, EntryState state)
    {
        try {
            entry.setState(state);
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Package local method for changing sticky records of an entry.
     */
    void setSticky(MetaDataRecord entry, String owner,
                   long expire, boolean overwrite)
        throws IllegalArgumentException
    {
        try {
            entry.setSticky(owner, expire, overwrite);
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error", e);
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
     * Schedules an expiration task for a sticky entry.
     */
    @GuardedBy("entry")
    private void scheduleExpirationTask(CacheEntry entry)
    {
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
        for (StickyRecord record: entry.getStickyRecords()) {
            if (record.expire() > -1) {
                expire = Math.min(expire, record.expire());
            }
        }

        /* Schedule a new task. Notice that we schedule an expiration
         * task even if expire is in the past. This guarantees that we
         * also remove records that already have expired.
         */
        if (expire != Long.MAX_VALUE) {
            ExpirationTask task = new ExpirationTask(entry.getPnfsId());
            future = _executor.schedule(task, expire - System.currentTimeMillis()
                                        + EXPIRATION_CLOCKSHIFT_EXTRA_TIME, TimeUnit.MILLISECONDS);
            _tasks.put(pnfsId, future);
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
        private final PnfsId _id;

        public ExpirationTask(PnfsId id)
        {
            _id = id;
        }

        @Override
        public void run()
        {
            try {
                _tasks.remove(_id);
                MetaDataRecord entry = _store.get(_id);
                if (entry != null) {
                    Collection<StickyRecord> removed = entry.removeExpiredStickyFlags();
                    if (removed.isEmpty()) {
                        /* If for some reason we didn't expire anything, we reschedule
                         * the expiration to be on the safe side (could be a timing
                         * issue).
                         */
                        synchronized (entry) {
                            scheduleExpirationTask(new CacheEntryImpl(entry));
                        }
                    }
                }
            } catch (DiskErrorCacheException ignored) {
                // MetaDataCache will already have disabled the pool if this happens
            } catch (CacheException e) {
                // This ought to be a transient error, so reschedule
                _log.warn("Failed to clear sticky flags for {}: {}", _id, e.getMessage());
                ScheduledFuture<?> future =
                        _executor.schedule(this, EXPIRATION_CLOCKSHIFT_EXTRA_TIME,
                                           TimeUnit.MILLISECONDS);
                _tasks.put(_id, future);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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

    public static final String hh_set_max_diskspace =
        "<bytes>[<unit>]|Infinity # unit = k|m|g|t";
    public static final String fh_set_max_diskspace =
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
