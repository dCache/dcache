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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.DiskSpace;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Argument;
import dmg.util.command.Command;

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
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.pool.repository.EntryState.NEW;
import static org.dcache.pool.repository.EntryState.PRECIOUS;
import static org.dcache.pool.repository.EntryState.REMOVED;
import static org.dcache.util.ByteUnit.GiB;


/**
 * Implementation of Repository interface.
 *
 * Allows openEntry, getEntry, getState and setSticky to be called
 * before the load method finishes. Other methods of the Repository
 * interface will fail until load has completed.
 */
public class CacheRepositoryV5
    extends AbstractCellComponent
    implements Repository, CellCommandListener, CellSetupProvider
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

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CacheRepositoryV5.class);

    /**
     * Time in millisecs added to each sticky expiration task.  We
     * schedule the task later than the expiration time to account for
     * small clock shifts.
     */
    public static final long EXPIRATION_CLOCKSHIFT_EXTRA_TIME = 1000L;

    public static final long DEFAULT_GAP = GiB.toBytes(4L);

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
    @GuardedBy("_stateLock")
    private ScheduledExecutorService _executor;

    /**
     * Meta data about files in the pool.
     */
    @GuardedBy("_stateLock")
    private MetaDataStore _store;

    /**
     * Current state of the repository.
     */
    enum State {
        UNINITIALIZED,
        INITIALIZED,
        LOADING,
        OPEN,
        FAILED,
        CLOSED
    }

    @GuardedBy("_stateLock")
    private State _state = State.UNINITIALIZED;

    /**
     * Lock for the field changes.
     */
    private final ReadWriteLock _stateLock = new ReentrantReadWriteLock();

    /**
     * Initialization progress between 0 and 1.
     */
    private volatile float _initializationProgress;

    /**
     * Shared repository account object for tracking space.
     */
    @GuardedBy("_stateLock")
    private Account _account;

    /**
     * Allocator used for when allocating space for new entries.
     */
    @GuardedBy("_stateLock")
    private Allocator _allocator;

    /**
     * Policy defining which files may be garbage collected.
     */
    @GuardedBy("_stateLock")
    private SpaceSweeperPolicy _sweeper;

    @GuardedBy("_stateLock")
    private PnfsHandler _pnfs;

    @GuardedBy("_stateLock")
    private boolean _volatile;

    /**
     * Pool size configured through the 'max disk space' command.
     */
    @GuardedBy("_stateLock")
    private DiskSpace _runtimeMaxSize = DiskSpace.UNSPECIFIED;

    /**
     * Pool size configured in the configuration files.
     */
    @GuardedBy("_stateLock")
    private DiskSpace _staticMaxSize = DiskSpace.UNSPECIFIED;

    /**
     * Pool size gap to report to pool manager.
     */
    @GuardedBy("_stateLock")
    private DiskSpace _gap = DiskSpace.UNSPECIFIED;

    /**
     * Throws an IllegalStateException if the repository has been
     * initialized.
     */
    @GuardedBy("_stateLock")
    private void checkUninitialized()
    {
        if (_state != State.UNINITIALIZED) {
            throw new IllegalStateException("Operation not allowed after initialization");
        }
    }

    /**
     * Throws an IllegalStateException if the repository is not open.
     */
    @GuardedBy("_stateLock")
    private void checkOpen()
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
    @GuardedBy("_stateLock")
    private void checkInitialized()
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
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _executor = executor;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    /**
     * Sets the handler for talking to the PNFS manager.
     */
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _pnfs = pnfs;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    public boolean getVolatile()
    {
        _stateLock.readLock().lock();
        try {
            return _volatile;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    /**
     * Sets whether pool is volatile. On volatile pools
     * ClearCacheLocation messages are flagged to trigger deletion of
     * the namespace entry when the last known replica is deleted.
     */
    public void setVolatile(boolean value)
    {
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _volatile = value;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    /**
     * The account keeps track of available space.
     */
    public void setAccount(Account account)
    {
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _account = account;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    /**
     * The allocator implements an allocation policy.
     */
    public void setAllocator(Allocator allocator)
    {
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _allocator = allocator;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    public void setMetaDataStore(MetaDataStore store)
    {
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _store = new MetaDataCache(store, new StateChangeListener()

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
                            LOGGER.info("remove entry for: {}", id);
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
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    public void setSpaceSweeperPolicy(SpaceSweeperPolicy sweeper)
    {
        _stateLock.readLock().lock();
        try {
            checkUninitialized();
            _sweeper = sweeper;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    public void setMaxDiskSpaceString(String size)
    {
        setMaxDiskSpace(size.isEmpty() ? DiskSpace.UNSPECIFIED : new DiskSpace(size));
    }

    public void setMaxDiskSpace(DiskSpace size)
    {
        _stateLock.writeLock().lock();
        try {
            _staticMaxSize = size;
            if (_state == State.OPEN) {
                updateAccountSize();
            }
        } finally {
            _stateLock.writeLock().unlock();
        }
    }

    public State getState()
    {
        _stateLock.readLock().lock();
        try {
            return _state;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    @Override
    public void init()
            throws IllegalStateException, CacheException
    {
        checkState(_pnfs != null, "Pnfs handler must be set.");
        checkState(_account != null, "Account must be set.");
        checkState(_allocator != null, "Allocator must be set.");

        if (!compareAndSetState(State.UNINITIALIZED, State.INITIALIZED)) {
            throw new IllegalStateException("Can only initialize uninitialized repository.");
        }
    }

    private boolean compareAndSetState(State expected, State state)
    {
        _stateLock.writeLock().lock();
        try {
            if (_state != expected) {
                return false;
            }
            _state = state;
            return true;
        } finally {
            _stateLock.writeLock().unlock();
        }
    }

    @Override
    public void load()
        throws CacheException, IllegalStateException,
               InterruptedException
    {
        if (!compareAndSetState(State.INITIALIZED, State.LOADING)) {
            throw new IllegalStateException("Can only load repository after initialization and only once.");
        }
        try {
            LOGGER.warn("Reading inventory from {}.", _store);
            _store.init();

            Collection<PnfsId> ids = _store.index();

            int fileCount = ids.size();
            LOGGER.info("Checking meta data for {} files.", fileCount);
            int cnt = 0;
            for (PnfsId id: ids) {
                MetaDataRecord entry = readMetaDataRecord(id);
                if (entry != null)  {
                    EntryState state = entry.getState();
                    LOGGER.debug("{} {}", id, state);
                }
                _initializationProgress = ((float) cnt) / fileCount;
                cnt++;

                // Lazily check if repository was closed
                if (_state != State.LOADING) {
                    throw new IllegalStateException("Repository was closed during loading.");
                }
            }

            _stateLock.writeLock().lock();
            try {
                updateAccountSize();
                if (!compareAndSetState(State.LOADING, State.OPEN)) {
                    throw new IllegalStateException("Repository was closed during loading.");
                }
            } finally {
                _stateLock.writeLock().unlock();
            }
        } finally {
            compareAndSetState(State.LOADING, State.FAILED);
        }

        LOGGER.info("Done generating inventory.");
    }

    @Override
    public Iterator<PnfsId> iterator()
    {
        _stateLock.readLock().lock();
        try {
            checkOpen();
            try {
                return Collections.unmodifiableCollection(_store.index()).iterator();
            } catch (CacheException e) {
                throw Throwables.propagate(e);
            }
        } finally {
            _stateLock.readLock().unlock();
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
        _stateLock.readLock().lock();
        try {
            checkOpen();

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

            LOGGER.info("Creating new entry for {}", id);

            MetaDataRecord entry = _store.create(id);
            return entry.update(r -> {
                r.setFileAttributes(fileAttributes);
                r.setState(transferState);
                try {
                    return new WriteHandleImpl(
                            this, _allocator, _pnfs, entry, fileAttributes,
                                    targetState, stickyRecords, flags);
                } catch (IOException e) {
                    throw new DiskErrorCacheException("Failed to create file: " + entry.getDataFile(), e);
                }
            });
        } catch (DuplicateEntryException e) {
            /* Somebody got the idea that we don't have the file, so we make
             * sure to register it.
             */
            _pnfs.notify(new PnfsAddCacheLocationMessage(id, getPoolName()));
            throw new FileInCacheException("Entry already exists: " + id);
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    @Override
    public ReplicaDescriptor openEntry(PnfsId id, Set<OpenFlags> flags)
        throws CacheException, InterruptedException
    {
        _stateLock.readLock().lock();
        try {
            checkInitialized();

            FileAttributes fileAttributes;
            MetaDataRecord entry = getMetaDataRecord(id);
            synchronized (entry) {
                switch (entry.getState()) {
                case NEW:
                case FROM_CLIENT:
                case FROM_STORE:
                case FROM_POOL:
                    throw new LockedCacheException("File is incomplete");
                case BROKEN:
                    throw new FileCorruptedCacheException("File is broken");
                case REMOVED:
                case DESTROYED:
                    throw new LockedCacheException("File has been removed");
                case PRECIOUS:
                case CACHED:
                    break;
                }
                fileAttributes = entry.getFileAttributes();
                if (!flags.contains(OpenFlags.NOATIME)) {
                    entry.setLastAccessTime(System.currentTimeMillis());
                }
                entry.incrementLinkCount();
            }

            return new ReadHandleImpl(_pnfs, entry, fileAttributes);
        } catch (FileNotInCacheException e) {
            /* Somebody got the idea that we have the file, so we make
             * sure to remove any stray pointers.
             */
            try {
                MetaDataRecord entry = _store.create(id);
                entry.update(r -> r.setState(REMOVED));
            } catch (DuplicateEntryException concurrentCreation) {
                return openEntry(id, flags);
            } catch (CacheException | RuntimeException f) {
                e.addSuppressed(f);
            }
            throw e;
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    @Override
    public CacheEntry getEntry(PnfsId id)
        throws CacheException, InterruptedException
    {
        _stateLock.readLock().lock();
        try {
            checkInitialized();

            MetaDataRecord entry = getMetaDataRecord(id);
            synchronized (entry) {
                if (entry.getState() == NEW) {
                    throw new FileNotInCacheException("File is incomplete");
                }
                return new CacheEntryImpl(entry);
            }
        } finally {
            _stateLock.readLock().unlock();
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

        _stateLock.readLock().lock();
        try {
            checkInitialized();

            MetaDataRecord entry;
            try {
                entry = getMetaDataRecord(id);
            } catch (FileNotInCacheException e) {
                /* Attempts to set a sticky bit on a missing file may
                 * indicate a stale registration in the name space.
                 */
                try {
                    entry = _store.create(id);
                    entry.update(r -> r.setState(REMOVED));
                } catch (DuplicateEntryException concurrentCreation) {
                    setSticky(id, owner, expire, overwrite);
                    return;
                } catch (CacheException | RuntimeException f) {
                    e.addSuppressed(f);
                }
                throw e;
            }

            entry.update(r -> {
                switch (r.getState()) {
                case NEW:
                case FROM_CLIENT:
                case FROM_STORE:
                case FROM_POOL:
                    throw new LockedCacheException("File is incomplete");
                case REMOVED:
                case DESTROYED:
                    throw new LockedCacheException("File has been removed");
                case BROKEN:
                case PRECIOUS:
                case CACHED:
                    break;
                }
                return r.setSticky(owner, expire, overwrite);
            });
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    @Override
    public SpaceRecord getSpaceRecord()
    {
        _stateLock.readLock().lock();
        try {
            SpaceRecord space = _account.getSpaceRecord();
            long lru = (System.currentTimeMillis() - _sweeper.getLru()) / 1000L;
            long gap = _gap.orElse(Math.min(space.getTotalSpace() / 4, DEFAULT_GAP));
            return new SpaceRecord(space.getTotalSpace(),
                                   space.getFreeSpace(),
                                   space.getPreciousSpace(),
                                   space.getRemovableSpace(),
                                   lru,
                                   gap);
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    @Override
    public void setState(PnfsId id, EntryState state)
        throws IllegalArgumentException, InterruptedException, CacheException
    {
        if (id == null) {
            throw new IllegalArgumentException("id is null");
        }

        _stateLock.readLock().lock();
        try {
            checkOpen();

            try {
                MetaDataRecord entry = getMetaDataRecord(id);
                entry.update(r -> {
                    EntryState source = r.getState();
                    switch (source) {
                    case NEW:
                    case REMOVED:
                    case DESTROYED:
                        if (state == EntryState.REMOVED) {
                            /* File doesn't exist or is already
                             * deleted. That's all we care about.
                             */
                            return null;
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
                            return r.setState(state);
                        default:
                            break;
                        }
                    default:
                        break;
                    }
                    throw new IllegalTransitionException(id, source, state);
                });
            } catch (FileNotInCacheException e) {
                /* File disappeared before we could change the
                 * state. That's okay if we wanted to remove it, otherwise
                 * not.
                 */
                if (state != REMOVED) {
                    throw new IllegalTransitionException(id, NEW, state);
                }
            }
        } finally {
            _stateLock.readLock().unlock();
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
        _stateLock.readLock().lock();
        try {
            checkInitialized();
            try {
                return getMetaDataRecord(id).getState();
            } catch (FileNotInCacheException e) {
                return NEW;
            }
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        _stateLock.readLock().lock();
        try {
            State state = _state;
            pw.append("State : ").append(state.toString());
            if (state == State.LOADING) {
                pw.append(" (").append(String.valueOf((int) (_initializationProgress * 100))).append("% done)");
            }
            pw.println();
            try {
                if (state == State.OPEN || state == State.LOADING || state == State.INITIALIZED) {
                    pw.println("Files : " + _store.index().size());
                }
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
            pw.println("    Total    : " + DiskSpace.toUnitString(total));
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
            pw.println("    Statically configured: " + _staticMaxSize);
            pw.println("    Runtime configured   : " + _runtimeMaxSize);
        } finally {
            _stateLock.readLock().unlock();
        }
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

    /**
     * Reports a fault to all fault listeners.
     */
    void fail(FaultAction action, String message)
    {
        FaultEvent event =
            new FaultEvent("repository", action, message, null);
        for (FaultListener listener : _faultListeners) {
            listener.faultOccurred(event);
        }
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
                LOGGER.warn("Failed to clear sticky flags for {}: {}", _id, e.getMessage());
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

    @Command(name = "set max diskspace",
            hint = "set size of pool",
            description = "Sets the maximum disk space to be used by this pool. Overrides " +
                          "whatever maximum was defined in the configuration file. The value " +
                          "will be saved to the pool setup file if the save command is " +
                          "executed.")
    class SetMaxDiskspaceCommand implements Callable<String>
    {
        @Argument(valueSpec = "-|BYTES[k|m|g|t]",
                usage = "Disk space in bytes, kibibytes, mebibytes, gibibytes, or tebibytes. If " +
                        "- is specified, then the pool will return to the size configured in " +
                        "the configuration file, or no maximum if such a size is not defined.")
        DiskSpace size;

        @Override
        public String call() throws IllegalArgumentException
        {
            _stateLock.writeLock().lock();
            try {
                _runtimeMaxSize = size;
                if (_state == State.OPEN) {
                    updateAccountSize();
                }
            } finally {
                _stateLock.writeLock().unlock();
            }
            return "";
        }
    }

    @Command(name = "set gap",
            hint = "set minimum free space target",
            description = "New transfers will not be assigned to a pool once it has less free space than the " +
                          "gap. This is to ensure that there is a reasonable chance for ongoing transfers to " +
                          "complete. To prevent that writes will fail due to lack of space, the gap should be " +
                          "in the order of the expected largest file size multiplied by the largest number of " +
                          "concurrent writes expected to a pool, although a smaller value will often do.\n\n" +
                          "It is not an error for a pool to have less free space than the gap.")
    class SetGapCommand implements Callable<String>
    {
        @Argument(valueSpec = "BYTES[k|m|g|t]", required = false,
                usage = "The gap in bytes, kibibytes, mebibytes, gibibytes or tebibytes. If not specified the " +
                        "default is the smaller of 4 GiB or 25% of the pool size.")
        DiskSpace gap = DiskSpace.UNSPECIFIED;

        @Override
        public String call() throws Exception
        {
            _stateLock.writeLock().lock();
            try {
                _gap = gap;
            } finally {
                _stateLock.writeLock().unlock();
            }
            return "Gap set to " + gap;
        }
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        DiskSpace runtimeMaxSize;
        DiskSpace gap;

        _stateLock.readLock().lock();
        try {
            runtimeMaxSize = _runtimeMaxSize;
            gap = _gap;
        } finally {
            _stateLock.readLock().unlock();
        }

        if (runtimeMaxSize.isSpecified()) {
            pw.println("set max diskspace " + runtimeMaxSize);
        }
        if (gap.isSpecified()) {
            pw.println("set gap " + gap);
        }
    }

    private DiskSpace getConfiguredMaxSize()
    {
        _stateLock.readLock().lock();
        try {
            return _runtimeMaxSize.orElse(_staticMaxSize);
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    private long getFileSystemMaxSize()
    {
        _stateLock.readLock().lock();
        try {
            return _store.getFreeSpace() + _account.getUsed();
        } finally {
            _stateLock.readLock().unlock();
        }
    }

    private boolean isTotalSpaceReported()
    {
        _stateLock.readLock().lock();
        try {
            return _store.getTotalSpace() > 0;
        } finally {
            _stateLock.readLock().unlock();
        }
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
    @GuardedBy("_stateLock")
    private void updateAccountSize()
    {
        Account account = _account;
        synchronized (account) {
            DiskSpace configuredPoolSize = getConfiguredMaxSize();
            long maxPoolSize = getFileSystemMaxSize();
            long used = account.getUsed();

            if (!isTotalSpaceReported()) {
                LOGGER.warn("Java reported the file system size as 0. This typically happens on Solaris with a 32-bit JVM. Please use a 64-bit JVM.");
                if (!configuredPoolSize.isSpecified()) {
                    throw new IllegalStateException("Failed to determine file system size. A pool size must be configured.");
                }
            }

            long newSize;

            if (configuredPoolSize.isLargerThan(maxPoolSize)) {
                LOGGER.warn("Configured pool size ({}) is larger than what is available on disk ({}).",
                            configuredPoolSize, maxPoolSize);
            } else if (configuredPoolSize.isLessThan(used)) {
                LOGGER.warn("Configured pool size ({}) is less than what is used already ({}).",
                            configuredPoolSize, used);
            }

            newSize = Math.max(used, configuredPoolSize.orElse(maxPoolSize));
            if (newSize != account.getTotal()) {
                LOGGER.info("Adjusting pool size to {}", newSize);
                account.setTotal(newSize);
            }
        }
    }
}
