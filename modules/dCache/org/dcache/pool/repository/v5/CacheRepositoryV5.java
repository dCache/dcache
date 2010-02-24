package org.dcache.pool.repository.v5;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.UnitInteger;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.EntryChangeEvent;
import org.dcache.pool.repository.StickyChangeEvent;
import org.dcache.pool.repository.StateChangeListener;
import org.dcache.pool.repository.ReadHandle;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.MetaDataLRUOrder;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.SpaceSweeperPolicy;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.FaultAction;
import org.dcache.cells.CellInfoProvider;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.util.CacheExceptionFactory;
import static org.dcache.pool.repository.EntryState.*;

import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledFuture;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Date;

import dmg.util.Args;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheRepositoryV5
    extends AbstractCellComponent
    implements Repository, CellCommandListener
{
    /**
    * time in millisecs added to each sticky expiration task
    * We schedule the task later than the expiration time to account
    * for small clock shifts.
    */
    public static final long EXPIRATION_CLOCKSHIFT_EXTRA_TIME = 1000L;
    private final static Logger _log =
        LoggerFactory.getLogger(CacheRepositoryV5.class);

    private final List<FaultListener> _faultListeners =
        new CopyOnWriteArrayList<FaultListener>();

    private final StateChangeListeners _stateChangeListeners =
        new StateChangeListeners();

    /**
     * Map of all entries.
     */
    private final Map<PnfsId, MetaDataRecord> _allEntries = new HashMap();

    /**
     * Sticky bit expiration tasks.
     */
    private final Map<PnfsId,ScheduledFuture> _tasks =
        Collections.synchronizedMap(new HashMap());

    /**
     * Collection of removable entries.
     */
    private final Set<PnfsId> _removable = new HashSet();

    /** Executor for periodic tasks. */
    private ScheduledExecutorService _executor;

    /**
     * Meta data about files in the pool.
     */
    private MetaDataStore _store;

    /**
     * True while an inventory is build. During this period we block
     * event processing.
     */
    private boolean _runningInventory = false;

    /**
     * True if inventory has been build, otherwise false.
     */
    private boolean _initialised = false;

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
    private boolean _checkRepository = true;
    private boolean _volatile = false;

    /**
     * Pool size configured through the 'max disk space' command.
     */
    private long _runtimeMaxSize = Long.MAX_VALUE;

    /**
     * Pool size configured in the configuration files.
     */
    private long _staticMaxSize = Long.MAX_VALUE;

    public CacheRepositoryV5()
    {
    }

    /**
     * Throws an IllegalStateException if the object has been initialised.
     */
    private synchronized void assertNotInitialised()
    {
        if (_initialised)
            throw new IllegalStateException("Cannot be changed after initialisation");
    }

    /**
     * The executor is used for periodic background checks and sticky
     * flag expiration.
     */
    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        assertNotInitialised();
        _executor = executor;
    }

    /**
     * Sets the handler for talking to the PNFS manager.
     */
    public synchronized void setPnfsHandler(PnfsHandler pnfs)
    {
        assertNotInitialised();
        _pnfs = pnfs;
    }

    public synchronized boolean getPeriodicChecks()
    {
        return _checkRepository;
    }

    /**
     * Enables or disables periodic consistency checks.
     */
    public synchronized void setPeriodicChecks(boolean enable)
    {
        assertNotInitialised();
        _checkRepository = enable;
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
        _volatile = value;
    }

    /**
     * The account keeps track of available space.
     */
    public synchronized void setAccount(Account account)
    {
        assertNotInitialised();
        _account = account;
    }

    /**
     * The allocator implements an allocation policy.
     */
    public synchronized void setAllocator(Allocator allocator)
    {
        assertNotInitialised();
        _allocator = allocator;
    }

    public synchronized void setMetaDataStore(MetaDataStore store)
    {
        assertNotInitialised();
        _store = store;
    }

    public synchronized void setSpaceSweeperPolicy(SpaceSweeperPolicy sweeper)
    {
        assertNotInitialised();
        _sweeper = sweeper;
    }

    public synchronized void setMaxDiskSpace(String size)
    {
        _staticMaxSize = UnitInteger.parseUnitLong(size);

        if (_initialised) {
            updateAccountSize();
        }
    }

    /**
     * Loads the repository from the on disk state. Must be done
     * exactly once before any other operation can be performed.
     *
     * @throws IllegalStateException if called multiple times
     * @throws IOException if an io error occurs
     * @throws RepositoryException in case of other internal errors
     */
    public synchronized void init()
        throws IOException, RepositoryException, IllegalStateException
    {
        assert _pnfs != null : "Pnfs handler must be set";
        assert _account != null : "Account must be set";
        assert _allocator != null : "Account must be set";

        try {
            if (_initialised)
                throw new IllegalStateException("Can only load repository once.");
            _initialised = true;

            _log.warn("Reading inventory from " + _store);

            List<PnfsId> ids = new ArrayList(_store.list());
            long usedDataSpace = 0L;
            long removableSpace = 0L;

            _log.info("Found " + ids.size() + " data files");

            /* On some file systems (e.g. GPFS) stat'ing files in
             * lexicographic order seems to trigger the pre-fetch
             * mechanism of the file system.
             */
            Collections.sort(ids);

            try {
                _runningInventory = true;

                /* Collect all entries.
                 */
                _log.info("Checking meta data for " + ids.size() + " files");
                for (PnfsId id: ids) {
                    MetaDataRecord entry = readMetaDataRecord(id);
                    if (entry == null)  {
                        continue;
                    }

                    long size = entry.getSize();
                    usedDataSpace += size;
                    if (_sweeper.isRemovable(entry)) {
                        removableSpace += size;
                    }

                    if (_log.isDebugEnabled()) {
                        _log.debug(id +" " + entry.getState());
                    }

                    _allEntries.put(id, entry);
                }

                /* Allocate space.
                 */
                _log.info("Pool contains " + usedDataSpace + " bytes of data");
                _account.setTotal(usedDataSpace);
                _account.allocateNow(usedDataSpace);

                /* Register with event listeners in LRU order. The
                 * sweeper relies on the LRU order.
                 */
                _log.info("Registering files in sweeper");
                List<MetaDataRecord> entries =
                    new ArrayList<MetaDataRecord>(_allEntries.values());
                Collections.sort(entries, new MetaDataLRUOrder());
                for (MetaDataRecord entry: entries) {
                    stateChanged(entry, NEW, entry.getState());
                }

                if (usedDataSpace != _account.getUsed()) {
                    throw new RuntimeException(String.format("Bug detected: Allocated space is not what we expected (%d vs %d)", usedDataSpace, _account.getUsed()));
                }
                if (removableSpace != _account.getRemovable()) {
                    throw new RuntimeException(String.format("Bug detected: Removable space is not what we expected (%d vs %d)", removableSpace, _account.getRemovable()));
                }

                /* Register sweeper timeouts.
                 */
                _log.info("Registering sticky bits");
                for (MetaDataRecord entry: _allEntries.values()) {
                    if (entry.isSticky()) {
                        scheduleExpirationTask(entry);
                    }
                }

                updateAccountSize();

                _log.info(String.format("Inventory contains %d files; total size is %d; used space is %d; free space is %d.",
                                        _allEntries.size(), _account.getTotal(),
                                        usedDataSpace, _account.getFree()));
            } catch (IOException e) {
                throw new CacheException(CacheException.ERROR_IO_DISK,
                                         "Failed to load repository: " + e);
            } catch (InterruptedException e) {
                throw new CacheException("Inventory was interrupted");
            } finally {
                _runningInventory = false;
            }

            _log.info("Done generating inventory");

            if (_checkRepository) {
                CheckHealthTask task = new CheckHealthTask(this);
                task.setAccount(_account);
                task.setMetaDataStore(_store);
                _executor.scheduleWithFixedDelay(task, 0, 60, TimeUnit.SECONDS);
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

        List<PnfsId> allEntries = new ArrayList<PnfsId>(_allEntries.keySet());
        return allEntries.iterator();
    }

    /**
     * Creates an entry in the repository. Returns a write handle for
     * the entry. The write handle must be explicitly closed. As long
     * as the write handle is not closed, reads are not allowed on the
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
     * @param sticky sticky records to apply to entry
     * @return A write handle for the entry.
     * @throws FileInCacheException if an entry with the same ID
     * already exists.
     */
    public synchronized WriteHandle createEntry(PnfsId id,
                                                StorageInfo info,
                                                EntryState transferState,
                                                EntryState targetState,
                                                List<StickyRecord> stickyRecords)
        throws FileInCacheException
    {
        try {
            if (stickyRecords == null) {
                throw new IllegalArgumentException("List of sticky records must not be null");
            }

            if (info == null) {
                throw new IllegalArgumentException("StorageInfo must not be null");
            }

            if (!_initialised) {
                throw new IllegalStateException("Repository has not been initialized");
            }

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

            MetaDataRecord entry = createMetaDataRecord(id);
            entry.setStorageInfo(info);
            _allEntries.put(id, entry);
            setState(entry, transferState);

            return new WriteHandleImpl(this,
                                       _allocator,
                                       _pnfs,
                                       entry,
                                       targetState,
                                       stickyRecords);
        } catch (FileInCacheException e) {
            throw e;
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
        try {
            MetaDataRecord entry = getMetaDataRecord(id);

            /* REVISIT: Is using FileNotInCacheException appropriate?
             */
            switch (entry.getState()) {
            case NEW:
            case FROM_CLIENT:
            case FROM_STORE:
            case FROM_POOL:
                throw new FileNotInCacheException("File is incomplete");
            case BROKEN:
                throw new FileNotInCacheException("File is broken");
            case DESTROYED:
                throw new FileNotInCacheException("File has been removed");
            case PRECIOUS:
            case CACHED:
            case REMOVED:
                break;
            }

            entry.touch();
            accessTimeChanged(entry);
            return new ReadHandleImpl(this, entry);
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
            return new CacheEntryImpl(getMetaDataRecord(id));
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
     * @param overwrite replace existing flag when true, extend
     *                  lifetime if false
     * @throws FileNotInCacheException when an entry with the given id
     * is not found in the repository
     * @throws IllegalArgumentException when <code>id</code> or
     * <code>owner</code> are null or when <code>lifetime</code> is
     * smaller than -1.
     */
    public synchronized void setSticky(PnfsId id, String owner,
                                       long expire, boolean overwrite)
        throws IllegalArgumentException,
               FileNotInCacheException
    {
        if (id == null)
            throw new IllegalArgumentException("Null argument not allowed");

        MetaDataRecord entry = getMetaDataRecord(id);
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

    /**
     * Returns information about the size and space usage of the
     * repository.
     *
     * @return snapshot of current space usage record
     */
    public SpaceRecord getSpaceRecord()
    {
        SpaceRecord space = _account.getSpaceRecord();
        long lru = (System.currentTimeMillis() - _sweeper.getLru()) / 1000L;
        return new SpaceRecord(space.getTotalSpace(),
                               space.getFreeSpace(),
                               space.getPreciousSpace(),
                               space.getRemovableSpace(),
                               lru);
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
                    setState(getMetaDataRecord(id), state);
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
        try {
            return getMetaDataRecord(id).getState();
        } catch (FileNotInCacheException e) {
            return NEW;
        }
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("Check Repository  : " + getPeriodicChecks());

        SpaceRecord space = getSpaceRecord();
        long total = space.getTotalSpace();
        long used = total - space.getFreeSpace();
        long precious = space.getPreciousSpace();
        long fsFree = _store.getFreeSpace();
        long fsTotal = _store.getTotalSpace();

        pw.println("Diskspace usage   : ");
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
        pw.println("File system");
        pw.println("    Size: " + fsTotal);
        pw.println("    Free: " + fsFree +
                   "    [" + (((float) fsFree) / fsTotal) + "]");
        pw.println("Limits for maximum disk space");
        pw.println("    File system          : " + (fsFree + used));
        pw.println("    Statically configured: " + UnitInteger.toUnitString(_staticMaxSize));
        pw.println("    Runtime configured   : " + UnitInteger.toUnitString(_runtimeMaxSize));
    }

    public void shutdown()
        throws InterruptedException
    {
        _stateChangeListeners.stop();
    }

    // Operations on MetaDataRecord ///////////////////////////////////////

    protected void updateRemovable(MetaDataRecord entry)
    {
        PnfsId id = entry.getPnfsId();
        if (_sweeper.isRemovable(entry)) {
            if (_removable.add(id)) {
                _account.adjustRemovable(entry.getSize());
            }
        } else {
            if (_removable.remove(id)) {
                _account.adjustRemovable(-entry.getSize());
            }
        }
    }

    /**
     * Asynchronously notify listeners about a state change.
     */
    protected void stateChanged(MetaDataRecord entry,
                                EntryState oldState, EntryState newState)
    {
        try {
            updateRemovable(entry);
            StateChangeEvent event =
                new StateChangeEvent(new CacheEntryImpl(entry),
                                     oldState, newState);
            _stateChangeListeners.stateChanged(event);

            if (oldState != PRECIOUS && newState == PRECIOUS) {
                _account.adjustPrecious(entry.getSize());
            } else if (oldState == PRECIOUS && newState != PRECIOUS) {
                _account.adjustPrecious(-entry.getSize());
            }
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Asynchronously notify listeners about an access time change.
     */
    protected void accessTimeChanged(MetaDataRecord entry)
    {
        try {
            updateRemovable(entry);
            EntryChangeEvent event =
                new EntryChangeEvent(new CacheEntryImpl(entry));
            _stateChangeListeners.accessTimeChanged(event);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Asynchronously notify listeners about a change of a sticky
     * record.
     */
    protected void stickyChanged(MetaDataRecord entry,
                                 StickyRecord record)
    {
        try {
            updateRemovable(entry);
            StickyChangeEvent event =
                new StickyChangeEvent(new CacheEntryImpl(entry), record);
            _stateChangeListeners.stickyChanged(event);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    /**
     * Package local method for setting the state of an entry.
     *
     * @param entry a repository entry
     * @param state an entry state
     */
    synchronized void setState(MetaDataRecord entry, EntryState state)
    {
        EntryState oldState = entry.getState();
        if (oldState == state)
            return;

        try {
            entry.setState(state);
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }

        stateChanged(entry, oldState, state);

        if (state == REMOVED) {
            if (_log.isInfoEnabled()) {
                _log.info("remove entry for: " + entry.getPnfsId().toString());
            }

            PnfsId id = entry.getPnfsId();
            _pnfs.clearCacheLocation(id, _volatile);

            ScheduledFuture oldTask = _tasks.remove(id);
            if (oldTask != null) {
                oldTask.cancel(false);
            }
        }

        destroyWhenRemovedAndUnused(entry);
    }

    /**
     * Package local method for changing sticky records of an entry.
     */
    synchronized void setSticky(MetaDataRecord entry, String owner,
                                long expire, boolean overwrite)
        throws IllegalArgumentException
    {
        try {
            if (entry == null || owner == null)
                throw new IllegalArgumentException("Null argument not allowed");
            if (expire < -1)
                throw new IllegalArgumentException("Expiration time must be -1 or non-negative");

            if (entry.setSticky(owner, expire, overwrite)) {
                stickyChanged(entry, new StickyRecord(owner, expire));
                scheduleExpirationTask(entry);
            }
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }

    private synchronized MetaDataRecord createMetaDataRecord(PnfsId id)
        throws FileInCacheException
    {
        try {
            /* Fail if file already exists.
             */
            if (_allEntries.containsKey(id)) {
                _log.warn("Entry already exists: " + id);
                throw new FileInCacheException("Entry already exists: " + id);
            }

            /* Create meta data record.
             */
            return _store.create(id);
        } catch (FileInCacheException e) {
            throw e;
        } catch (CacheException e) {
            fail(FaultAction.READONLY, "Internal repository error", e);
            throw new RuntimeException("Internal repository error", e);
        }
    }


    /**
     * @throw FileNotInCacheException in case file is not in
     *        repository
     */
    private synchronized MetaDataRecord getMetaDataRecord(PnfsId pnfsId)
        throws FileNotInCacheException
    {
        MetaDataRecord entry = _allEntries.get(pnfsId);
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
    private synchronized MetaDataRecord readMetaDataRecord(PnfsId id)
        throws CacheException, IOException, InterruptedException
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
                    String s =
                        String.format("Failed to read meta data record [%s]: %s",
                                      id, e.getMessage());
                    throw CacheExceptionFactory.exceptionOf(e.getRc(), s);
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
    synchronized void destroyWhenRemovedAndUnused(MetaDataRecord entry)
    {
        EntryState state = entry.getState();
        PnfsId id = entry.getPnfsId();
        if (entry.getLinkCount() == 0 && state == EntryState.REMOVED
            && _allEntries.containsKey(id)) {
            _account.free(entry.getSize());
            stateChanged(entry, state, DESTROYED);
            _allEntries.remove(id);
            _store.remove(id);
        }
    }

    /**
     * Removes all expired sticky flags of entry.
     */
    private synchronized void removeExpiredStickyFlags(MetaDataRecord entry)
    {
        List<StickyRecord> removed = entry.removeExpiredStickyFlags();
        for (StickyRecord record: removed) {
            stickyChanged(entry, record);
        }
        scheduleExpirationTask(entry);
    }

    /**
     * Schedules an expiration task for a sticky entry.
     */
    private synchronized void scheduleExpirationTask(MetaDataRecord entry)
    {
        /* Cancel previous task.
         */
        PnfsId pnfsId = entry.getPnfsId();
        ScheduledFuture future = _tasks.remove(pnfsId);
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

    // Callbacks for fault notification ////////////////////////////////////

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
     * Runnable for removing expired sticky flags.
     */
    class ExpirationTask implements Runnable
    {
        private final MetaDataRecord _entry;

        public ExpirationTask(MetaDataRecord entry)
        {
            _entry = entry;
        }

        public void run()
        {
            _tasks.remove(_entry.getPnfsId());
            removeExpiredStickyFlags(_entry);
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
    public String ac_set_max_diskspace_$_1(Args args)
    {
        _runtimeMaxSize = UnitInteger.parseUnitLong(args.argv(0));
        if (_initialised) {
            updateAccountSize();
        }
        return "";
    }

    public void printSetup(PrintWriter pw)
    {
        if (_runtimeMaxSize < Long.MAX_VALUE) {
            pw.println("set max diskspace " + _runtimeMaxSize);
        }
    }

    private long getConfiguredMaxSize()
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
    private void updateAccountSize()
    {
        if (!_initialised) {
            throw new IllegalStateException("Pool is not yet initialized");
        }

        synchronized (_account) {
            long configuredMaxSize = getConfiguredMaxSize();
            long fileSystemMaxSize = getFileSystemMaxSize();
            boolean hasConfiguredMaxSize = (configuredMaxSize < Long.MAX_VALUE);
            long used = _account.getUsed();

            if (!isTotalSpaceReported()) {
                _log.warn("Java reported the file system size as 0. This typically happens on Solaris with a 32-bit JVM. Please use a 64-bit JVM.");
                if (!hasConfiguredMaxSize) {
                    throw new IllegalStateException("Failed to determine file system size. A pool size must be configured.");
                }
            }

            if (hasConfiguredMaxSize && fileSystemMaxSize < configuredMaxSize) {
                _log.warn(String.format("Configured pool size (%d) is larger than what is available on disk (%d)", configuredMaxSize, fileSystemMaxSize));
            }

            if (configuredMaxSize < used) {
                _log.warn(String.format("Configured pool size (%d) is smaller than what is used already (%d)", configuredMaxSize, used));
            }

            long newSize =
                Math.max(used, Math.min(configuredMaxSize, fileSystemMaxSize));
            if (newSize != _account.getTotal()) {
                _log.info("Adjusting pool size to " + newSize);
                _account.setTotal(newSize);
            }
        }
    }
}