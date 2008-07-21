package org.dcache.pool.repository.v4;

import org.apache.log4j.Logger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.Collections;

import com.sleepycat.je.DatabaseException;

import dmg.util.Args;

import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.StickyInspector;
import org.dcache.pool.repository.EventType;
import org.dcache.pool.repository.AbstractCacheRepository;
import org.dcache.pool.repository.MetaDataRepository;
import org.dcache.pool.repository.DataFileRepository;
import org.dcache.pool.repository.FlatDataFileRepository;
import org.dcache.pool.repository.RepositoryEntryHealer;
import org.dcache.pool.repository.CacheEntryLRUOrder;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.v3.RepositoryException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.CacheRepositoryEntryInfo;

/**
 * The CacheRepositoryV4 is an implementation of the CacheRepository
 * interface. It is based on the DataFileRepository and
 * MetaDataRepository interfaces.
 *
 * Currently the class hard-codes the use of a flat data repository
 * layout and of a Berkeley DB based meta data repoository.
 *
 * The class employs a reader-writer lock to control concurrent
 * access.  For compatibility with older implementations of the
 * interface, a few methods are also marked as synchronized. This
 * allows the client to obtain and lock an entry atomically.
 */
public class CacheRepositoryV4 extends AbstractCacheRepository
{
    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    /**
     * Name of file in which space reservation information is stored.
     */
    private static final String SPACE_RESERVATION = "SPACE_RESERVATION";


    private static final String DEFAULT_META_DATA_REPOSITORY =
        "org.dcache.pool.repository.meta.file.FileMetaDataRepository";

    /**
     * File containing the information about the current amount of
     * reserved space.
     */
    private final File _spaceReservation;

    /**
     * Reader-writer lock used for most access to the cache
     * repository.
     */
    private final ReadWriteLock _operationLock =
        new ReentrantReadWriteLock();

    /**
     * Map of all entries.
     */
    private final Map<PnfsId, CacheRepositoryEntry> _allEntries =
        new HashMap<PnfsId, CacheRepositoryEntry>();

    /**
     * File layout within pool.
     */
    private final DataFileRepository _dataRepository;

    /**
     * Meta data about files in the pool.
     */
    private final MetaDataRepository _metaRepository;

    /**
     * Meta data to import of <i>_metaRepository</i> does not contain
     * it.
     */
    private final MetaDataRepository _importRepository;

    /**
     * The sticky inspector expires running
     */
    private StickyInspector _stickyInspector;

    /**
     * True while an inventory is build. During this period we block
     * event processing.
     */
    private boolean _runningInventory = false;

    /**
     * The base directory of the pool. This directory contains the
     * setup file.
     */
    private File _basedir;

    private MetaDataRepository
        createMetaDataRepository(String name, DataFileRepository data,
                                 File directory)
    {
        _basedir = directory;

        try {
            Class [] argClass = {
                org.dcache.pool.repository.DataFileRepository.class,
                org.dcache.pool.repository.EventProcessor.class,
                java.io.File.class
            };
            Class       repositoryClass = Class.forName(name);
            Constructor con             = repositoryClass.getConstructor(argClass);
            Object []   args         = { data, this, directory };
            return (MetaDataRepository)con.newInstance(args);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class " + name + " could not be found");
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Class " + name + " does not implement expected constructor");
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Class " + name + " could not be instantiated", e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Class " + name + " could not be instantiated", e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Class " + name + " does not implement MetaDataRepository");
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Class " + name + " could not be accessed", e);
        }
    }

    /**
     * Creates a new instance.
     *
     * @param directory the pool base directory.
     */
    public CacheRepositoryV4(File directory, Args args)
        throws FileNotFoundException, DatabaseException
    {
        String importClass = args.getOpt("metaDataRepositoryImport");
        String metaClass = args.getOpt("metaDataRepository");
        if (metaClass == null) {
            metaClass = DEFAULT_META_DATA_REPOSITORY;
        }

        _dataRepository =
            new FlatDataFileRepository(directory);
        _metaRepository =
            createMetaDataRepository(metaClass, _dataRepository, directory);
        if (importClass != null && importClass.length() != 0) {
            _importRepository = createMetaDataRepository(importClass,
                                                         _dataRepository,
                                                         directory);
        } else {
            _importRepository = null;
        }

        _spaceReservation =
            new File(directory, SPACE_RESERVATION);
    }

    public boolean contains(PnfsId pnfsId)
    {
        _operationLock.readLock().lock();
        try {
            return _allEntries.containsKey(pnfsId);
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    public synchronized CacheRepositoryEntry createEntry(PnfsId pnfsId)
        throws FileInCacheException, RepositoryException
    {
        if (_log.isInfoEnabled()) {
            _log.info("Creating new entry for " + pnfsId);
        }

        _operationLock.writeLock().lock();
        try {
            File dataFile = _dataRepository.get(pnfsId);
            if (_allEntries.containsKey(pnfsId) || dataFile.exists()) {
                _log.warn("Entry already exists: " + pnfsId);
                throw new
                    FileInCacheException("Entry already exists: " + pnfsId);
            }

            CacheRepositoryEntry entry;
            try {
                entry = _metaRepository.create(pnfsId);
            } catch (DuplicateEntryException e) {
                _log.warn("Deleting orphaned meta data entry for " + pnfsId);
                _metaRepository.remove(pnfsId);
                try {
                    entry = _metaRepository.create(pnfsId);
                } catch (DuplicateEntryException f) {
                    throw
                        new RuntimeException("Unexpected repository error", e);
                }
            }
            _allEntries.put(pnfsId, entry);
            return entry;
        } finally {
            _operationLock.writeLock().unlock();
        }
    }

    /**
     * @return CacheRepositoryEntry on pnfsid, exclude in removed state
     * @throw FileNotInCacheException in case file is not in
     *        repository or in removed state
     */
    public CacheRepositoryEntry getEntry(PnfsId pnfsId)
        throws CacheException
    {
        CacheRepositoryEntry entry = getGenericEntry(pnfsId);

        if (entry.isRemoved()) {
            throw new FileNotInCacheException("Entry not in repository (removed): " + pnfsId);
        }

        return entry;
    }

    /**
     * @return CacheRepositoryEntry on pnfsid, including in removed state
     * @throw FileNotInCacheException in case of file is not in repository
     */
    public CacheRepositoryEntry getGenericEntry(PnfsId pnfsId)
        throws FileNotInCacheException
    {
        _operationLock.readLock().lock();
        try {
            CacheRepositoryEntry entry = _allEntries.get(pnfsId);
            if (entry == null) {
                throw new FileNotInCacheException("Entry not in repository : "
                                                  + pnfsId);
            }
            return entry;
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    public List<PnfsId> getValidPnfsidList()
    {
        _operationLock.readLock().lock();
        try {
            Collection<CacheRepositoryEntry> entries = _allEntries.values();
            List<PnfsId> validIds = new ArrayList<PnfsId>(entries.size());
            for (CacheRepositoryEntry entry : entries) {
                if (entry.isCached() || entry.isPrecious()) {
                    validIds.add(entry.getPnfsId());
                }
            }
            return validIds;
        } catch (CacheException e) {
            throw new RuntimeException("Bug. This should not happen.", e);
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    public List<CacheRepositoryEntryInfo> getValidCacheRepostoryEntryInfoList()
    {
        _operationLock.readLock().lock();
        try {
            Collection<CacheRepositoryEntry> entries = _allEntries.values();
            List<CacheRepositoryEntryInfo> validIds =
                    new ArrayList<CacheRepositoryEntryInfo>(entries.size());
            for (CacheRepositoryEntry entry : entries) {
                if (entry.isCached() || entry.isPrecious()) {
                    validIds.add(new CacheRepositoryEntryInfo(entry));
                }
            }
            return validIds;
        } catch (CacheException e) {
            throw new RuntimeException("Bug. This should not happen.", e);
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    /**
     * return all known iterator pnfsid
     */
    public Iterator<PnfsId> pnfsids()
    {
        _operationLock.readLock().lock();
        try {
            List<PnfsId> allEntries =
                new ArrayList<PnfsId>(_allEntries.keySet());
            return allEntries.iterator();
        } finally {
            _operationLock.readLock().unlock();
        }
    }

    /**
     * Mark entry as removed. The file will be physically deleted as
     * soon as it is no longer in use.
     *
     * Implementation note: We rely on all our entries to remove
     * themself from the meta data repository and send a DESTROY event,
     * when they have been marked as removed and their link count
     * drops to zero.
     */
    public synchronized boolean removeEntry(CacheRepositoryEntry entry)
        throws CacheException, FileNotInCacheException
    {
        if (_log.isInfoEnabled()) {
            _log.info("remove entry for: " + entry.getPnfsId().toString());
        }

        _operationLock.writeLock().lock();
        try {
            PnfsId pnfsId = entry.getPnfsId();

            if (!_allEntries.containsKey(pnfsId)) {
                throw new FileNotInCacheException("Entry already removed");
            }

            if (entry.isLocked()) {
                return false;
            }

            entry.setRemoved();
        } finally {
            _operationLock.writeLock().unlock();
        }
        return true;
    }

    public void runInventory() throws CacheException
    {
        runInventory(null, 0);
    }

    /**
     * Returns true if this file is removable. This is the case if the
     * file is not sticky and is cached (which under normal
     * circumstances implies that it is ready and not precious).
     */
    private boolean isRemovable(CacheRepositoryEntry entry)
    {
        try {
            synchronized (entry) {
                return !entry.isReceivingFromClient()
                    && !entry.isReceivingFromStore()
                    && !entry.isPrecious()
                    && !entry.isSticky()
                    && entry.isCached();
            }
        } catch (CacheException e) {
            /* Returning false is the safe option.
             */
            return false;
        }
    }

    /* Must not be executed more than once!
     */
    public synchronized void runInventory(PnfsHandler pnfs, int flags)
        throws CacheException
    {
        if (!_allEntries.isEmpty())
            throw new IllegalStateException("Repository already has an inventory");

        _log.info("Generating inventory");

        List<PnfsId> ids = _dataRepository.list();
        long usedDataSpace = 0L;
        long removableSpace = 0L;

        _log.info("Found " + ids.size() + " data files");

        /* On some file systems (e.g. GPFS) stat'ing files in
         * lexicographic order seems to trigger the pre-fetch
         * mechanism of the file system.
         */
        Collections.sort(ids);

        RepositoryEntryHealer healer =
            new RepositoryEntryHealer(pnfs, _dataRepository,
                                      _metaRepository, _importRepository);

        _operationLock.writeLock().lock();
        try {
            _runningInventory = true;

            _log.info("Checking repository consistency");

            /* Collect all entries.
             */
            for (PnfsId id: ids) {
                CacheRepositoryEntry entry = healer.entryOf(id);
                if (entry == null)  {
                    continue;
                }

                long size = entry.getSize();
                usedDataSpace += size;
                if (isRemovable(entry)) {
                    removableSpace += size;
                }

                if (_log.isDebugEnabled()) {
                    _log.debug(id +" " + entry.getState());
                }

                _allEntries.put(id, entry);
            }

            _log.info("Registering files with event listeners");


            /* Detect overbooking.
             */
            long total = getTotalSpace();
            if (usedDataSpace > total) {
                String error =
                    "Overbooked, " + usedDataSpace +
                    " bytes of data exceeds inventory size of " +
                    total + " bytes";
                if ((flags & ALLOW_SPACE_RECOVERY) == 0)
                    throw new CacheException(206, error);

                _log.error(error);

                if (usedDataSpace - removableSpace > total) {
                    throw new
                        CacheException("Inventory overbooked and excess data is not removable. Cannot recover.");
                }

                _log.warn("Found " + removableSpace + " bytes of removable data. Proceeding by removing excess data.");
            }

            /* Report SCAN events and resolve overbooking in LRU order.
             */
            List<CacheRepositoryEntry> entries =
                new ArrayList<CacheRepositoryEntry>(_allEntries.values());
            Collections.sort(entries, new CacheEntryLRUOrder());
            for (CacheRepositoryEntry entry : entries) {
                processEvent(EventType.SCAN,
                             new CacheRepositoryEvent(this, entry));
                if (isRemovable(entry) && usedDataSpace > total) {
                    long size = entry.getSize();
                    _log.warn("Pool overbooked: " + entry.getPnfsId()
                              + " removed");
                    usedDataSpace -= size;
                    removableSpace -= size;
                    removeEntry(entry);
                }
            }

            if (usedDataSpace != getTotalSpace() - getFreeSpace()) {
                throw new RuntimeException(String.format("Bug detected: Allocated space is not what we expected (%d vs %d)", usedDataSpace, getTotalSpace() - getFreeSpace()));
            }

            _log.info(String.format("Inventory contains %d files; total size is %d; used space is %d; free space is %d.",
                                    _allEntries.size(), getTotalSpace(),
                                    usedDataSpace, getFreeSpace()));

            /* We have to create the sticky inspector as the last
             * thing. Otherwise we would risk that it changes the
             * sticky flag on entries while generating the inventory,
             * which would screw up the accounting.
             */
            _runningInventory = false;

            _stickyInspector = new StickyInspector(_allEntries.values());
            addCacheRepositoryListener(_stickyInspector);
        } catch (IOException e) {
            throw new CacheException(ERROR_IO_DISK , "Failed to load repository: " + e);
        } finally {
            _runningInventory = false;
            _operationLock.writeLock().unlock();
        }

        _log.info("Done generating inventory");
    }

    public boolean isRepositoryOk()
    {
       try {
           if (!_metaRepository.isOk() || !_dataRepository.isOk())
               return false;

           File setup = new File(_basedir, "setup");
           if (!setup.exists()) {
               _log.fatal("setup file is missing");
               return false;
           }

           File tmp = new File(_basedir, "RepositoryOk");
	   tmp.delete();
	   tmp.deleteOnExit();

           if (!tmp.createNewFile() || !tmp.exists()) {
               _log.fatal("Could not create " + tmp);
               return false;
           }

	   return true;
	} catch (IOException e) {
           _log.error("Repository failure: " + e);
	   return false;
	}
    }

    /**
     * Removes an entry from the in-memory cache and erases the data
     * file. Since <code>destroyEntry</code> is called as a result of
     * the entry being removed from the meta data repository, the
     * method does not remove the entry from the meta data repository.
     */
    private void destroyEntry(CacheRepositoryEntry entry)
    {
        _operationLock.writeLock().lock();
        try {
            PnfsId id = entry.getPnfsId();
            _allEntries.remove(id);
            _dataRepository.get(id).delete();
        } finally {
            _operationLock.writeLock().unlock();
        }
    }

    public void processEvent(EventType type, CacheRepositoryEvent event)
    {
        switch (type) {
        case SCAN:
        case REMOVE:
            super.processEvent(type, event);
            break;

        case DESTROY:
            super.processEvent(type, event);
            destroyEntry(event.getRepositoryEntry());
            break;

        default:
            /* Entries updated during entry healing will propagate
             * events. We do however not want to process those events,
             * since the inventory algorithm takes care of accounting.
             */
            if (!_runningInventory) {
                super.processEvent(type, event);
            }
            break;
        }
    }

    protected void storeReservedSpace() throws CacheException
    {
        try {
            PrintWriter pw =
                new PrintWriter(new FileOutputStream(_spaceReservation));
            try {
                pw.println(getReservedSpace());
            } finally {
                pw.close();
            }
        } catch (IOException e) {
            throw new
                CacheException(103, "IO Exception, writing " + _spaceReservation) ;
        }
    }

    public void close()
    {
        if (_stickyInspector != null) {
            _stickyInspector.close();
        }
        _metaRepository.close();
    }
}
