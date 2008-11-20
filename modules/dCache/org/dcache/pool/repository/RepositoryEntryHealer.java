package org.dcache.pool.repository;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.dcache.pool.repository.DataFileRepository;
import org.dcache.pool.repository.MetaDataRepository;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.GenericStorageInfo;

/**
 * The RepositoryEntryHealer encapsulates the logic for recovering
 * CacheRepositoryEntry objects from PNFS in case they are missing or
 * broken in a MetaDataRepository.
 */
public class RepositoryEntryHealer
{
    private final static Logger _log =
        Logger.getLogger("logger.org.dcache.repository."+RepositoryEntryHealer.class.getName());

    private final static String REPLICA_BAD_SIZE_MSG =
        "Replica %1$s has an incorrect size. It is %2$d and should " +
        "have been %3$d. Marking replica as bad.";
    private final static String PNFS_BAD_SIZE_MSG =
        "File size missing in PNFS for %1$s. Setting it to %2$d.";
    private final static String BAD_SIZE_MSG =
        "File size mismatch for %1$s. %2$d according to cached " +
        "meta data, but %3$d in replica.";
    private final static String PARTIAL_FROM_CLIENT_MSG =
        "%1$s is incomplete. Recovering meta data from PNFS and " +
        "marking replica as CACHED and STICKY.";
    private final static String PARTIAL_FROM_TAPE_MSG =
        "%1$s is not fully staged. Deleting replica.";
    private final static String MISSING_SI_MSG =
        "Storage info for %1$s was lost.";

    private final PnfsHandler _pnfsHandler;
    private final MetaDataRepository _metaRepository;
    private final DataFileRepository _dataRepository;
    private final MetaDataRepository _oldRepository;


    public RepositoryEntryHealer(PnfsHandler pnfsHandler,
                                 DataFileRepository dataRepository,
                                 MetaDataRepository metaRepository)
    {
        this(pnfsHandler,dataRepository, metaRepository, null );
    }

    public RepositoryEntryHealer(PnfsHandler pnfsHandler,
                                 DataFileRepository dataRepository,
                                 MetaDataRepository metaRepository,
                                 MetaDataRepository oldRepository)
    {
        _pnfsHandler = pnfsHandler;
        _dataRepository = dataRepository;
        _metaRepository = metaRepository;
        _oldRepository = oldRepository;
    }

    protected CacheRepositoryEntry reconstruct(File file, PnfsId id)
        throws CacheException
    {
        /* Get new storage info from pnfs
         */
        try {
            /* We add cache location first to make sure it is
             * registered even if the following calls fail with
             * NOT_IN_TRASH.
             */
            _pnfsHandler.addCacheLocation(id);

            StorageInfo storageInfo =
                _pnfsHandler.getStorageInfo(id.toString());

            boolean precious =
                storageInfo.getRetentionPolicy().equals(RetentionPolicy.CUSTODIAL) && !storageInfo.isStored();
            boolean sticky =
                storageInfo.getAccessLatency().equals(AccessLatency.ONLINE);
            boolean bad = false;

            CacheRepositoryEntry entry = _metaRepository.create(id);

            /* If the file size does not match the one in PNFS, we
             * have no way to know which is correct.
             *
             * We make the assumption that if the file is precious and
             * has zero size in PNFS, then the replica's file size is
             * the correct one. In this case we update the file size
             * in PNFS.
             *
             * Otherwise the replica is corrupted and we mark it as
             * bad.
             *
             * REVISIT: As of this writing, we are revising the
             * semantics of size information stored at the pool, so
             * the following code should be updated again.
             */
            long length = file.length();
            if (storageInfo.getFileSize() != length) {
                if (precious && storageInfo.getFileSize() == 0) {
                    _log.warn(String.format(PNFS_BAD_SIZE_MSG, id, length));
                    _pnfsHandler.setFileSize(id, length);
                } else {
                    _log.error(String.format(REPLICA_BAD_SIZE_MSG, id, length,
                                             storageInfo.getFileSize()));
                    bad = true;
                }
                storageInfo.setFileSize(length);
            }

            /* The order in which we update the entry is significant:
             *
             * - The storage information defines the file size and
             *   must be set before the file is marked cached.
             *
             * - The sticky flag must be set before marking the file
             *   cached to avoid that the file is prematurely garbage
             *   collected.
             *
             * - Bad must be set last, as state transitions on bad
             *   entries are not allowed.
             */
            entry.setStorageInfo(storageInfo);

            if (bad) {
                entry.setBad(true);
            } else {
                if (sticky) {
                    entry.setSticky(true, "system", -1);
                }
 
                if (precious) {
                    entry.setPrecious(true);
                } else {
                    entry.setCached();
                }
            }

            _log.warn("Meta data recovered: " + entry.toString());

            return entry;
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.NOT_IN_TRASH:
                long length = file.length();
                if (length > 0) {
                    _log.warn(id + " is not in trash. Keeping replica...");
 
                    /* To avoid misacounting in the pool and complains
                     * about the file not showing up in 'rep ls', we
                     * create a meta data entry for the file.
                     */
                    CacheRepositoryEntry entry = _metaRepository.create(id);
                    StorageInfo storageInfo = new GenericStorageInfo();
                    storageInfo.setFileSize(length);
                    entry.setStorageInfo(storageInfo);
                    entry.setBad(true);
 
                    return entry;
                } else {
                    _log.warn(id + " is not in trash, but is empty. Removing replica...");
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    return null;
                }
 
            case CacheException.FILE_NOT_FOUND:
                _log.warn(id + " was deleted. Removing replica...");
                _metaRepository.remove(id);
                file.delete();
                return null;

            default:
                throw e;
            }
        }
    }


    /**
     * Retrieves a CacheRepositoryEntry from a MetaDataRepository. If
     * the entry is missing or fails consistency checks, the entry is
     * reconstructed with information from PNFS.
     */
    public CacheRepositoryEntry entryOf(PnfsId id)
        throws IOException, IllegalArgumentException, CacheException
    {
        File file = _dataRepository.get(id);
        if (!file.isFile()) {
            throw new IllegalArgumentException("File not does exist: " + id);
        }

        long length = file.length();
        CacheRepositoryEntry entry = _metaRepository.get(id);

        /* Import from old repository if possible.
         */
        if (entry == null && _oldRepository != null) {
            entry = _oldRepository.get(id);
            if (entry != null) {
                _log.warn("Imported meta data for " + id
                          + " from " + _oldRepository.toString());
                entry = _metaRepository.create(entry);
            }
        }

        /* Check entry and heal it if necessary.
         */
        if (entry == null) {
            _log.warn("Missing meta data for " + id);
            entry = reconstruct(file, id);
        } else if (entry.isBad()) {
            /* Make sure that the cache location is registered and
             * remove the replica if the file has been deleted, but
             * otherwise leave the entry as it is.
             */
            try {
                _pnfsHandler.addCacheLocation(id);
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    _log.warn(id + " was deleted. Removing replica...");
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    return null;
                }
            }
        } else if (entry.isReceivingFromClient()) {
            _log.warn(String.format(PARTIAL_FROM_CLIENT_MSG, id));

            try {
                StorageInfo storageInfo =
                    _pnfsHandler.getStorageInfo(id.toString());
                storageInfo.setFileSize(length);

                entry.setStorageInfo(storageInfo);
                entry.setSticky(true, "system", -1);
                entry.setCached();

                _pnfsHandler.addCacheLocation(id);
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    /* The file is already gone
                     */
                    _log.warn(id + " was deleted. Removing replica.");
                    _metaRepository.remove(id);
                    file.delete();
                } else if (e.getRc() == CacheException.NOT_IN_TRASH) {
                    _log.warn(id + " is not in trash. Keep replica...");
                } else
                    throw e;
            }
        } else if (entry.isReceivingFromStore()) {
            // it's safe to remove partialyFromStore file, we have a
            // copy on HSM anyway
            _log.info(String.format(PARTIAL_FROM_TAPE_MSG, id));
            _metaRepository.remove(id);
            file.delete();
            return null;
        } else if (!entry.isCached() && !entry.isPrecious()) {
            /* Make sure that the cache location is registered and
             * remove the replica if the file has been deleted, but
             * otherwise leave the entry as it is.
             */
            entry.setBad(true);
            try {
                _pnfsHandler.addCacheLocation(id);
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    _log.warn(id + " was deleted. Removing replica...");
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    return null;
                }
            }
        } else if (entry.getStorageInfo() == null) {
            _log.warn(String.format(MISSING_SI_MSG, id));
            _metaRepository.remove(id);
            entry = reconstruct(file, id);
        } else if (entry.getSize() != length) {
            _log.warn(String.format(BAD_SIZE_MSG, id, entry.getSize(), length));
            _metaRepository.remove(id);
            entry = reconstruct(file, id);
        }

        return entry;
    }
}
