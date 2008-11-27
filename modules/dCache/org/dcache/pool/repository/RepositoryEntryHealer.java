package org.dcache.pool.repository;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.dcache.pool.classic.ChecksumModuleV1;
import org.dcache.pool.classic.PoolIOWriteTransfer;
import org.dcache.pool.repository.DataFileRepository;
import org.dcache.pool.repository.MetaDataRepository;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.GenericStorageInfo;

import dmg.cells.nucleus.NoRouteToCellException;

/**
 * The RepositoryEntryHealer encapsulates the logic for recovering
 * CacheRepositoryEntry objects from PNFS in case they are missing or
 * broken in a MetaDataRepository.
 */
public class RepositoryEntryHealer
{
    private final static Logger _log =
        Logger.getLogger(RepositoryEntryHealer.class);

    private final static String MISSING_MSG =
        "Recovering: Missing meta data entry for %1$s.";
    private final static String PARTIAL_FROM_TAPE_MSG =
        "Recovering: Removing %1$s because it was not fully staged.";
    private final static String MISSING_SI_MSG =
        "Recovering: Storage info for %1$s was lost.";
    private final static String FILE_NOT_FOUND_MSG =
        "Recovering: Removing %1$s because name space entry was deleted.";
    private final static String UPDATE_SIZE_MSG =
        "Recovering: Setting size of %1$s in PNFS to %2$d";

    private final static String BAD_MSG =
        "Marking %1$s as bad: %2$s"; 
    private final static String BAD_SIZE_MSG =
        "File size mismatch for %1$s. Expected %2$d bytes, but found %3$d bytes.";

    private final PnfsHandler _pnfsHandler;
    private final MetaDataRepository _metaRepository;
    private final DataFileRepository _dataRepository;
    private final MetaDataRepository _oldRepository;
    private final ChecksumModuleV1 _checksumModule;

    public RepositoryEntryHealer(PnfsHandler pnfsHandler,
                                 ChecksumModuleV1 checksumModule,
                                 DataFileRepository dataRepository,
                                 MetaDataRepository metaRepository)
    {
        this(pnfsHandler,checksumModule, dataRepository, metaRepository, null);
    }

    public RepositoryEntryHealer(PnfsHandler pnfsHandler,
                                 ChecksumModuleV1 checksumModule,
                                 DataFileRepository dataRepository,
                                 MetaDataRepository metaRepository,
                                 MetaDataRepository oldRepository)
    {
        _pnfsHandler = pnfsHandler;
        _checksumModule = checksumModule;
        _dataRepository = dataRepository;
        _metaRepository = metaRepository;
        _oldRepository = oldRepository;
    }

    /**
     * Retrieves a CacheRepositoryEntry from a MetaDataRepository. If
     * the entry is missing or fails consistency checks, the entry is
     * reconstructed with information from PNFS.
     */
    public CacheRepositoryEntry entryOf(PnfsId id)
        throws IOException, IllegalArgumentException, CacheException,
               InterruptedException
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

        boolean isBroken =
            (entry == null) ||
            (entry.getStorageInfo() == null) ||
            (entry.getStorageInfo().getFileSize() != entry.getSize()) ||
            (!entry.isPrecious() && !entry.isCached()) || 
            entry.isBad();
        
        if (entry == null) {
            _log.warn(String.format(MISSING_MSG, id));
            entry = _metaRepository.create(id);
        }

        if (isBroken) {
            try {
                /* It is safe to remove FROM_STORE files: We have a copy
                 * on HSM anyway.
                 */
                if (entry.isReceivingFromStore()) {
                    _log.info(String.format(PARTIAL_FROM_TAPE_MSG, id));
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    return null;
                }

                /* Make sure that the copy is registered in PNFS. This
                 * may fail with FILE_NOT_FOUND if the file was
                 * already deleted.
                 */
                _pnfsHandler.addCacheLocation(id);

                /* In particular with the file backend, it could
                 * happen that the SI files was deleted outside of
                 * dCache. If storage info is missing, we try to fetch
                 * a new copy from PNFS.
                 */
                StorageInfo info = entry.getStorageInfo();
                if (info == null) {
                    _log.warn(String.format(MISSING_SI_MSG, id));
                    info = _pnfsHandler.getStorageInfo(id.toString());
                    entry.setStorageInfo(info);
                }

                /* If the intended file size is known, then compare it
                 * to the actual file size on disk. Fail in case of a
                 * mismatch. Notice we do this before the checksum
                 * check: First of all it is a lot cheaper than the
                 * checksum check and we may thus safe some time for
                 * incomplete files. Second, if file size is known but
                 * checksum is not, then we want to fail before the
                 * checksum is updated in PNFS.
                 */
                if (info.getFileSize() > 0 && info.getFileSize() != length) {
                    throw new CacheException(String.format(BAD_SIZE_MSG, id, info.getFileSize(), length));
                }

                /* Compute and update checksum. May fail if there is a
                 * mismatch. 
                 */
                if (_checksumModule != null) {
                    ChecksumFactory factory = 
                        _checksumModule.getDefaultChecksumFactory();
                    _checksumModule.setMoverChecksums(id, file, factory, 
                                                      null, null);
                }
            
                /* Update the size in the storage info and in PNFS if
                 * file size is unknown. TODO: Check that we are not
                 * precious or cached already - in that case we know
                 * file size has been set.
                 */
                if (info.getFileSize() == 0 && length > 0) {
                    _log.warn(String.format(UPDATE_SIZE_MSG, id, length));
                    _pnfsHandler.setFileSize(id, length);
                    info.setFileSize(length);
                    entry.setStorageInfo(info);
                }

                /* If not already precious or cached, we move the entry to
                 * the target state of a newly uploaded file.
                 */
                if (!entry.isCached() && !entry.isPrecious()) {
                    for (StickyRecord record: PoolIOWriteTransfer.getStickyRecords(info)) {
                        entry.setSticky(record.owner(), record.expire(), false);
                    }
            
                    if (PoolIOWriteTransfer.getTargetState(info) == EntryState.PRECIOUS) {
                        entry.setPrecious();
                    } else {
                        entry.setCached();
                    }
                }

                entry.setBad(false);
            } catch (CacheException e) {
                switch (e.getRc()) {
                case CacheException.FILE_NOT_FOUND:
                    _log.warn(String.format(FILE_NOT_FOUND_MSG, id));
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    return null;

                case CacheException.TIMEOUT:
                    throw e;

                default:
                    _log.error(String.format(BAD_MSG, id, e.getMessage()));
                    entry.setBad(true);
                    break;
                }
            } catch (NoRouteToCellException e) {
                /* As far as the caller of entryOf is concerned, there
                 * is no difference between the PnfsManager being down
                 * and it timing out. We therefore masquerade the
                 * exception as a timeout.
                 */ 
                throw new CacheException(CacheException.TIMEOUT, 
                                         "Timeout talking to PnfsManager");
            }
        }

        return entry;
    }
}
