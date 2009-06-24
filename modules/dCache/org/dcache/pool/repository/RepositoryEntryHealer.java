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

    private final static String RECOVERING_MSG =
        "Recovering %1$s...";
    private final static String MISSING_MSG =
        "Recovering: Reconstructing meta data for %1$s";
    private final static String PARTIAL_FROM_TAPE_MSG =
        "Recovering: Removed %1$s because it was not fully staged";
    private final static String MISSING_SI_MSG =
        "Recovering: Fetched storage info for %1$s from PNFS";
    private final static String FILE_NOT_FOUND_MSG =
        "Recovering: Removed %1$s because name space entry was deleted";
    private final static String UPDATE_SIZE_MSG =
        "Recovering: Set size of %1$s in PNFS to %2$d";
    private final static String PRECIOUS_MSG =
        "Recovering: Marked %1$s precious";
    private final static String CACHED_MSG =
        "Recovering: Marked %1$s cached";

    private final static String BAD_MSG =
        "Marked %1$s bad: %2$s";
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
                entry = _metaRepository.create(entry);
                _log.warn("Imported meta data for " + id
                          + " from " + _oldRepository.toString());
            }
        }

        boolean isBroken =
            (entry == null) ||
            (entry.getStorageInfo() == null) ||
            (entry.getStorageInfo().getFileSize() != entry.getSize()) ||
            (!entry.isPrecious() && !entry.isCached()) ||
            entry.isBad();


        if (isBroken) {
            if (entry == null) {
                entry = _metaRepository.create(id);
                _log.warn(String.format(MISSING_MSG, id));
            }

            try {
                _log.warn(String.format(RECOVERING_MSG, id));

                /* It is safe to remove FROM_STORE files: We have a
                 * copy on HSM anyway. Files in REMOVED or DESTROYED
                 * where about to be deleted, so we can finish the
                 * job.
                 */
                if (entry.isReceivingFromStore() ||
                    entry.isRemoved() ||
                    entry.isDestroyed()) {
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    _log.info(String.format(PARTIAL_FROM_TAPE_MSG, id));
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
                 * a new copy from PNFS. We also fetch storage info if
                 * the entry is neither cached nor precious; just to
                 * be sure that information about size and the stored
                 * bit is up to date.
                 */
                StorageInfo info = entry.getStorageInfo();
                if (info == null || (!entry.isCached() && !entry.isPrecious())) {
                    info = _pnfsHandler.getStorageInfo(id.toString());
                    entry.setStorageInfo(info);
                    _log.warn(String.format(MISSING_SI_MSG, id));
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
                if (!(entry.isReceivingFromClient() && info.getFileSize() == 0)
                    && info.getFileSize() != length) {
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
                 * file size is unknown.
                 */
                if (entry.isReceivingFromClient() && info.getFileSize() == 0) {
                    _pnfsHandler.setFileSize(id, length);
                    info.setFileSize(length);
                    entry.setStorageInfo(info);
                    _log.warn(String.format(UPDATE_SIZE_MSG, id, length));
                }

                /* If not already precious or cached, we move the entry to
                 * the target state of a newly uploaded file.
                 */
                if (!entry.isCached() && !entry.isPrecious()) {
                    for (StickyRecord record: PoolIOWriteTransfer.getStickyRecords(info)) {
                        entry.setSticky(record.owner(), record.expire(), false);
                    }

                    if (PoolIOWriteTransfer.getTargetState(info) == EntryState.PRECIOUS && !info.isStored()) {
                        entry.setPrecious();
                        _log.warn(String.format(PRECIOUS_MSG, id));
                    } else {
                        entry.setCached();
                        _log.warn(String.format(CACHED_MSG, id));
                    }
                }

                entry.setBad(false);
            } catch (CacheException e) {
                switch (e.getRc()) {
                case CacheException.FILE_NOT_FOUND:
                    _metaRepository.remove(id);
                    file.delete();
                    _pnfsHandler.clearCacheLocation(id);
                    _log.warn(String.format(FILE_NOT_FOUND_MSG, id));
                    return null;

                case CacheException.TIMEOUT:
                    throw e;

                default:
                    entry.setBad(true);
                    _log.error(String.format(BAD_MSG, id, e.getMessage()));
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
