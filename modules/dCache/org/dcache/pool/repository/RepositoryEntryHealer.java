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

/**
 * The RepositoryEntryHealer encapsulates the logic for recovering
 * CacheRepositoryEntry objects from PNFS in case they are missing or
 * broken in a MetaDataRepository.
 */
public class RepositoryEntryHealer 
{
    private static Logger _log = 
        Logger.getLogger("logger.org.dcache.repository");

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
            StorageInfo storageInfo = 
                _pnfsHandler.getStorageInfo(id.toString());

            /* Update file size if it's wrong and update pnfs as well
             */
            long length = file.length();
            if (storageInfo.getFileSize() != length) {
                storageInfo.setFileSize(length);
                _pnfsHandler.setFileSize(id, length);
            }

            CacheRepositoryEntry entry = _metaRepository.create(id);
            entry.setStorageInfo(storageInfo);

            if (storageInfo.getRetentionPolicy().equals(RetentionPolicy.CUSTODIAL) && !storageInfo.isStored()) {
                entry.setPrecious(true);
            } else {
                entry.setCached();
            }
			
            if (storageInfo.getAccessLatency().equals(AccessLatency.ONLINE)) {
                entry.setSticky(true, "system", -1);
            }

            _log.warn("Meta data recovered: " + entry.toString());

            return entry;
        } catch (CacheException e) {
            if (e.getRc() != CacheException.FILE_NOT_FOUND) {
                throw e;
            }

            _log.info(id + ": file was deleted. Removing replica...");
            _metaRepository.remove(id);
            file.delete();
		
            /*
             * TODO: this part should take care that we do not
             * remove files is pnfs manager in trouble
             * 
             CacheRepositoryEntryState entryState = new CacheRepositoryEntryState(controlFile);
             if( entryState.canRemove() ) {
             _log.warn("removing missing removable entry : " + entryName );
             _repositoryTree.destroy(entryName);
                 
             // everybody is happy						 
                 
             return null;
             }else{
             _log.warn("mark as bad non removable missing entry : " + entryName );
             entryState.setSticky("system",-1);
             entryState.setError();
                 
             return repositoryEntry;
             }
            */
            return null;
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
        } else if (entry.isReceivingFromClient()) {
            /*
             * well, following steps have to be done:
             *
             *    1. get storageInfo from pnfs
             *    2. mark file as CACHED + STICKY
             *    3. add cache location
             */
            try {
                StorageInfo storageInfo =
                    _pnfsHandler.getStorageInfo(id.toString());
                storageInfo.setFileSize(length);

                entry = _metaRepository.create(id);                
                entry.setCached();
                entry.setSticky(true, "system", -1);
                entry.setBad(true);
                entry.setStorageInfo(storageInfo);

                _pnfsHandler.addCacheLocation(id);
            } catch (CacheException e) {
                if (e.getRc() != CacheException.FILE_NOT_FOUND) {
                    throw e;
                }
                 
                /* The file is already gone
                 */
                _log.info(id + ": partial file was deleted. Removing replica...");
                _metaRepository.remove(id);
                file.delete();
            }
        } else if (entry.isReceivingFromStore()) {
            // it's safe to remove partialyFromStore file, we have a
            // copy on HSM anyway
            _log.info(id + ": removing partially staged file.");
            _metaRepository.remove(id);
            file.delete();
            return null;
        } else if (entry.getStorageInfo() == null) {
            _log.warn(id + "Storage info for " + id + " was lost");
            _metaRepository.remove(id);
            entry = reconstruct(file, id);
        } else if (entry.getSize() != length) {
            _log.warn("File size mismatch for " + id + "(" 
                      + entry.getSize() 
                      + " bytes according to meta data, but "
                      + length 
                      + " bytes in actual file)");
            _metaRepository.remove(id);
            entry = reconstruct(file, id);
        }        

        return entry;
    }
}
