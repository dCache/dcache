package org.dcache.pool.repository.meta.file;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.v3.RepositoryException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

/**
 *
 * This class wraps the old control- and SI-file- based metadata
 * access method.
 */
public class FileMetaDataRepository
    implements MetaDataStore
{
    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    private static final String DIRECTORY_NAME = "control";

    private FileStore _fileStore;
    private File _metadir;

    public FileMetaDataRepository(FileStore fileStore,
                                  File baseDir)

    {
    	_fileStore = fileStore;
    	_metadir = new File(baseDir, DIRECTORY_NAME);
        if (!_metadir.exists()) {
            _metadir.mkdir();
        }
    }

    public MetaDataRecord create(PnfsId id)
        throws DuplicateEntryException, CacheException
    {
        try {
            File controlFile = new File(_metadir, id.toString());
            File siFile = new File(_metadir, "SI-" + id.toString());
            File dataFile = _fileStore.get(id);

            /* We call get() to check whether the entry exists and is
             * intact.
             */
            if (get(id) != null) {
                throw new DuplicateEntryException(id);
            }

            /* In case of left over or corrupted files, we delete them
             * before creating a new entry.
             */
            if (controlFile.exists()) {
                controlFile.delete();
            }

            if (siFile.exists()) {
                siFile.delete();
            }

            return
                new CacheRepositoryEntryImpl(id, controlFile, dataFile, siFile);
        } catch (IOException e) {
            throw new RepositoryException("Failed to create new entry: "
                                          + e.getMessage());
        }
    }

    // todo
    public MetaDataRecord create(MetaDataRecord entry)
        throws DuplicateEntryException, CacheException {

        return null;
    }

    public MetaDataRecord get(PnfsId id)
        throws CacheException
    {
        try {
            File siFile = new File(_metadir, "SI-"+id.toString());
            if (siFile.exists()) {
                File controlFile = new File(_metadir, id.toString());
                File dataFile = _fileStore.get(id);

                return new CacheRepositoryEntryImpl(id, controlFile, dataFile, siFile);
            }
        } catch (IOException e) {
            throw new CacheException(CacheException.ERROR_IO_DISK,
                                     "Failed to read meta data for " + id);
        }
        return null;
    }

    public void remove(PnfsId id) {
        File controlFile = new File(_metadir, id.toString());
        File siFile = new File(_metadir, "SI-"+id.toString());

        controlFile.delete();
        siFile.delete();
    }

    public boolean isOk()
    {
        File tmp = new File(_metadir, ".repository_is_ok");
        try {
            tmp.delete();
            tmp.deleteOnExit();

            if (!tmp.createNewFile() || !tmp.exists()) {
                _log.fatal("Could not create " + tmp);
                return false;
            }

            return true;
	} catch (IOException e) {
            _log.fatal("Failed to touch " + tmp + ": " + e.getMessage());
            return false;
	}
    }

    /** NOP */
    public void close()
    {

    }

    /**
     * Returns the path
     */
    public String toString()
    {
        return _metadir.toString();
    }

}
