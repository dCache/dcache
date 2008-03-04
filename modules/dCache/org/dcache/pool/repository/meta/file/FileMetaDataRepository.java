package org.dcache.pool.repository.meta.file;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

import org.dcache.pool.repository.DataFileRepository;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.EventProcessor;
import org.dcache.pool.repository.EventType;
import org.dcache.pool.repository.MetaDataRepository;
import org.dcache.pool.repository.v3.RepositoryException;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.event.CacheRepositoryEvent;

/**
 *
 * This class wraps the old control- and SI-file- based metadata
 * access method.
 */
public class FileMetaDataRepository
    implements MetaDataRepository, EventProcessor
{
    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    private static final String DIRECTORY_NAME = "control";

    private DataFileRepository _dataRepository;
    private EventProcessor _eventProcessor;
    private File _metadir;

    public FileMetaDataRepository(DataFileRepository dataRepository,
                                  EventProcessor eventProcessor,
                                  File baseDir)

    {
    	_dataRepository = dataRepository;
    	_eventProcessor = eventProcessor;
    	_metadir = new File(baseDir, DIRECTORY_NAME);
    }

    public CacheRepositoryEntry create(PnfsId id)
        throws DuplicateEntryException, RepositoryException
    {
        try {
            File controlFile = new File(_metadir, id.toString());
            File siFile = new File(_metadir, "SI-" + id.toString());
            File dataFile = _dataRepository.get(id);

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

            return new CacheRepositoryEntryImpl(_eventProcessor, id,
                                                controlFile, dataFile, siFile);
        } catch (IOException e) {
            throw new RepositoryException("Failed to create new entry: "
                                          + e.getMessage());
        }
    }

    // todo
    public CacheRepositoryEntry create(CacheRepositoryEntry entry)
        throws DuplicateEntryException, CacheException {

        return null;
    }

    public CacheRepositoryEntry get(PnfsId id) {

        try {
            File siFile = new File(_metadir, "SI-"+id.toString());
            if (siFile.exists()) {
                File controlFile = new File(_metadir, id.toString());
                File dataFile = _dataRepository.get(id);

                return new CacheRepositoryEntryImpl(_eventProcessor, id, controlFile, dataFile, siFile);
            }
        } catch (RepositoryException e) {

        } catch (IOException e) {

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

    /**
     * Forwards an event to the CacheRepository. Used by the entries
     * for event notification.
     */
    public void processEvent(EventType type, CacheRepositoryEvent event)
    {
        _eventProcessor.processEvent(type, event);
    }


    /**
     * Returns the path
     */
    public String toString()
    {
        return _metadir.toString();
    }

}
