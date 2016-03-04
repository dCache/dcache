package org.dcache.pool.repository.meta.file;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.v3.RepositoryException;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;

/**
 *
 * This class wraps the old control- and SI-file- based metadata
 * access method.
 */
public class FileMetaDataRepository
    implements MetaDataStore
{
    private static final Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    private static final String DIRECTORY_NAME = "control";
    private static final String REMOVING_REDUNDANT_META_DATA =
            "Removing redundant meta data for %s.";

    private final FileStore _fileStore;
    private final File _metadir;

    public FileMetaDataRepository(FileStore fileStore,
                                  File baseDir)
        throws FileNotFoundException
    {
        this(fileStore, baseDir, false);
    }

    public FileMetaDataRepository(FileStore fileStore,
                                  File baseDir,
                                  boolean readOnly)
        throws FileNotFoundException
    {
    	_fileStore = fileStore;
    	_metadir = new File(baseDir, DIRECTORY_NAME);
        if (!_metadir.exists()) {
            if (readOnly) {
                throw new FileNotFoundException("No such directory and not allowed to create it: " + _metadir);
            }
            if (!_metadir.mkdir()) {
                throw new FileNotFoundException("Failed to create directory: " + _metadir);
            }
        } else if (!_metadir.isDirectory()) {
            throw new FileNotFoundException("No such directory: " + _metadir);
        }
    }

    @Override
    public void init() throws CacheException
    {
    }

    @Override
    public Set<PnfsId> index(IndexOption... options) throws CacheException
    {
        List<IndexOption> indexOptions = asList(options);

        if (indexOptions.contains(IndexOption.META_ONLY)) {
            return asList(_metadir.list()).stream()
                    .map(name -> name.startsWith("SI-") ? name.substring(3) : name)
                    .filter(PnfsId::isValid)
                    .map(PnfsId::new)
                    .collect(toSet());
        }

        Stopwatch watch = Stopwatch.createStarted();
        Set<PnfsId> files = _fileStore.index();
        _log.info("Indexed {} entries in {} in {}.", files.size(), _fileStore, watch);

        watch.reset().start();
        String[] metaFiles = _metadir.list();
        _log.info("Indexed {} entries in {} in {}.", metaFiles.length, _metadir, watch);

        if (indexOptions.contains(IndexOption.ALLOW_REPAIR)) {
            for (String name : metaFiles) {
                try {
                    String s = name.startsWith("SI-") ? name.substring(3) : name;
                    if (PnfsId.isValid(s) && !files.contains(new PnfsId(s))) {
                        Files.deleteIfExists(_metadir.toPath().resolve(name));
                    }
                } catch (IllegalArgumentException ignored) {
                } catch (IOException e) {
                    throw new DiskErrorCacheException(
                            "Failed to remove " + name + ": " + e.getMessage(), e);
                }
            }
        }
        return files;
    }

    @Override
    public MetaDataRecord create(PnfsId id)
        throws DuplicateEntryException, CacheException
    {
        try {
            File controlFile = new File(_metadir, id.toString());
            File siFile = new File(_metadir, "SI-" + id.toString());
            File dataFile = _fileStore.get(id);

            if (dataFile.exists()) {
                throw new DuplicateEntryException(id);
            }

            /* In case of left over or corrupted files, we delete them
             * before creating a new entry.
             */
            controlFile.delete();
            siFile.delete();

            return new CacheRepositoryEntryImpl(id, controlFile, dataFile, siFile);
        } catch (IOException e) {
            throw new RepositoryException(
                    "Failed to create new entry " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public MetaDataRecord copy(MetaDataRecord entry)
        throws CacheException
    {
        PnfsId id = entry.getPnfsId();
        try {
            File controlFile = new File(_metadir, id.toString());
            File siFile = new File(_metadir, "SI-" + id.toString());
            File dataFile = _fileStore.get(id);

            return new CacheRepositoryEntryImpl(id, controlFile, dataFile, siFile, entry);
        } catch (IOException e) {
            throw new RepositoryException(
                    "Failed to create new entry " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public MetaDataRecord get(PnfsId id)
        throws CacheException
    {
        File dataFile = _fileStore.get(id);
        if (!dataFile.isFile()) {
            return null;
        }
        try {
            File siFile = new File(_metadir, "SI-"+id.toString());
            File controlFile = new File(_metadir, id.toString());
            return new CacheRepositoryEntryImpl(id, controlFile, dataFile, siFile);
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to read meta data for " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void remove(PnfsId id)
            throws CacheException
    {
        try {
            File f = _fileStore.get(id);
            if (!f.delete() && f.exists()) {
                throw new DiskErrorCacheException("Failed to delete " + id);
            }

            File controlFile = new File(_metadir, id.toString());
            File siFile = new File(_metadir, "SI-"+id.toString());

            Files.deleteIfExists(controlFile.toPath());
            Files.deleteIfExists(siFile.toPath());
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to remove meta data for " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized boolean isOk()
    {
        if (!_fileStore.isOk()) {
            return false;
        }
        File tmp = new File(_metadir, ".repository_is_ok");
        try {
            tmp.delete();
            tmp.deleteOnExit();

            if (!tmp.createNewFile() || !tmp.exists()) {
                _log.error("Could not create " + tmp);
                return false;
            }

            return true;
        } catch (IOException e) {
            _log.error("Failed to touch " + tmp + ": " + e.getMessage(), e);
            return false;
        }
    }

    /** NOP */
    @Override
    public void close()
    {
    }

    /**
     * Returns the path
     */
    @Override
    public String toString()
    {
        return String.format("[data=%s;meta=%s]", _fileStore, _metadir);
    }

    /**
     * Provides the amount of free space on the file system containing
     * the data files.
     */
    @Override
    public long getFreeSpace()
    {
        return _fileStore.getFreeSpace();
    }

    /**
     * Provides the total amount of space on the file system
     * containing the data files.
     */
    @Override
    public long getTotalSpace()
    {
        return _fileStore.getTotalSpace();
    }
}
