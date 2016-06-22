package org.dcache.pool.repository.meta.file;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.v3.RepositoryException;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
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
    private final Path _metadir;

    public FileMetaDataRepository(FileStore fileStore, Path baseDir)
            throws IOException
    {
        this(fileStore, baseDir, false);
    }

    public FileMetaDataRepository(FileStore fileStore, Path baseDir, boolean readOnly)
            throws IOException
    {
    	_fileStore = fileStore;
        _metadir = baseDir.resolve(DIRECTORY_NAME);
        if (!Files.exists(_metadir)) {
            if (readOnly) {
                throw new FileNotFoundException("No such directory and not allowed to create it: " + _metadir);
            }
            Files.createDirectory(_metadir);
        } else if (!Files.isDirectory(_metadir)) {
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
        try {
            List<IndexOption> indexOptions = asList(options);

            if (indexOptions.contains(IndexOption.META_ONLY)) {
                try (Stream<Path> list = Files.list(_metadir)) {
                    return list
                            .map(path -> path.getFileName().toString())
                            .map(name -> name.startsWith("SI-") ? name.substring(3) : name)
                            .filter(PnfsId::isValid)
                            .map(PnfsId::new)
                            .collect(toSet());
                }
            }

            Stopwatch watch = Stopwatch.createStarted();
            Set<PnfsId> files = _fileStore.index();
            _log.info("Indexed {} entries in {} in {}.", files.size(), _fileStore, watch);

            if (indexOptions.contains(IndexOption.ALLOW_REPAIR)) {
                watch.reset().start();
                List<Path> metaFilesToBeDeleted;
                try (Stream<Path> list = Files.list(_metadir)) {
                    metaFilesToBeDeleted = list
                            .filter(path -> {
                                String name = path.getFileName().toString();
                                String s = name.startsWith("SI-") ? name.substring(3) : name;
                                return PnfsId.isValid(s) && !files.contains(new PnfsId(s));
                            })
                            .collect(toList());
                }
                _log.info("Found {} orphaned meta data entries in {} in {}.", metaFilesToBeDeleted.size(), _metadir, watch);

                for (Path name : metaFilesToBeDeleted) {
                    deleteIfExists(name);
                }
            }
            return files;
        } catch (IOException e) {
            throw new DiskErrorCacheException("Meta data lookup failed and a pool restart is required: " + e.getMessage(), e);
        }
    }

    @Override
    public MetaDataRecord create(PnfsId id, Set<Repository.OpenFlags> flags)
        throws DuplicateEntryException, CacheException
    {
        try {
            Path controlFile = _metadir.resolve(id.toString());
            Path siFile = _metadir.resolve("SI-" + id.toString());
            Path dataFile = _fileStore.get(id);

            if (Files.exists(dataFile)) {
                throw new DuplicateEntryException(id);
            }

            /* In case of left over or corrupted files, we delete them
             * before creating a new entry.
             */
            Files.deleteIfExists(controlFile);
            Files.deleteIfExists(siFile);

            if (flags.contains(Repository.OpenFlags.CREATEFILE)) {
                Files.createFile(dataFile);
            }

            return new CacheRepositoryEntryImpl(id, controlFile, dataFile, siFile);
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to create new entry " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public MetaDataRecord get(PnfsId id)
        throws CacheException
    {
        Path dataFile = _fileStore.get(id);
        if (!Files.isRegularFile(dataFile)) {
            return null;
        }
        try {
            Path siFile = _metadir.resolve("SI-" + id.toString());
            Path controlFile = _metadir.resolve(id.toString());
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
        deleteIfExists(_fileStore.get(id));
        deleteIfExists(_metadir.resolve(id.toString()));
        deleteIfExists(_metadir.resolve("SI-" + id.toString()));
    }

    protected void deleteIfExists(Path path) throws DiskErrorCacheException
    {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to remove " + path + ": " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized boolean isOk()
    {
        if (!_fileStore.isOk()) {
            return false;
        }
        Path tmp = _metadir.resolve(".repository_is_ok");
        try {
            Files.deleteIfExists(tmp);
            Files.createFile(tmp);
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
        try {
            return _fileStore.getFreeSpace();
        } catch (IOException e) {
            _log.warn("Failed to query free space: " + e.toString());
            return 0;
        }
    }

    /**
     * Provides the total amount of space on the file system
     * containing the data files.
     */
    @Override
    public long getTotalSpace()
    {
        try {
            return _fileStore.getTotalSpace();
        } catch (IOException e) {
            _log.warn("Failed to query total space: " + e.toString());
            return 0;
        }
    }
}
