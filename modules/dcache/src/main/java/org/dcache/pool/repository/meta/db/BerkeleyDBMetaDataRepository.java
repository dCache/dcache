package org.dcache.pool.repository.meta.db;

import com.google.common.base.Stopwatch;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.RepositoryChannel;

import static java.util.Arrays.asList;
import static org.dcache.util.Exceptions.messageOrClassName;

/**
 * BerkeleyDB based MetaDataRepository implementation.
 *
 * Uses a FileStore implementatino as the backing store for the data files.
 */
public class BerkeleyDBMetaDataRepository extends AbstractBerkeleyDBReplicaStore
{
    private static final String REMOVING_REDUNDANT_META_DATA =
            "Removing redundant meta data for %s.";

    /**
     * The file store for which we hold the meta data.
     */
    private final FileStore _fileStore;

    /**
     * Opens a BerkeleyDB based meta data repository. If the database
     * does not exist yet, then it is created. If the 'meta' directory
     * does not exist, it is created.
     */
    public BerkeleyDBMetaDataRepository(FileStore fileStore, Path directory)
            throws IOException, DatabaseException, CacheException
    {
        this(fileStore, directory, false);
    }

    public BerkeleyDBMetaDataRepository(FileStore fileStore, Path directory, boolean readOnly)
            throws IOException, DatabaseException
    {
        super(directory, readOnly);
        _fileStore = fileStore;
    }

    @Override
    public Set<PnfsId> index(IndexOption... options) throws CacheException
    {
        try {
            List<IndexOption> indexOptions = asList(options);

            if (indexOptions.contains(IndexOption.META_ONLY)) {
                return views.collectKeys(Collectors.mapping(PnfsId::new, Collectors.toSet()));
            }

            Stopwatch watch = Stopwatch.createStarted();
            Set<PnfsId> files = _fileStore.index();
            LOGGER.info("Indexed {} entries in {} in {}.", files.size(), _fileStore, watch);

            watch.reset().start();
            Set<String> records = views.collectKeys(Collectors.toSet());
            LOGGER.info("Indexed {} entries in {} in {}.", records.size(), dir, watch);

            if (indexOptions.contains(IndexOption.ALLOW_REPAIR)) {
                for (String id : records) {
                    if (!files.contains(new PnfsId(id))) {
                        LOGGER.warn(String.format(REMOVING_REDUNDANT_META_DATA, id));
                        views.getStorageInfoMap().remove(id);
                        views.getStateMap().remove(id);
                    }
                }
            }

            return files;
        } catch (EnvironmentFailureException e) {
            if (!isValid()) {
                throw new DiskErrorCacheException("Meta data lookup failed and a pool restart is required: " + e.getMessage(), e);
            }
            throw new CacheException("Meta data lookup failed: " + e.getMessage(), e);
        } catch (OperationFailureException e) {
            throw new CacheException("Meta data lookup failed: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Meta data lookup failed and a pool restart is required: " + messageOrClassName(e), e);
        }
    }

    @Override
    public ReplicaRecord get(PnfsId id) throws CacheException
    {
        try {

            BasicFileAttributes attributes = _fileStore.getFileAttributeView(id).readAttributes();
            if (!attributes.isRegularFile()) {
                throw new DiskErrorCacheException("Not a regular file: " + _fileStore.get(id));
            }
            return CacheRepositoryEntryImpl.load(this, id, attributes);
        } catch (EnvironmentFailureException e) {
            if (!isValid()) {
                throw new DiskErrorCacheException("Meta data lookup failed and a pool restart is required: " + e.getMessage(), e);
            }
            throw new CacheException("Meta data lookup failed: " + e.getMessage(), e);
        } catch (OperationFailureException e) {
            throw new CacheException("Meta data lookup failed: " + e.getMessage(), e);
        } catch (NoSuchFileException | FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            throw new CacheException("Failed to read " + id + ": " + messageOrClassName(e), e);
        }
    }

    /**
     * TODO: The entry is not persistent yet!
     */
    @Override
    public ReplicaRecord create(PnfsId id, Set<Repository.OpenFlags> flags)
            throws CacheException
    {
        try {

            if (_fileStore.contains(id)) {
                throw new DuplicateEntryException(id);
            }
            views.getStorageInfoMap().remove(id.toString());
            views.getStateMap().remove(id.toString());
            if (flags.contains(Repository.OpenFlags.CREATEFILE)) {
                _fileStore.create(id);
            }
            return new CacheRepositoryEntryImpl(this, id);
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to create new entry " + id + ": " + messageOrClassName(e), e);
        }
    }

    @Override
    public void remove(PnfsId id) throws CacheException
    {

        try {
            _fileStore.remove(id);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to delete " + id + ": " + messageOrClassName(e), e);
        }
        try {
            views.getStorageInfoMap().remove(id.toString());
            views.getStateMap().remove(id.toString());
        } catch (EnvironmentFailureException e) {
            if (!isValid()) {
                throw new DiskErrorCacheException("Meta data update failed and a pool restart is required: " + e.getMessage(), e);
            }
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        } catch (OperationFailureException e) {
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isOk()
    {
        return _fileStore.isOk() && super.isOk();

    }

    /**
     * Returns the path
     */
    @Override
    public String toString()
    {
        return String.format("[data=%s;meta=%s]", _fileStore, dir);
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
            LOGGER.warn("Failed to query free space: " + e.toString());
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
            LOGGER.warn("Failed to query total space: " + e.toString());
            return 0;
        }
    }

    @Override
    public void setLastModifiedTime(PnfsId pnfsId, long time) throws IOException
    {
        _fileStore.getFileAttributeView(pnfsId)
                .setTimes(FileTime.fromMillis(time), null, null);
    }

    @Override
    public long getFileSize(PnfsId pnfsId) throws IOException
    {
        try {
            return _fileStore
                    .getFileAttributeView(pnfsId)
                    .readAttributes()
                    .size();
        } catch (NoSuchFileException e) {
            return 0;
        }
    }

    @Override
    public URI getUri(PnfsId pnfsId)
    {
        return _fileStore.get(pnfsId);
    }

    @Override
    public RepositoryChannel openChannel(PnfsId pnfsId, IoMode mode) throws IOException
    {
        return _fileStore.openDataChannel(pnfsId, mode);
    }
}
