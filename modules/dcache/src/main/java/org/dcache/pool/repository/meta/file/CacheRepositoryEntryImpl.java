package org.dcache.pool.repository.meta.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Collection;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryState;
import org.dcache.vehicles.FileAttributes;

public class CacheRepositoryEntryImpl implements ReplicaRecord, ReplicaRecord.UpdatableRecord
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheRepositoryEntryImpl.class);
    private final CacheRepositoryEntryState _state;
    private final PnfsId _pnfsId;
    private int _linkCount;
    private StorageInfo _storageInfo;
    private long _creationTime = System.currentTimeMillis();
    private long _lastAccess;
    private long _size;

    /**
     * control file
     */
    private final Path _controlFile;

    /**
     * serialized storage info file
     */
    private final Path _siFile;

    /**
     * File store used for data file.
     */
    private final FileStore _fileStore;



    public CacheRepositoryEntryImpl(PnfsId pnfsId, Path controlFile, FileStore fileStore, Path siFile) throws IOException
    {

        _pnfsId = pnfsId;
        _controlFile = controlFile;
        _siFile = siFile;
        _fileStore = fileStore;

        _state = new CacheRepositoryEntryState(_controlFile);

        try {
            _storageInfo = readStorageInfo(siFile);
            _creationTime = Files.getLastModifiedTime(_siFile).toMillis();
        } catch (FileNotFoundException | NoSuchFileException fnf) {
            /*
             * it's not an error state.
             */
        }

        try {
            BasicFileAttributes attributes = _fileStore
                    .getFileAttributeView(pnfsId)
                    .readAttributes();
            _lastAccess = attributes.lastModifiedTime().toMillis();
            _size = attributes.size();
        } catch (FileNotFoundException | NoSuchFileException fnf) {
            /*
             * it's not an error state.
             */
        }

        if (_lastAccess == 0) {
            _lastAccess = _creationTime;
        }
    }

    @Override
    public synchronized int incrementLinkCount()
    {
        _linkCount++;
        return _linkCount;
    }

    @Override
    public synchronized int decrementLinkCount()
    {

        if (_linkCount <= 0) {
            throw new IllegalStateException("Link count is already  zero");
        }
        _linkCount--;
        return _linkCount;
    }

    @Override
    public synchronized int getLinkCount() {
        return _linkCount;
    }

    @Override
    public synchronized long getCreationTime() {
        return _creationTime;
    }

    @Override
    public synchronized URI getReplicaUri()
    {
        return _fileStore.get(_pnfsId);
    }

    @Override
    public RepositoryChannel openChannel(IoMode mode) throws IOException
    {
        return _fileStore.openDataChannel(_pnfsId, mode);
    }

    @Override
    public synchronized long getLastAccessTime() {
        return _lastAccess;
    }

    @Override
    public synchronized void setLastAccessTime(long time) throws CacheException
    {
        try {
            _fileStore
                    .getFileAttributeView(_pnfsId)
                    .setTimes(FileTime.fromMillis(time), null, null);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to set modification time: " + _pnfsId, e);
        }
        _lastAccess = System.currentTimeMillis();
    }

    @Override
    public synchronized PnfsId getPnfsId() {
        return _pnfsId;
    }

    @Override
    public synchronized long getReplicaSize()
    {
        try {
            return _state.getState().isMutable() ?
                    _fileStore.getFileAttributeView(_pnfsId).readAttributes().size()
                    : _size;
        } catch (NoSuchFileException e) {
            return 0;
        } catch (IOException e) {
            LOGGER.error("Failed to read file size: " + e);
            return 0;
        }
    }

    @Override
    public synchronized boolean isSticky() {
        return _state.isSticky();
    }

    @Override
    public synchronized ReplicaState getState()
    {
        return _state.getState();
    }

    @Override
    public synchronized Void setState(ReplicaState state)
        throws CacheException
    {
        try {
            if (_state.getState().isMutable() && !state.isMutable()) {
                try {
                    _size = _fileStore.getFileAttributeView(_pnfsId).readAttributes().size();
                } catch (NoSuchFileException e) {
                    _size = 0;
                }
            }
            _state.setState(state);
        } catch (IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public synchronized Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
    {
        try {
            return _state.removeExpiredStickyFlags();
        } catch (IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
    }

    @Override
    public boolean setSticky(String owner, long expire, boolean overwrite) throws CacheException {
        try {
            if (_state.setSticky(owner, expire, overwrite)) {
                return true;
            }
            return false;

        } catch (IllegalStateException | IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
    }

    @Override
    public synchronized FileAttributes getFileAttributes() {
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(_pnfsId);
        StorageInfo storageInfo = getStorageInfo();
        if (storageInfo != null) {
            StorageInfos.injectInto(storageInfo, attributes);
        }
        return attributes;
    }

    @Override
    public Void setFileAttributes(FileAttributes attributes) throws CacheException {
        try {
            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                setStorageInfo(StorageInfos.extractFrom(attributes));
            } else {
                setStorageInfo(null);
            }
        } catch (IOException e) {
            throw new DiskErrorCacheException(_pnfsId + " " + e.getMessage(), e);
        }
        return null;
    }

    private synchronized StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    private synchronized void setStorageInfo(StorageInfo storageInfo) throws IOException
    {
        Path siFileTemp = Files.createTempFile(_siFile.getParent(), _siFile.getFileName().toString(), null);
        try {
            try (ObjectOutputStream objectOut = new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(siFileTemp)))) {
                objectOut.writeObject(storageInfo);
            }
            Files.move(siFileTemp, _siFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(siFileTemp);
        }
        _storageInfo = storageInfo;
    }

    private static StorageInfo readStorageInfo(Path objIn) throws IOException {
        try {
            try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(objIn)))) {
                return (StorageInfo) in.readObject();
            }
        } catch (Throwable t) {
            LOGGER.debug("Failed to read {}: {}", objIn, t.toString());
        }
        return null;
    }

    @Override
    public synchronized Collection<StickyRecord> stickyRecords() {
        return _state.stickyRecords();
    }

    @Override
    public synchronized <T> T update(Update<T> update) throws CacheException
    {
        return update.apply(this);
    }
}
