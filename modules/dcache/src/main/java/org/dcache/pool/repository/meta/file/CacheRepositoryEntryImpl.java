package org.dcache.pool.repository.meta.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryState;
import org.dcache.vehicles.FileAttributes;

public class CacheRepositoryEntryImpl implements MetaDataRecord, MetaDataRecord.UpdatableRecord
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
    private final File _controlFile;

    /**
     * serialized storage info file
     */
    private final File _siFile;

    /**
     * data file
     */
    private final File _dataFile;



    public CacheRepositoryEntryImpl(PnfsId pnfsId, File controlFile, File dataFile, File siFile ) throws IOException
    {

        _pnfsId = pnfsId;
        _controlFile = controlFile;
        _siFile = siFile;
        _dataFile = dataFile;

        _state = new CacheRepositoryEntryState(_controlFile);

        try {
            _storageInfo = readStorageInfo(siFile);
            _creationTime = _siFile.lastModified();
        }catch(FileNotFoundException fnf) {
            /*
             * it's not an error state.
             */
        }

        _lastAccess = _dataFile.lastModified();
        _size = _dataFile.length();

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
    public synchronized File getDataFile()
    {
        return _dataFile;
    }

    @Override
    public RepositoryChannel openChannel(IoMode mode) throws IOException
    {
        return new FileRepositoryChannel(getDataFile(), mode.toOpenString());
    }

    @Override
    public synchronized long getLastAccessTime() {
        return _lastAccess;
    }

    @Override
    public synchronized void setLastAccessTime(long time) throws CacheException
    {
        if (!_dataFile.setLastModified(time)) {
            throw new DiskErrorCacheException("Failed to set modification time: " + _dataFile);
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
        return _state.getState().isMutable() ? getDataFile().length() : _size;
    }

    @Override
    public synchronized boolean isSticky() {
        return _state.isSticky();
    }

    @Override
    public synchronized EntryState getState()
    {
        return _state.getState();
    }

    @Override
    public synchronized Void setState(EntryState state)
        throws CacheException
    {
        try {
            if (_state.getState().isMutable() && !state.isMutable()) {
                _size = getDataFile().length();
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
        File siFileTemp = File.createTempFile(_siFile.getName(), null, _siFile.getParentFile());
        try {
            try (ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(siFileTemp))) {
                objectOut.writeObject(storageInfo);
            }
            Files.move(siFileTemp.toPath(), _siFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(siFileTemp.toPath());
        }
        _storageInfo = storageInfo;
    }

    private static StorageInfo readStorageInfo(File objIn) throws IOException {
        try {
            try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(objIn)))) {
                return (StorageInfo) in.readObject();
            }
        } catch (Throwable t) {
            LOGGER.debug("Failed to read {}: {}", objIn.getPath(), t.toString());
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
