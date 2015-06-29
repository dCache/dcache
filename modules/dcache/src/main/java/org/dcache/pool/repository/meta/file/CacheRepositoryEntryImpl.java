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
import java.util.List;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryState;
import org.dcache.vehicles.FileAttributes;

public class CacheRepositoryEntryImpl implements MetaDataRecord
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

    /**
     * Copy from existing MetaDataRecord.
     */
    public CacheRepositoryEntryImpl(PnfsId pnfsId, File controlFile,
                                    File dataFile,  File siFile,
                                    MetaDataRecord entry)
        throws IOException, RepositoryException, CacheException
    {
        _pnfsId = pnfsId;
        _controlFile = controlFile;
        _siFile = siFile;
        _dataFile = dataFile;
        _lastAccess   = entry.getLastAccessTime();
        _linkCount    = entry.getLinkCount();
        _creationTime = entry.getCreationTime();
        _size         = entry.getSize();
        _state        = new CacheRepositoryEntryState(_controlFile, entry);
        setFileAttributes(entry.getFileAttributes());
    }

    @Override
    public synchronized void incrementLinkCount()
    {
        _linkCount++;
    }

    @Override
    public synchronized void decrementLinkCount()
    {

        if (_linkCount <= 0) {
            throw new IllegalStateException("Link count is already  zero");
        }
        _linkCount--;
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
    public synchronized long getLastAccessTime() {
        return _lastAccess;
    }

    @Override
    public synchronized PnfsId getPnfsId() {
        return _pnfsId;
    }

    @Override
    public synchronized void setSize(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative entry size is not allowed");
        }
        _size = size;
    }

    @Override
    public synchronized long getSize() {
        return _size;
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
    public synchronized void setState(EntryState state)
        throws CacheException
    {
        try {
            _state.setState(state);
        } catch (IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
    }

    @Override
    public synchronized List<StickyRecord> removeExpiredStickyFlags() throws CacheException
    {
        try {
            return _state.removeExpiredStickyFlags();
        } catch (IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
    }

    @Override
    public synchronized boolean setSticky(String owner, long expire, boolean overwrite) throws CacheException {
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
    public void setFileAttributes(FileAttributes attributes) throws CacheException {
        try {
            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                setStorageInfo(StorageInfos.extractFrom(attributes));
            } else {
                setStorageInfo(null);
            }
        } catch (IOException e) {
            throw new DiskErrorCacheException(_pnfsId + " " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void touch() throws CacheException {

        try{
            if( ! _dataFile.exists() ) {
                _dataFile.createNewFile();
            }
        }catch(IOException ee){
            throw new DiskErrorCacheException("Io Error creating : "+_dataFile ,ee);
        }

        _lastAccess = System.currentTimeMillis();
        _dataFile.setLastModified(_lastAccess);
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
    public synchronized List<StickyRecord> stickyRecords() {
        return _state.stickyRecords();
    }

    public synchronized String toString()
    {
        StorageInfo si = getStorageInfo();
        return _pnfsId.toString()+
            " <"+_state.toString()+"-"+
            "(0)"+
            "["+_linkCount+"]> "+
            getSize()+
            " si={"+(si==null?"<unknown>":si.getStorageClass())+"}" ;
    }
}
