package org.dcache.pool.repository.meta.file;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.pool.repository.EventProcessor;
import org.dcache.pool.repository.EventType;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.repository.v3.SiFileCorruptedException;
import org.dcache.pool.repository.v3.entry.CacheRepositoryEntryState;

import com.sun.corba.se.impl.io.OptionalDataException;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.CacheRepositoryStatistics;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.vehicles.StorageInfo;

public class CacheRepositoryEntryImpl implements CacheRepositoryEntry {


    private final CacheRepositoryEntryState _state;
    private final PnfsId _pnfsId;
    private int _linkCount = 0;
    private boolean _isLocked = false;
    private long _lockUntil = 0;
    private StorageInfo _storageInfo = null;
    private long _creationTime = System.currentTimeMillis();
    private long _lastAccess = _creationTime;
    private long _size;

    private final EventProcessor _eventProcessor;

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



    public CacheRepositoryEntryImpl(EventProcessor processor, PnfsId pnfsId, File controlFile, File dataFile, File siFile ) throws IOException, RepositoryException {

        _eventProcessor = processor;

        _pnfsId = pnfsId;
        _controlFile = controlFile;
        _siFile = siFile;
        _dataFile = dataFile;

        _state = new CacheRepositoryEntryState(_controlFile);

        try {
            _storageInfo =  readStorageInfo(siFile);
            if( _storageInfo == null ) {
                throw new SiFileCorruptedException("bad SI file for");
            }

            _creationTime = _siFile.lastModified();
        }catch(FileNotFoundException fnf) {
            /*
             * it's not an error state.
             */
        }

        _lastAccess = _dataFile.lastModified();
        _size = _dataFile.length();
    }

    private void destroy()
    {
        _controlFile.delete();
        _siFile.delete();
        CacheRepositoryEvent event =
            new CacheRepositoryEvent(_eventProcessor, clone());
        _eventProcessor.processEvent(EventType.DESTROY, event);
    }

    public void decrementLinkCount() throws CacheException {

        boolean isDead;
        synchronized (this) {
            if (_linkCount <= 0)
                throw new IllegalStateException("Link count is already  zero");
            _linkCount--;
            isDead = (_linkCount == 0 && isRemoved());
        }

        if (isDead) {
            destroy();
        }
    }

    public CacheRepositoryStatistics getCacheRepositoryStatistics() throws CacheException {
        // TODO Auto-generated method stub
        return null;
    }

    public long getCreationTime() throws CacheException {
        return _creationTime;
    }

    public File getDataFile() throws CacheException {
        return _dataFile;
    }

    public long getLastAccessTime() throws CacheException {
        return _lastAccess;
    }

    public synchronized int getLinkCount() throws CacheException {
        return _linkCount;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public void setSize(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative entry size is not allowed");
        }
        _size = size;
    }

    public long getSize() {
        return _size;
    }

    public String getState() {
        return _state.toString();
    }

    public StorageInfo getStorageInfo() throws CacheException {
        return _storageInfo;
    }

    public synchronized void incrementLinkCount() throws CacheException {
        _linkCount++;
    }

    public boolean isBad() {
        return _state.isError();
    }

    public boolean isCached() throws CacheException {
        return _state.isCached();
    }

    public boolean isRemoved() throws CacheException {
        return _state.isRemoved();
    }

    public boolean isDestroyed() throws CacheException {
        // TODO Auto-generated method stub
        return false;
    }

    public synchronized boolean isLocked() {
        return _isLocked || _lockUntil > System.currentTimeMillis();
    }

    public boolean isPrecious() throws CacheException {
        return _state.isPrecious();
    }

    public boolean isReceivingFromClient() throws CacheException {
        return _state.isRecivingFromClient();
    }

    public boolean isReceivingFromStore() throws CacheException {
        return _state.isRecivingFromStore();
    }

    public boolean isSendingToStore() throws CacheException {
        return _state.isSendingToStore();
    }

    public boolean isSticky() throws CacheException {
        return _state.isSticky();
    }

    public synchronized void lock(boolean locked) {
        _isLocked = locked;
    }

    public synchronized void lock(long millisSeconds) {

        long now = System.currentTimeMillis();

        if( now + millisSeconds > _lockUntil ) {
            _lockUntil = now + millisSeconds;
        }

    }

    public void setBad(boolean bad) {

        // TODO: catch and throw Cache exception
        try {
            if( bad ) {
                _state.setError();
            }else{
                _state.cleanBad();
            }
        } catch (IllegalStateException e) {
            // ignored
        } catch (IOException e) {
            // never thrown
        }
    }

    public void setCached() throws CacheException {
        try {

            if( _state.isRecivingFromClient() || _state.isRecivingFromStore() ) {
                CacheRepositoryEvent availableEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
                _eventProcessor.processEvent(EventType.AVAILABLE, availableEvent);
            }

            _state.setCached();

            CacheRepositoryEvent cachedEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
            _eventProcessor.processEvent(EventType.CACHED, cachedEvent);


        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setPrecious() throws CacheException {
        try {


            if( _state.isRecivingFromClient() ) {
                CacheRepositoryEvent availableEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
                _eventProcessor.processEvent(EventType.AVAILABLE, availableEvent);
            }

            _state.setPrecious();

            CacheRepositoryEvent preciousEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
            _eventProcessor.processEvent(EventType.PRECIOUS, preciousEvent);

        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setReceivingFromClient() throws CacheException {
        try {
            _state.setFromClinet();

            CacheRepositoryEvent createEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
            _eventProcessor.processEvent(EventType.CREATE, createEvent);

        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setReceivingFromStore() throws CacheException {
        try {
            _state.setFromStore();

            CacheRepositoryEvent createEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
            _eventProcessor.processEvent(EventType.CREATE, createEvent);

        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setSendingToStore(boolean sending) throws CacheException {
        try {
            if( sending ) {
                _state.setToStore();
            }else{
                _state.cleanToStore();
            }
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void removeExpiredStickyFlags()
    {
        if (_state.removeExpiredStickyFlags() && !_state.isSticky()) {
            CacheRepositoryEvent event =
                new CacheRepositoryEvent(_eventProcessor, clone());
            _eventProcessor.processEvent(EventType.STICKY, event);
        }
    }

    public void setSticky(boolean sticky) throws CacheException {
        try {
            if( sticky ) {
                _state.setSticky("system", -1, false);
            }else{
                _state.setSticky("system", 0, true);
            }

            CacheRepositoryEvent sickyEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
            _eventProcessor.processEvent(EventType.STICKY, sickyEvent);

        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setSticky(String owner, long expire, boolean overwrite) throws CacheException {
        try {
            _state.setSticky(owner, expire, overwrite);

            CacheRepositoryEvent sickyEvent = new CacheRepositoryEvent(_eventProcessor, clone() );
            _eventProcessor.processEvent(EventType.STICKY, sickyEvent);

        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        } catch (IOException e) {
            throw new CacheException(e.getMessage());
        }
    }
    public void setStorageInfo(StorageInfo storageInfo) throws CacheException {

        ObjectOutputStream objectOut = null;
        File siFileTemp = null;
        try {

            siFileTemp = File.createTempFile(_siFile.getName(), null, _siFile.getParentFile() );

            objectOut = new ObjectOutputStream( new FileOutputStream( siFileTemp) );

            objectOut.writeObject( storageInfo ) ;


        } catch (IOException ioe) {
            // TODO: disk io error code here
            if( siFileTemp != null && siFileTemp.exists() ){
                siFileTemp.delete();
            }
            throw new CacheException(10,_pnfsId+" "+ioe.getMessage() );
        }finally {
            if( objectOut != null ) {
                try {
                    objectOut.close();
                } catch (IOException ignore) {
                }
            }
        }

        if( ! siFileTemp.renameTo(_siFile) ) {
            // TODO: disk io error code here
            throw new CacheException(10,_pnfsId+" rename failed" );
        }

        _storageInfo = storageInfo;
    }

    public void setRemoved() throws CacheException {
        try {
            boolean isDead;
            synchronized (this) {
                _state.setRemoved();
                isDead = (getLinkCount() == 0);
            }

            CacheRepositoryEvent createEvent =
                new CacheRepositoryEvent(_eventProcessor, clone());
            _eventProcessor.processEvent(EventType.REMOVE, createEvent);

            if (isDead) {
                destroy();
            }
        }catch(IOException ignored) {
            // if's only a mask, exception is never thrown
        }

    }

    public void touch() throws CacheException {

        try{
            if( ! _dataFile.exists() )_dataFile.createNewFile() ;
        }catch(IOException ee){
            throw new
                CacheException("Io Error creating : "+_dataFile ) ;
        }

        _lastAccess = System.currentTimeMillis();
        _dataFile.setLastModified(_lastAccess);

        CacheRepositoryEvent event =
            new CacheRepositoryEvent(_eventProcessor, clone());
        _eventProcessor.processEvent(EventType.TOUCH, event);
    }


    private static StorageInfo readStorageInfo(File objIn) throws IOException {

        ObjectInputStream in = null;
        StorageInfo storageInfo = null;

        try {

            in = new ObjectInputStream(
                                       new BufferedInputStream(new FileInputStream(objIn))
                                       );
            storageInfo = (StorageInfo) in.readObject();

        } catch (ClassNotFoundException cnf) {

        } catch (InvalidClassException ife) {
            // valid exception if siFIle is broken
        } catch( StreamCorruptedException sce ) {
            // valid exception if siFIle is broken
        } catch (OptionalDataException ode) {
            // valid exception if siFIle is broken
        } catch (EOFException eof){
            // object file size mismatch
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException we) {
                    // close on read can be ignored
                }
            }
        }

        return storageInfo;
    }

    public List<StickyRecord> stickyRecords() {
        return _state.stickyRecords();
    }


    // TODO; return a copy
    public CacheRepositoryEntry clone() {
        return this;
    }

    public synchronized String toString(){

        return _pnfsId.toString()+
            " <"+_state.toString()+(_isLocked ? "L":"-")+
            "(" + _lockUntil +")"+
            "["+_linkCount+"]> "+
            getSize()+
            " si={"+(_storageInfo==null?"<unknown>":_storageInfo.getStorageClass())+"}" ;
    }

}
