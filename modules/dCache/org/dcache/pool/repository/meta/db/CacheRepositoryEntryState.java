package org.dcache.pool.repository.meta.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.io.Serializable;
import java.io.ObjectInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.dcache.pool.repository.StickyRecord;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;

/**
 * The CacheRepositoryEntryState encapsulates state information about
 * a file.
 */
public class CacheRepositoryEntryState implements Serializable
{
    static final long serialVersionUID = -715461991190516015L;

    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    private final Set<StickyRecord> _sticky = new HashSet<StickyRecord>(0);
    private boolean _precious    = false;
    private boolean _cached      = false;
    private boolean _toStore     = false;
    private boolean _toClient    = false;
    private boolean _fromClient  = false;
    private boolean _fromStore   = false;
    private boolean _error       = false;
    private boolean _removed     = false;

    /**
     * When true, the state needs to be written to permanent storage.
     */
    private transient boolean _dirty = false;

    public CacheRepositoryEntryState()
    {
    }

    public CacheRepositoryEntryState(CacheRepositoryEntry entry)
        throws CacheException
    {
        _precious   = entry.isPrecious();
        _cached     = entry.isCached();
        _toStore    = entry.isSendingToStore();
        _fromClient = entry.isReceivingFromClient();
        _fromStore  = entry.isReceivingFromStore();
        _error      = entry.isBad();
        _removed    = entry.isRemoved();
        for (StickyRecord r : entry.stickyRecords()) {
            _sticky.add(new StickyRecord(r.owner(), r.expire()));
        }
        _dirty      = true;
    }

    /**
     * Returns true if the state is dirty, i.e., needs to be made
     * persistent. Clears the dirty flag. A second call to dirty()
     * will always return false, unless the state was made dirty
     * inbetween the two calls.
     */
    public synchronized boolean dirty()
    {
        if (_dirty) {
            _dirty = false;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Marks the state as dirty, i.e., it needs to be made persistent.
     */
    private synchronized void markDirty()
    {
        _dirty = true;
    }

    /**
     * file is busy if there is a transfer in progress
     * @return
     */
    public synchronized boolean isBusy()
    {
        return _toStore || _toClient || _fromClient || _fromStore;
    }

    /*
     * State getters
     */
    public synchronized boolean isError()
    {
        return _error;
    }

    public synchronized boolean isCached()
    {
        return _cached;
    }

    public synchronized boolean isPrecious()
    {
        return _precious;
    }

    /**
     *
     * @return true if file ready for clients (CACHED or PRECIOUS)
     */
    public synchronized boolean isReady()
    {
        return _precious | _cached;
    }

    public synchronized boolean isReceivingFromClient()
    {
        return _fromClient;
    }

    public synchronized boolean isReceivingFromStore()
    {
        return _fromStore;
    }

    public synchronized boolean isSendingToStore()
    {
        return _toStore;
    }

    public synchronized boolean isSticky()
    {
        long now = System.currentTimeMillis();
        for (StickyRecord record : _sticky) {
            if (record.isValidAt(now)) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean isRemoved()
    {
        return _removed;
    }

    public synchronized List<StickyRecord> stickyRecords()
    {
        return new ArrayList<StickyRecord>(_sticky);
    }

    public synchronized List<StickyRecord> removeExpiredStickyFlags()
    {
        List<StickyRecord> removed = new ArrayList();
        long now = System.currentTimeMillis();
        Iterator<StickyRecord> i = _sticky.iterator();
        while (i.hasNext()) {
            StickyRecord record = i.next();
            if (!record.isValidAt(now)) {
                i.remove();
                removed.add(record);
                markDirty();
            }
        }
        return removed;
    }

    /*
     *
     *  State transitions
     *
     */
    public synchronized boolean setSticky(String owner, long expire, boolean overwrite)
        throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        if (cleanSticky(owner, overwrite ? -1 : expire)) {
            if (expire == -1 || expire > System.currentTimeMillis()) {
                _sticky.add(new StickyRecord(owner, expire));
                markDirty();
            }
            return true;
        }
        return false;
    }

    /**
     * Removes all sticky flags owned by <code>owner</code> and not
     * valid at <code>time</code>. No flag is valid at time point -1.
     *
     * Returns true if all flags owned by <code>owner</code> have been
     * removed, false otherwise.
     */
    private synchronized boolean cleanSticky(String owner, long time)
        throws IllegalStateException
    {
        Iterator<StickyRecord> i = _sticky.iterator();
        while (i.hasNext()) {
            StickyRecord record = i.next();
            if (record.owner().equals(owner)) {
                if ((time > -1) && record.isValidAt(time))
                    return false;
                i.remove();
                markDirty();
            }
        }
        return true;
    }

    public synchronized void cleanBad()
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        _error = false;
        markDirty();
    }

    public synchronized void setPrecious()
        throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        // precious file can't be in receiving state
        _fromClient = false;
        _fromStore = false;
        _precious = true;
        _cached = false;

        markDirty();
    }

    public synchronized void setCached() throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        // cached file can't be in receiving state
        _fromClient = false;
        _fromStore = false;
        _cached = true;
        _precious = false;

        markDirty();
    }

    public synchronized void setFromClient() throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        // only 'clean' file allowed to be received
        if (_precious || _cached || _fromStore) {
            throw new IllegalStateException("File still transient");
        }

        _fromClient = true;
        markDirty();
    }

    public synchronized void setFromStore() throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        // only 'clean' file allowed to be received
        if (_precious || _cached || _fromClient) {
            throw new IllegalStateException("File still transient");
        }

        _fromStore = true;
        markDirty();
    }

    public synchronized void setToClient() throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        // only received files can be delivered to a client
        if (!(_precious || _cached)) {
            throw new IllegalStateException("File still transient");
        }

        _toClient = true;
        markDirty();
    }

    public synchronized void setToStore() throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        // only received precious files can be flushed to store
        if (!_precious) {
            throw new IllegalStateException("File still transient");
        }

        _toStore = true;
        markDirty();
    }

    public synchronized void cleanToStore() throws IllegalStateException
    {
        // too late
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        _toStore = false;
        markDirty();
    }

    public synchronized void setError() throws IllegalStateException
    {
        if (_removed) {
            throw new IllegalStateException("Entry in removed state");
        }

        _error = true;
        markDirty();
    }

    public synchronized void setRemoved() throws IllegalStateException
    {
        _removed = true;
        markDirty();
    }

    public synchronized String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(_cached && !_precious ? "C" : "-" );
        sb.append(_precious             ? "P" : "-" );
        sb.append(_fromClient           ? "C" : "-" );
        sb.append(_fromStore            ? "S" : "-" );
        sb.append(_toClient             ? "c" : "-" );
        sb.append(_toStore              ? "s" : "-" );
        sb.append(_removed              ? "R" : "-" ); // REMOVED
        sb.append(                              "-" ); // DESTROYED
        sb.append(isSticky()            ? "X" : "-" );
        sb.append(_error                ? "E" : "-" );
        return sb.toString();
    }

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        _dirty = false;
    }
}
