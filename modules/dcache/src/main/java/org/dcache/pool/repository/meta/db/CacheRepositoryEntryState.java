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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.MetaDataRecord;

/**
 * The CacheRepositoryEntryState encapsulates state information about
 * a file.
 */
public class CacheRepositoryEntryState implements Serializable
{
    private static final long serialVersionUID = -715461991190516015L;

    private static Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    private final Set<StickyRecord> _sticky = new HashSet<>(0);
    private boolean _precious;
    private boolean _cached;
    private boolean _toStore;
    private boolean _toClient;
    private boolean _fromClient;
    private boolean _fromStore;
    private boolean _error;
    private boolean _removed;

    /**
     * When true, the state needs to be written to permanent storage.
     */
    private transient boolean _dirty;

    /**
     * This is the state exposed to the pool. The other bits are
     * simply a legacy representation of this state.
     */
    private transient EntryState _state;

    public CacheRepositoryEntryState()
    {
        _state = EntryState.NEW;
    }

    public CacheRepositoryEntryState(MetaDataRecord entry)
    {
        setState(entry.getState());
        for (StickyRecord r : entry.stickyRecords()) {
            _sticky.add(new StickyRecord(r.owner(), r.expire()));
        }
        _dirty      = true;
    }

    public EntryState getState()
    {
        return _state;
    }

    public void setState(EntryState state)
    {
        if (state == _state) {
            return;
        }

        switch (state) {
        case NEW:
            throw new IllegalStateException("Entry is " + _state);
        case FROM_CLIENT:
            if (_state != EntryState.NEW) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _precious = _cached = _fromStore = _error = _removed = false;
            _fromClient = true;
            break;
        case FROM_STORE:
            if (_state != EntryState.NEW) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _precious = _cached = _fromClient = _error = _removed = false;
            _fromStore = true;
            break;
        case FROM_POOL:
            if (_state != EntryState.NEW) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _precious = _cached = _fromClient = _error = _removed = false;
            _fromStore = true;
            break;
        case CACHED:
            if (_state == EntryState.REMOVED ||
                _state == EntryState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _precious = _fromClient = _fromStore = _error = _removed = false;
            _cached = true;
            break;
        case PRECIOUS:
            if (_state == EntryState.REMOVED ||
                _state == EntryState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _cached = _fromClient = _fromStore = _error = _removed = false;
            _precious = true;
            break;
        case BROKEN:
            if (_state == EntryState.REMOVED ||
                _state == EntryState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _precious = _cached = _fromClient = _fromStore = _removed = false;
            _error = true;
            break;
        case REMOVED:
            if (_state == EntryState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            _precious = _cached = _fromClient = _fromStore = _error = false;
            _removed = true;
            break;
        case DESTROYED:
            if (_state != EntryState.REMOVED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        }

        _state = state;
        markDirty();
    }

    /**
     * Returns true if the state is dirty, i.e., needs to be made
     * persistent. Clears the dirty flag. A second call to dirty()
     * will always return false, unless the state was made dirty
     * inbetween the two calls.
     */
    public boolean dirty()
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
    private void markDirty()
    {
        _dirty = true;
    }

    public boolean isSticky()
    {
        return !_sticky.isEmpty();
    }

    public List<StickyRecord> stickyRecords()
    {
        return new ArrayList<>(_sticky);
    }

    public List<StickyRecord> removeExpiredStickyFlags()
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

    public boolean setSticky(String owner, long expire, boolean overwrite)
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
    private boolean cleanSticky(String owner, long time)
        throws IllegalStateException
    {
        Iterator<StickyRecord> i = _sticky.iterator();
        while (i.hasNext()) {
            StickyRecord record = i.next();
            if (record.owner().equals(owner)) {
                if ((time > -1) && record.isValidAt(time)) {
                    return false;
                }
                i.remove();
                markDirty();
            }
        }
        return true;
    }

    public String toString()
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

        _state = EntryState.BROKEN;
        if (_fromClient) {
            _state = EntryState.FROM_CLIENT;
        }
        if (_fromStore) {
            _state = EntryState.FROM_STORE;
        }
        if (_cached) {
            _state = EntryState.CACHED;
        }
        if (_precious) {
            _state = EntryState.PRECIOUS;
        }
        if (_removed) {
            _state = EntryState.REMOVED;
        }
        if (_error) {
            _state = EntryState.BROKEN;
        }
    }
}
