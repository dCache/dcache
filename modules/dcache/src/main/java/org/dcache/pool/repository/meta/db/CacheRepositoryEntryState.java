package org.dcache.pool.repository.meta.db;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

/**
 * The CacheRepositoryEntryState encapsulates state information about
 * a file.
 */
public class CacheRepositoryEntryState implements Serializable
{
    private static final long serialVersionUID = -715461991190516015L;

    private final Set<StickyRecord> _sticky;
    private boolean _precious;
    private boolean _cached;
    private boolean _fromClient;
    private boolean _fromStore;
    private boolean _error;
    private boolean _removed;

    public CacheRepositoryEntryState(EntryState state, Collection<StickyRecord> sticky)
    {
        switch (state) {
        case NEW:
            break;
        case FROM_CLIENT:
            _fromClient = true;
            break;
        case FROM_STORE:
            _fromStore = true;
            break;
        case FROM_POOL:
            _fromStore = true;
            break;
        case CACHED:
            _cached = true;
            break;
        case PRECIOUS:
            _precious = true;
            break;
        case BROKEN:
            _error = true;
            break;
        case REMOVED:
        case DESTROYED:
            _removed = true;
            break;
        default:
            throw new IllegalArgumentException();
        }

        _sticky = new HashSet<>(sticky);
    }

    public EntryState getState()
    {
        if (_fromClient) {
            return EntryState.FROM_CLIENT;
        }
        if (_fromStore) {
            return EntryState.FROM_STORE;
        }
        if (_cached) {
            return EntryState.CACHED;
        }
        if (_precious) {
            return EntryState.PRECIOUS;
        }
        if (_removed) {
            return EntryState.REMOVED;
        }
        if (_error) {
            return  EntryState.BROKEN;
        }
        return EntryState.NEW;
    }

    public Collection<StickyRecord> stickyRecords()
    {
        return Collections.unmodifiableCollection(_sticky);
    }
}
