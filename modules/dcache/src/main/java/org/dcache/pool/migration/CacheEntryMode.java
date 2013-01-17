package org.dcache.pool.migration;

import java.util.List;
import java.util.Collections;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

/**
 * Immutable record class combining a list of sticky records and a
 * CacheEntry state update. Instances of this class describe how the
 * state of source or target replica is to be updated after the
 * transfer.
 */
public class CacheEntryMode
{
    enum State { SAME, DELETE, REMOVABLE, CACHED, PRECIOUS }

    public final State state;
    public final List<StickyRecord> stickyRecords;

    public CacheEntryMode(State state, List<StickyRecord> stickyRecords)
    {
        this.state = state;
        this.stickyRecords = Collections.unmodifiableList(stickyRecords);
    }
}
