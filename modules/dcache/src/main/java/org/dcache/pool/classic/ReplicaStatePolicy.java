package org.dcache.pool.classic;

import java.util.List;

import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

/**
 * A ReplicaStatePolicy defines the initial EntryState and
 * StickyRecords of a new replica when it is uploaded to a pool from a
 * client.
 */
public interface ReplicaStatePolicy
{
    List<StickyRecord> getStickyRecords(FileAttributes fileAttributes);
    ReplicaState getTargetState(FileAttributes fileAttributes);
}
