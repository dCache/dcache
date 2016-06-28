package org.dcache.pool.classic;

import java.util.Collections;
import java.util.List;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

/**
 * A ReplicaStatePolicy which uses the AccessLatency and
 * RetentionPolicy of a file to determine the state of a new replica.
 *
 *     precious: have to goto tape
 *     cached: free to be removed by sweeper
 *     cached+sticky: does not go to tape, isn't removed by sweeper
 *
 * New states depending on AL and RP:
 *
 *     Custodial+ONLINE   (T1D1) : precious+sticky  => cached+sticky
 *     Custodial+NEARLINE (T1D0) : precious         => cached
 *     Output+ONLINE      (T0D1) : cached+sticky    => cached+sticky
 */
public class ALRPReplicaStatePolicy implements ReplicaStatePolicy
{
    @Override
    public List<StickyRecord> getStickyRecords(FileAttributes fileAttributes)
    {
        if (fileAttributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
            return Collections.singletonList(new StickyRecord("system", -1));
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public ReplicaState getTargetState(FileAttributes fileAttributes)
    {
        // flush to tape only if the file defined as a 'tape
        // file'( RP = Custodial) and the HSM is defined
        StorageInfo info = fileAttributes.getStorageInfo();
        if (info.getKey("overwrite") != null) {
            return ReplicaState.CACHED;
        } else if (!info.isStored() && fileAttributes.getRetentionPolicy().equals(RetentionPolicy.CUSTODIAL)) {
            return ReplicaState.PRECIOUS;
        } else {
            return ReplicaState.CACHED;
        }
    }
}
