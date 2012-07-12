package org.dcache.pool.classic;

import java.util.List;
import java.util.Collections;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

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
    public List<StickyRecord> getStickyRecords(StorageInfo info)
    {
        AccessLatency al = info.getAccessLatency();
        if (al != null && al.equals(AccessLatency.ONLINE)) {
            return Collections.singletonList(new StickyRecord("system", -1));
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public EntryState getTargetState(StorageInfo info)
    {
        // flush to tape only if the file defined as a 'tape
        // file'( RP = Custodial) and the HSM is defined
        RetentionPolicy rp = info.getRetentionPolicy();
        if (info.getKey("overwrite") != null) {
            return EntryState.CACHED;
        } else if (rp != null && !info.isStored() && rp.equals(RetentionPolicy.CUSTODIAL)) {
            return EntryState.PRECIOUS;
        } else {
            return EntryState.CACHED;
        }
    }
}