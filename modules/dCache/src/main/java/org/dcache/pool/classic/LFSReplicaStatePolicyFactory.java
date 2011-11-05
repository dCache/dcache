package org.dcache.pool.classic;

public class LFSReplicaStatePolicyFactory
{
    /**
     * Factory method for creating ReplicaStatePolicies based on LFS
     * mode.
     */
    public static ReplicaStatePolicy createInstance(String lfs)
    {
        if (lfs == null || !lfs.equals("volatile") && !lfs.equals("transient")) {
            return new ALRPReplicaStatePolicy();
        } else {
            return new VolatileReplicaStatePolicy();
        }
    }
}