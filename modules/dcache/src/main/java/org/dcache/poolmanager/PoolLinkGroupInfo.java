package org.dcache.poolmanager;

import com.google.common.base.MoreObjects;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;

public class PoolLinkGroupInfo
{
    private final String _groupName;
    private final long _totalSpaceInBytes;
    private final long _availableSpaceInBytes;
    private final boolean _custodialAllowed;
    private final boolean _replicaAllowed;
    private final boolean _outputAllowed;
    private final boolean _nearlineAllowed;
    private final boolean _onlineAllowed;


    public PoolLinkGroupInfo(SelectionLinkGroup linkGroup, long totalSpace, long availableSpace) {
        _groupName = linkGroup.getName();
        _availableSpaceInBytes = availableSpace;
        _totalSpaceInBytes = totalSpace;
        _custodialAllowed = linkGroup.isCustodialAllowed();
        _replicaAllowed = linkGroup.isReplicaAllowed();
        _outputAllowed = linkGroup.isOutputAllowed();
        _nearlineAllowed = linkGroup.isNearlineAllowed();
        _onlineAllowed = linkGroup.isOnlineAllowed();
    }

    /**
     *
     * @return the linkGroup name
     */
    public String getName() {
        return _groupName;
    }

    /**
     *
     * @return total space of all pools in the linkGroup in bytes
     */
    public long getTotalSpace() {
        return _totalSpaceInBytes;
    }

    /**
     *
     * @return available space of all pools in the linkGroup in bytes
     */
    public long getAvailableSpaceInBytes() {
        return _availableSpaceInBytes;
    }

    /**
     *
     * @return true if LinkGroup allows custodial files
     */
    public boolean isCustodialAllowed() {
        return _custodialAllowed;
    }

    /**
     *
     * @return true if LinkGroup allows output files
     */
    public boolean isOutputAllowed() {
        return _outputAllowed;
    }

    /**
     *
     * @return true if LinkGroup allows replica files
     */
    public boolean isReplicaAllowed() {
        return _replicaAllowed;
    }

    /**
     *
     * @return true if LinkGour allows online files
     */
    public boolean isOnlineAllowed() {
        return _onlineAllowed;
    }

    /**
     *
     * @return true if LinkGour allows nearline files
     */
    public boolean isNearlineAllowed() {
        return _nearlineAllowed;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("groupName", _groupName)
                .add("totalSpace", _totalSpaceInBytes)
                .add("availableSpace", _availableSpaceInBytes)
                .add("custodial", _custodialAllowed)
                .add("replica", _replicaAllowed)
                .add("output", _outputAllowed)
                .add("nearline", _nearlineAllowed)
                .add("online", _onlineAllowed)
                .toString();
    }
}
