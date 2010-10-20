package org.dcache.webadmin.model.businessobjects;

import org.dcache.webadmin.model.util.AccessLatency;
import org.dcache.webadmin.model.util.RetentionPolicy;

/**
 * Datacontainer for the Spacereservation-entity of dCache
 * @author jans
 */
public class SpaceReservation {

    private String _id = "";
    private String _description = "";
    private String _linkGroupRef = "";
    private RetentionPolicy _retentionPolicy;
    private AccessLatency _accessLatency;
    private String _vogroup = "";
    private String _state = "";
    private long _totalSpace;
    private long _usedSpace;
    private long _freeSpace;
    private long _allocatedSpace;
    private String _created = "";

    public long getAllocatedSpace() {
        return _allocatedSpace;
    }

    public void setAllocatedSpace(long allocated) {
        _allocatedSpace = allocated;
    }

    public String getCreated() {
        return _created;
    }

    public void setCreated(String created) {
        _created = created;
    }

    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    public String getLinkGroupRef() {
        return _linkGroupRef;
    }

    public void setLinkGroupRef(String linkGroupId) {
        _linkGroupRef = linkGroupId;
    }

    public boolean belongsToLinkGroup(String linkGroupId) {
        return _linkGroupRef.equals(linkGroupId);
    }

    public long getTotalSpace() {
        return _totalSpace;
    }

    public void setTotalSpace(long size) {
        _totalSpace = size;
    }

    public String getState() {
        return _state;
    }

    public void setState(String state) {
        _state = state;
    }

    public long getUsedSpace() {
        return _usedSpace;
    }

    public void setUsedSpace(long used) {
        _usedSpace = used;
    }

    public String getVogroup() {
        return _vogroup;
    }

    public void setVogroup(String vogroup) {
        _vogroup = vogroup;
    }

    public long getFreeSpace() {
        return _freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        _freeSpace = freeSpace;
    }

    public AccessLatency getAccessLatency() {
        return _accessLatency;
    }

    public void setAccessLatency(AccessLatency accessLatency) {
        _accessLatency = accessLatency;
    }

    public RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        _retentionPolicy = retentionPolicy;
    }

    @Override
    public int hashCode() {
        return _id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SpaceReservation)) {
            return false;
        }
        SpaceReservation otherReservation = (SpaceReservation) other;
        return _id.equals(otherReservation._id);
    }
}
