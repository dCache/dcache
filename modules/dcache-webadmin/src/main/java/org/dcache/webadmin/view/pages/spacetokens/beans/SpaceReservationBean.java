package org.dcache.webadmin.view.pages.spacetokens.beans;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.dcache.webadmin.view.util.DiskSpaceUnit;

/**
 *
 * @author jans
 */
public class SpaceReservationBean implements Serializable {

    private static final long serialVersionUID = 8230858874160681453L;
    private String _id = "";
    private String _description = "";
//  -1 means not assigend yet
    private long _linkGroupRef = -1;
    private String _storage = "";
    private String _vogroup = "";
    private String _state = "";
    private long _size;
    private long _usedSpace;
    private long _allocatedSpace;
    private String _created = "";
    private long _lifetime;
    private long _expiration;
    private DiskSpaceUnit _displayUnit = DiskSpaceUnit.MIBIBYTES;

    public long getAllocatedSpace() {
        return DiskSpaceUnit.BYTES.convert(_allocatedSpace, _displayUnit);
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

    public String getExpiration() {
        if (_expiration != 0) {
            return _expiration == -1
                    ? "NEVER" : new Date(_expiration).toString();
        }
        return "UNKNOWN";
    }

    public void setExpiration(long expiration) {
        _expiration = expiration;
    }

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    public String getLifetime(TimeUnit unit) {
        if (_lifetime != 0) {
            return _lifetime == -1 ? "NEVER"
                    : Long.toString(unit.convert(_lifetime, TimeUnit.MILLISECONDS));
        }
        return "UNKNOWN";
    }

    public void setLifetime(long lifetime) {
        _lifetime = lifetime;
    }

    public boolean belongsTo(LinkGroupBean linkGroup) {
        if (isAssignedToALinkGroup() && linkGroup.hasId()) {
            return linkGroup.getId() == this.getLinkGroupRef();
        }
        return false;
    }

    public boolean isAssignedToALinkGroup() {
        return _linkGroupRef != -1;
    }

    public long getLinkGroupRef() {
        return _linkGroupRef;
    }

    public void setLinkGroupRef(long linkGroupId) {
        _linkGroupRef = linkGroupId;
    }

    public long getSize() {
        return DiskSpaceUnit.BYTES.convert(_size, _displayUnit);
    }

    public void setSize(long size) {
        _size = size;
    }

    public String getState() {
        return _state;
    }

    public void setState(String state) {
        _state = state;
    }

    public String getStorage() {
        return _storage;
    }

    public void setStorage(String storage) {
        _storage = storage;
    }

    public long getUsedSpace() {
        return DiskSpaceUnit.BYTES.convert(_usedSpace, _displayUnit);
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
}
