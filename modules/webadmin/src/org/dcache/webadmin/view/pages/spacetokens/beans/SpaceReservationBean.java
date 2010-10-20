package org.dcache.webadmin.view.pages.spacetokens.beans;

import java.io.Serializable;
import org.dcache.webadmin.view.util.DiskSpaceUnit;

/**
 *
 * @author jans
 */
public class SpaceReservationBean implements Serializable {

    private String _id = "";
    private String _description = "";
    private String _linkGroupRef = "";
    private String _storage = "";
    private String _vogroup = "";
    private String _state = "";
    private long _size;
    private long _usedSpace;
    private long _allocatedSpace;
    private String _created = "";
    private String _lifetime = "";
    private String _expiration = "";
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
        return _expiration;
    }

    public void setExpiration(String expiration) {
        _expiration = expiration;
    }

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    public String getLifetime() {
        return _lifetime;
    }

    public void setLifetime(String lifetime) {
        _lifetime = lifetime;
    }

    public String getLinkGroupRef() {
        return _linkGroupRef;
    }

    public void setLinkGroupRef(String linkGroupId) {
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
