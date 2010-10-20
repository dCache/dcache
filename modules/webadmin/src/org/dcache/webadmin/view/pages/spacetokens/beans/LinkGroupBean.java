package org.dcache.webadmin.view.pages.spacetokens.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.dcache.webadmin.view.util.DiskSpaceUnit;

/**
 *
 * @author jans
 */
public class LinkGroupBean implements Serializable, Comparable<LinkGroupBean> {

    private String _name = "";
    private String _id = "";
    private String _allowed = "";
    private String _vos = "";
    private long _available;
    private long _reserved;
    private long _free;
    private long _total;
    private List<SpaceReservationBean> _reservations = new ArrayList<SpaceReservationBean>();
    private DiskSpaceUnit _displayUnit = DiskSpaceUnit.MIBIBYTES;

    public List<SpaceReservationBean> getReservations() {
        return Collections.unmodifiableList(_reservations);
    }

    public void setReservations(List<SpaceReservationBean> reservations) {
        _reservations = reservations;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public String getAllowed() {
        return _allowed;
    }

    public void setAllowed(String allowed) {
        _allowed = allowed;
    }

    public long getAvailable() {
        return DiskSpaceUnit.BYTES.convert(_available, _displayUnit);
    }

    public void setAvailable(long available) {
        _available = available;
    }

    public long getFree() {
        return DiskSpaceUnit.BYTES.convert(_free, _displayUnit);
    }

    public void setFree(long free) {
        _free = free;
    }

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    public long getReserved() {
        return DiskSpaceUnit.BYTES.convert(_reserved, _displayUnit);
    }

    public void setReserved(long reserved) {
        _reserved = reserved;
    }

    public long getTotal() {
        return DiskSpaceUnit.BYTES.convert(_total, _displayUnit);
    }

    public void setTotal(long total) {
        _total = total;
    }

    public String getVos() {
        return _vos;
    }

    public void setVos(String vos) {
        _vos = vos;
    }

    @Override
    public int compareTo(LinkGroupBean other) {
        return this._name.compareTo(other._name);
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof LinkGroupBean)) {
            return false;
        }

        LinkGroupBean otherGroup = (LinkGroupBean) other;

        if (!(otherGroup._name.equals(_name))) {
            return false;
        }

        return true;
    }
}
