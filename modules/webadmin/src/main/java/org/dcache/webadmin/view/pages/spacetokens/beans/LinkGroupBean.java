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

    private static final long serialVersionUID = 268459757771571652L;
    private String _name = "";
//  -1 means "not assinged"
    private long _id = -1;
    private String _allowed = "";
    private String _vos = "";
    private long _available;
    private long _reserved;
    private long _free;
    private long _total;
    private List<SpaceReservationBean> _reservations = new ArrayList<>();
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
        calculateTotal();
    }

    public long getFree() {
        return DiskSpaceUnit.BYTES.convert(_free, _displayUnit);
    }

    public void setFree(long free) {
        _free = free;
    }

    public boolean hasId() {
        return _id != -1;
    }

    public long getId() {
        return _id;
    }

    public void setId(long id) {
        _id = id;
    }

    public long getReserved() {
        return DiskSpaceUnit.BYTES.convert(_reserved, _displayUnit);
    }

    public void setReserved(long reserved) {
        _reserved = reserved;
        calculateTotal();
    }

    public long getTotal() {
        return DiskSpaceUnit.BYTES.convert(_total, _displayUnit);
    }

    public String getVos() {
        return _vos;
    }

    public void setVos(String vos) {
        _vos = vos;
    }

    private void calculateTotal() {
        _total = _available + _reserved;
    }

    public void addSpaceReservation(SpaceReservationBean reservation) {
        _reservations.add(reservation);
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
