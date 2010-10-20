package org.dcache.webadmin.model.businessobjects;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Datacontainer for the Linkgroup-entity of dCache
 * @author jans
 */
public class LinkGroup {

    private String _name = "";
    private String _id = "";
    private String _vos = "";
    private boolean _onlineAllowed;
    private boolean _nearlineAllowed;
    private boolean _custodialAllowed;
    private boolean _outputAllowed;
    private boolean _replicaAllowed;
    private long _available;
    private long _reserved;
    private long _free;
    private long _used;
    private long _total;
    private Set<SpaceReservation> _spaceReservations =
            new HashSet<SpaceReservation>();

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public boolean isCustodialAllowed() {
        return _custodialAllowed;
    }

    public void setCustodialAllowed(boolean custodialAllowed) {
        _custodialAllowed = custodialAllowed;
    }

    public boolean isNearlineAllowed() {
        return _nearlineAllowed;
    }

    public void setNearlineAllowed(boolean nearlineAllowed) {
        _nearlineAllowed = nearlineAllowed;
    }

    public boolean isOnlineAllowed() {
        return _onlineAllowed;
    }

    public void setOnlineAllowed(boolean onlineAllowed) {
        _onlineAllowed = onlineAllowed;
    }

    public boolean isOutputAllowed() {
        return _outputAllowed;
    }

    public void setOutputAllowed(boolean outputAllowed) {
        _outputAllowed = outputAllowed;
    }

    public boolean isReplicaAllowed() {
        return _replicaAllowed;
    }

    public void setReplicaAllowed(boolean replicaAllowed) {
        _replicaAllowed = replicaAllowed;
    }

    public Set<SpaceReservation> getSpaceReservations() {
        return Collections.unmodifiableSet(_spaceReservations);
    }

    public long getAvailable() {
        return _available;
    }

    public void setAvailable(long available) {
        _available = available;
    }

    public long getFree() {
        return _free;
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
        return _reserved;
    }

    public void setReserved(long reserved) {
        _reserved = reserved;
    }

    public long getTotal() {
        return _total;
    }

    public void setTotal(long total) {
        _total = total;
    }

    public long getUsed() {
        return _used;
    }

    public void setUsed(long used) {
        _used = used;
    }

    public String getVos() {
        return _vos;
    }

    public void setVos(String vos) {
        _vos = vos;
    }

    public void addSpaceReservation(SpaceReservation reservation) {
        _spaceReservations.add(reservation);
    }
}
