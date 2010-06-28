package org.dcache.webadmin.model.businessobjects;

import java.io.Serializable;

/**
 * This is a simple Data-Container Object for the relevant information
 * of dCache-Pools for later displaying.
 *
 * @author jan schaefer
 */
public class Pool implements Serializable {

    private String _name = "";
    private boolean _enabled = false;
    private long _totalSpace = 0;
    private long _freeSpace = 0;
    private long _preciousSpace = 0;
    private long _usedSpace = 0;
    private MoverQueue _movers = new MoverQueue(0, 0, 0);
    private MoverQueue _restores = new MoverQueue(0, 0, 0);
    private MoverQueue _stores = new MoverQueue(0, 0, 0);
    private MoverQueue _p2pserver = new MoverQueue(0, 0, 0);
    private MoverQueue _p2pclient = new MoverQueue(0, 0, 0);
    private MoverQueue _p2p = new MoverQueue(0, 0, 0);
    private MoverQueue _regular = new MoverQueue(0, 0, 0);

    public Pool() {
    }

    public boolean isEnabled() {
        return _enabled;
    }

    public void setEnabled(boolean enabled) {
        _enabled = enabled;
    }

    public long getFreeSpace() {
        return _freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        _freeSpace = freeSpace;
    }

    public long getUsedSpace() {
        return _usedSpace;
    }

    public void setUsedSpace(long usedSpace) {
        _usedSpace = usedSpace;
    }

    public long getPreciousSpace() {
        return _preciousSpace;
    }

    public void setPreciousSpace(long preciousSpace) {
        _preciousSpace = preciousSpace;
    }

    public long getTotalSpace() {
        return _totalSpace;
    }

    public void setTotalSpace(long totalSpace) {
        _totalSpace = totalSpace;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public MoverQueue getMovers() {
        return _movers;
    }

    public void setMovers(MoverQueue movers) {
        _movers = movers;
    }

    public MoverQueue getP2p() {
        return _p2p;
    }

    public void setP2p(MoverQueue p2p) {
        _p2p = p2p;
    }

    public MoverQueue getP2pclient() {
        return _p2pclient;
    }

    public void setP2pclient(MoverQueue p2pclient) {
        _p2pclient = p2pclient;
    }

    public MoverQueue getP2pserver() {
        return _p2pserver;
    }

    public void setP2pserver(MoverQueue p2pserver) {
        _p2pserver = p2pserver;
    }

    public MoverQueue getRegular() {
        return _regular;
    }

    public void setRegular(MoverQueue regular) {
        _regular = regular;
    }

    public MoverQueue getRestores() {
        return _restores;
    }

    public void setRestores(MoverQueue restores) {
        _restores = restores;
    }

    public MoverQueue getStores() {
        return _stores;
    }

    public void setStores(MoverQueue stores) {
        _stores = stores;
    }

    @Override
    public int hashCode() {
        return (int) (_name.hashCode() ^ _totalSpace ^
                _freeSpace ^ _preciousSpace ^ _usedSpace);
    }

    @Override
    public boolean equals(Object testObject) {
        if (this == testObject) {
            return true;
        }

        if (!(testObject instanceof Pool)) {
            return false;
        }

        Pool otherPool = (Pool) testObject;

        if (!(otherPool._name.equals(_name))) {
            return false;
        }

        if (!(otherPool._freeSpace == _freeSpace)) {
            return false;
        }
        if (!(otherPool._preciousSpace == _preciousSpace)) {
            return false;
        }
        if (!(otherPool._totalSpace == _totalSpace)) {
            return false;
        }
        if (!(otherPool._usedSpace == _usedSpace)) {
            return false;
        }
        if (!(otherPool._movers.equals(_movers))) {
            return false;
        }
        if (!(otherPool._movers.equals(_movers))) {
            return false;
        }
        if (!(otherPool._p2p.equals(_p2p))) {
            return false;
        }
        if (!(otherPool._p2pclient.equals(_p2pclient))) {
            return false;
        }
        if (!(otherPool._p2pserver.equals(_p2pserver))) {
            return false;
        }
        if (!(otherPool._regular.equals(_regular))) {
            return false;
        }
        if (!(otherPool._restores.equals(_restores))) {
            return false;
        }
        if (!(otherPool._stores.equals(_stores))) {
            return false;
        }
        return true;
    }
}
