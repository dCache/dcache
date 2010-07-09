package org.dcache.webadmin.model.businessobjects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
    private List<MoverQueue> _moverQueues = new ArrayList<MoverQueue>();

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

    public List<MoverQueue> getMoverQueues() {
        return _moverQueues;
    }

    public void setMoverQueues(List<MoverQueue> moverQueues) {
        _moverQueues = moverQueues;
    }

    public void addMoverQueue(MoverQueue queue) {
        _moverQueues.add(queue);
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
        if (!(otherPool._moverQueues.equals(_moverQueues))) {
            return false;
        }

        return true;
    }
}
