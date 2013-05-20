package org.dcache.webadmin.view.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Representation of a Pool-Group
 * @author jans
 */
public class PoolGroupBean implements Comparable<PoolGroupBean>, Serializable {

    private static final long serialVersionUID = -8750211605825234643L;
    /**   this PoolBeans are a representation of the whole Group - so to say the sum of
     *   all Pools in this Group
     */
    private PoolSpaceBean _poolGroupSpace = new PoolSpaceBean();
    private PoolQueueBean _totalMovers = new PoolQueueBean();
    private List<CellServicesBean> _cellStatuses;
    private List<PoolSpaceBean> _poolSpaces;
    private List<PoolQueueBean> _poolQueues;
    private List<String> _allPoolQueueNames = new ArrayList<>();

    public PoolGroupBean(String name, List<PoolSpaceBean> poolSpaces,
            List<PoolQueueBean> poolQueues) {
        _poolGroupSpace.setName(name);
        _poolSpaces = poolSpaces;
        _poolQueues = poolQueues;
        calculateGroupValues();
        calculateTotalMovers();
    }

    private void calculateGroupValues() {
        _poolGroupSpace.setEnabled(true);
        if (_poolSpaces != null) {
            for (PoolSpaceBean pool : _poolSpaces) {
                _poolGroupSpace.addPoolSpace(pool);
//                if one of the pools of the group is disabled the whole group
//                counts as disabled
                if (!pool.isEnabled()) {
                    _poolGroupSpace.setEnabled(false);
                }
            }
        }
    }

    private void calculateTotalMovers() {
        _allPoolQueueNames = new ArrayList<>();
        if (_poolQueues != null) {
            for (PoolQueueBean pool : _poolQueues) {
                for (PoolRequestQueue queue : pool.getMoverQueues().values()) {
                    PoolRequestQueue totalsQueue = _totalMovers.getMoverQueues().
                            get(queue.getName());
                    if (totalsQueue != null) {
                        totalsQueue.addQueue(queue);
                    } else {
                        totalsQueue = new PoolRequestQueue(queue.getName(),
                                queue.getActive(), queue.getMax(), queue.getQueued());
                        _totalMovers.addRequestQueue(totalsQueue);
                        _allPoolQueueNames.add(queue.getName());
                    }
                }
            }
        } else {
            _totalMovers = new PoolQueueBean();
        }
        _totalMovers.setName("Total");
        _totalMovers.setDomainName("Total");
    }

    public String getName() {
        return _poolGroupSpace.getName();
    }

    public void setName(String name) {
        _poolGroupSpace.setName(name);
    }

    public void setEnabled(boolean enabled) {
        _poolGroupSpace.setEnabled(enabled);
    }

    public boolean isEnabled() {
        return _poolGroupSpace.isEnabled();
    }

    public long getUsedSpace() {
        return _poolGroupSpace.getUsedSpace();
    }

    public long getTotalSpace() {
        return _poolGroupSpace.getTotalSpace();
    }

    public long getPreciousSpace() {
        return _poolGroupSpace.getPreciousSpace();
    }

    public long getFreeSpace() {
        return _poolGroupSpace.getFreeSpace();
    }

    public float getPercentageFree() {
        return _poolGroupSpace.getPercentageFree();
    }

    public float getPercentagePrecious() {
        return _poolGroupSpace.getPercentagePrecious();
    }

    public float getPercentageRemovable() {
        return _poolGroupSpace.getPercentageRemovable();
    }

    public float getPercentagePinned() {
        return _poolGroupSpace.getPercentagePinned();
    }

    public List<CellServicesBean> getCellStatuses() {
        return _cellStatuses;
    }

    public void setCellStatuses(List<CellServicesBean> cellStatuses) {
        _cellStatuses = cellStatuses;
    }

    public List<PoolSpaceBean> getPoolSpaces() {
        return _poolSpaces;
    }

    public void setPoolSpaces(List<PoolSpaceBean> pools) {
        _poolSpaces = pools;
        calculateGroupValues();
    }

    public List<PoolQueueBean> getPoolQueues() {
        return _poolQueues;
    }

    public void setPoolQueues(List<PoolQueueBean> queues) {
        _poolQueues = queues;
        calculateTotalMovers();
    }

    public List<String> getAllPoolQueueNames() {
        return _allPoolQueueNames;
    }

    public PoolQueueBean getTotalMovers() {
        return _totalMovers;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof PoolGroupBean)) {
            return false;
        }
        PoolGroupBean otherBean = (PoolGroupBean) other;

        return getName().equals(otherBean.getName());
    }

    @Override
    public int compareTo(PoolGroupBean other) {
        return getName().compareTo(other.getName());
    }
}
