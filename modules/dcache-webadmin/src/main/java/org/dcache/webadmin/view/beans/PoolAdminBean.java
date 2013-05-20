package org.dcache.webadmin.view.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.dcache.webadmin.view.util.SelectableWrapper;

/**
 * Corresponding Bean to the PoolAdminPage
 * @author jans
 */
public class PoolAdminBean implements Comparable<PoolAdminBean>, Serializable {

    private static final long serialVersionUID = 275064974609435532L;
    private String _groupName;
    private List<SelectableWrapper<PoolCommandBean>> _pools =
            new ArrayList<>();

    public PoolAdminBean(String groupName) {
        _groupName = groupName;
    }

    public List<SelectableWrapper<PoolCommandBean>> getPools() {
        return _pools;
    }

    public void setPools(List<SelectableWrapper<PoolCommandBean>> pools) {
        _pools = pools;
    }

    public String getGroupName() {
        return _groupName;
    }

    public void setGroupName(String groupName) {
        _groupName = groupName;
    }

    @Override
    public int hashCode() {
        return getGroupName().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof PoolAdminBean)) {
            return false;
        }
        PoolAdminBean otherBean = (PoolAdminBean) other;

        return getGroupName().equals(otherBean.getGroupName());
    }

    @Override
    public int compareTo(PoolAdminBean other) {
        return getGroupName().compareTo(other.getGroupName());
    }
}
