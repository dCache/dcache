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

    private String _groupName;
    private List<SelectableWrapper<PoolCommandBean>> _pools =
            new ArrayList<SelectableWrapper<PoolCommandBean>>();

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
    public int compareTo(PoolAdminBean other) {
        if (other == null) {
            throw new NullPointerException();
        }

        return this.getGroupName().compareTo(other.getGroupName());

    }
}
