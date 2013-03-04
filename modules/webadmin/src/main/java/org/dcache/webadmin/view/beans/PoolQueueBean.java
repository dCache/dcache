package org.dcache.webadmin.view.beans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author jans
 */
public class PoolQueueBean implements Comparable<PoolQueueBean>, Serializable {

    private static final long serialVersionUID = -316655119254618712L;
    private String _name = "";
    private String _domainName = "";
    private Map<String, PoolRequestQueue> _movers =
            new HashMap<>();
    private static final Logger _log = LoggerFactory.getLogger(PoolQueueBean.class);

    public PoolQueueBean() {
    }

    public String getDomainName() {
        return _domainName;
    }

    public void setDomainName(String domainName) {
        _domainName = domainName;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public Map<String, PoolRequestQueue> getMoverQueues() {
        return _movers;
    }

    public void setMoverQueues(Map<String, PoolRequestQueue> moverQueues) {
        _movers = moverQueues;
    }

    public void addRequestQueue(PoolRequestQueue queue) {
        _movers.put(queue.getName(), queue);
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ getDomainName().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof PoolQueueBean)) {
            return false;
        }
        PoolQueueBean otherBean = (PoolQueueBean) other;

        return (getName().equals(otherBean.getName()) &&
                getDomainName().equals(otherBean.getDomainName()));
    }

    @Override
    public int compareTo(PoolQueueBean other) {
        return (getName().compareTo(other.getName()) +
                getDomainName().compareTo(other.getDomainName()));
    }
}
