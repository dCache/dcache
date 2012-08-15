package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 *
 * @author jans
 */
public class CellServicesBean implements Comparable<CellServicesBean>,
        Serializable {

    private static final long serialVersionUID = 8879607744752544606L;
    private String _name = "";
    private String _domainName = "";
    private String _createdDateTime = "unknown";
    private String _version = "";
    private long _ping = Long.MAX_VALUE;
    private int _threadCount;
    private int _eventQueueSize;

    public String getCreatedDateTime() {
        return _createdDateTime;
    }

    public void setCreatedDateTime(String createdDateTime) {
        _createdDateTime = createdDateTime;
    }

    public String getDomainName() {
        return _domainName;
    }

    public void setDomainName(String domainName) {
        _domainName = domainName;
    }

    public int getEventQueueSize() {
        return _eventQueueSize;
    }

    public void setEventQueueSize(int eventQueueSize) {
        _eventQueueSize = eventQueueSize;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public String getPing() {
        return (_ping != Long.MAX_VALUE) ? String.valueOf(_ping) : "not reached";
    }

    public void setPing(long ping) {
        _ping = ping;
    }

    public int getThreadCount() {
        return _threadCount;
    }

    public void setThreadCount(int threadCount) {
        _threadCount = threadCount;
    }

    public String getVersion() {
        return _version;
    }

    public void setVersion(String version) {
        _version = version;
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
        if (!(other instanceof CellServicesBean)) {
            return false;
        }
        CellServicesBean otherBean = (CellServicesBean) other;
        return (getName().equals(otherBean.getName()) &&
                getDomainName().equals(otherBean.getDomainName()));
    }

    @Override
    public int compareTo(CellServicesBean other) {
        return (getName().compareTo(other.getName()) +
                getDomainName().compareTo(other.getDomainName()));
    }
}
