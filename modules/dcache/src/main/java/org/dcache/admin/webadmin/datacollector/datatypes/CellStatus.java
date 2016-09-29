package org.dcache.admin.webadmin.datacollector.datatypes;

import java.util.Objects;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellPath;

/**
 * Ping returning Long.MAX_VALUE means unknown/never reached
 * @author jans
 */
public class CellStatus {

    private String _createdDateTime = "unknown";
    private String _version = "";
    private long _ping = Long.MAX_VALUE;
    private long _lastAliveTime = System.currentTimeMillis();
    private CellInfo _cellInfo = new CellInfo();

    public CellStatus(CellAddressCore address) {
        _cellInfo.setCellName(address.getCellName());
        _cellInfo.setDomainName(address.getCellDomainName());
    }

    public void setPingUnreached() {
        _ping = Long.MAX_VALUE;
    }

    public void updateLastAliveTime() {
        _lastAliveTime = System.currentTimeMillis();
    }

    public String getCreatedDateTime() {
        return _createdDateTime;
    }

    public void setCreatedDateTime(String createdDateTime) {
        _createdDateTime = createdDateTime;
    }

    public String getDomainName() {
        return _cellInfo.getDomainName();
    }

    public void setDomainName(String domainName) {
        _cellInfo.setDomainName(domainName);
    }

    public int getEventQueueSize() {
        return _cellInfo.getEventQueueSize();
    }

    public void setEventQueueSize(int eventQueueSize) {
        _cellInfo.setEventQueueSize(eventQueueSize);
    }

    public String getCellName() {
        return _cellInfo.getCellName();
    }

    public long getPing() {
        return _ping;
    }

    public void setPing(long ping) {
        _ping = ping;
    }

    public int getThreadCount() {
        return _cellInfo.getThreadCount();
    }

    public void setThreadCount(int threadCount) {
        _cellInfo.setThreadCount(threadCount);
    }

    public String getVersion() {
        return _version;
    }

    public void setVersion(String version) {
        _version = version;
    }

    public void setCellInfo(CellInfo cellInfo) {
        _cellInfo = cellInfo;
        _createdDateTime = _cellInfo.getCreationTime().toString();
        _version = Objects.toString(_cellInfo.getCellVersion(), "");
    }

    public long getLastAliveTime() {
        return _lastAliveTime;
    }

    public void setLastAliveTime(long lastAliveTime) {
        _lastAliveTime = lastAliveTime;
    }

    public CellAddressCore getCellAddress()
    {
        return new CellAddressCore(_cellInfo.getCellName(), _cellInfo.getDomainName());
    }

    public CellPath getCellPath()
    {
        return new CellPath(_cellInfo.getCellName(), _cellInfo.getDomainName());
    }
}

