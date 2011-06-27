package org.dcache.admin.webadmin.datacollector.datatypes;

import static com.google.common.base.Preconditions.checkNotNull;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;

public class MoverInfo implements Comparable<MoverInfo> {

    private final IoDoorInfo _ioDoorInfo;
    private final IoDoorEntry _ioDoorEntry;
    private IoJobInfo _ioJobInfo = null;

    public MoverInfo(IoDoorInfo info, IoDoorEntry entry) {
        checkNotNull(info);
        checkNotNull(entry);
        _ioDoorInfo = info;
        _ioDoorEntry = entry;
    }

    public void setIoJobInfo(IoJobInfo ioJobInfo) {
        _ioJobInfo = ioJobInfo;
    }

    public IoJobInfo getIoJobInfo() {
        return _ioJobInfo;
    }

    public IoDoorEntry getIoDoorEntry() {
        return _ioDoorEntry;
    }

    public IoDoorInfo getIoDoorInfo() {
        return _ioDoorInfo;
    }

    public boolean hasJobInfo() {
        return _ioJobInfo != null;
    }

    @Override
    public int compareTo(MoverInfo other) {
        int tmp = _ioDoorInfo.getDomainName().compareTo(other._ioDoorInfo.getDomainName());
        if (tmp != 0) {
            return tmp;
        }
        tmp = _ioDoorInfo.getCellName().compareTo(other._ioDoorInfo.getCellName());
        if (tmp != 0) {
            return tmp;
        }
        return Long.valueOf(_ioDoorEntry.getSerialId()).
                compareTo(Long.valueOf(other._ioDoorEntry.getSerialId()));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MoverInfo)) {
            return false;
        }

        MoverInfo other = (MoverInfo) obj;
        return _ioDoorInfo.getDomainName().equals(other._ioDoorInfo.getDomainName()) &&
                _ioDoorInfo.getCellName().equals(other._ioDoorInfo.getCellName()) &&
                (_ioDoorEntry.getSerialId() == other._ioDoorEntry.getSerialId());
    }

    @Override
    public int hashCode() {
        return (int) _ioDoorEntry.getSerialId() ^
                _ioDoorInfo.getCellName().hashCode() ^
                _ioDoorInfo.getDomainName().hashCode();
    }
}
