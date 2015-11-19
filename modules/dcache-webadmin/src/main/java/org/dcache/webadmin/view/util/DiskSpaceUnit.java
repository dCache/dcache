package org.dcache.webadmin.view.util;

/**
 * Conveniance class for DiskSpaceUnits
 * @author jans
 */
public enum DiskSpaceUnit {

    MIBIBYTES(1048576L),
    BYTES(1);
    private final long _numberOfBytes;

    DiskSpaceUnit(long numberOfBytes) {
        _numberOfBytes = numberOfBytes;
    }

    public long convert(long size, DiskSpaceUnit targetUnit) {
        long sizeInBytes = size * this._numberOfBytes;
        return sizeInBytes / targetUnit._numberOfBytes;
    }
}
