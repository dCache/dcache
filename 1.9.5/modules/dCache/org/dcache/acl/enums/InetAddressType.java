package org.dcache.acl.enums;

/**
 * @author David Melkumyan, DESY Zeuthen
 *
 */
@Deprecated
public enum InetAddressType {
    Other(0),
    IPv4(4),
    IPv6(16);

    private final int _size;

    private InetAddressType(int size) {
        _size = size;
    }

    public int getSize() {
        return _size;
    }
}
