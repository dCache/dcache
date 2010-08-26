package org.dcache.acl.enums;

/**
 * Enumeration of authentication type.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public enum AuthType {
    /**
     * Principal has authenticated itself with the system that established the
     * connection to this service (example: NFS2).
     */
    ORIGIN_AUTHTYPE_WEAK(0x00000000, 'W'),

    /**
     * Principal has authenticated itself with the service (example: gsiftp).
     */
    ORIGIN_AUTHTYPE_STRONG(0x00000001, 'S');

    private final int _value;

    private final char _abbreviation;

    private AuthType(int value, char abbreviation) {
        _value = value;
        _abbreviation = abbreviation;
    }

    public int getValue() {
        return _value;
    }

    public char getAbbreviation() {
        return _abbreviation;
    }

    public boolean equals(int value) {
        return _value == value;
    }
}
