package org.dcache.acl.enums;


/**
 * The RsType value allows to distinguish between different resource types:
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum RsType {
    /**
     * File system directory.
     */
    DIR(0x00000000),

    /**
     * File system file.
     */
    FILE(0x00000001);

    private final int _value;

    private RsType(int value) {
        _value = value;
    }

    public int getValue() {
        return _value;
    }

    public boolean matches(int mask) {
        return (_value & mask) == _value;
    }

    public static RsType valueOf(int value) throws IllegalArgumentException {
        for (RsType type : RsType.values()) {
            if (type._value == value) {
                return type;
            }
        }

        throw new IllegalArgumentException("Illegal argument (value of resource type): " + value);
    }

}
