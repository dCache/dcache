package org.dcache.acl.enums;

/**
 * This object consists of a enumeration of possible access definitions.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum AccessType {
    /**
     * Explicitly grants the access
     */
    ACCESS_ALLOWED(0x00000000),

    /**
     * Explicitly denies the access
     */
    ACCESS_DENIED(0x00000001),

    /**
     * Undefined access
     */
    ACCESS_UNDEFINED(0x00000002);

    private final int _value;

    AccessType(int value) {
        _value = value;
    }

    public int getValue() {
        return _value;
    }

    public boolean equals(int value) {
        return _value == value;
    }

    public static AccessType valueOf(int value) throws IllegalArgumentException {
        for (AccessType type : AccessType.values()) {
            if (type._value == value) {
                return type;
            }
        }

        throw new IllegalArgumentException("Illegal value of Access Type): " + value);
    }

    /**
     * Returns ACCESS_ALLOWED for true and ACCESS_DENIED for false.
     */
    public static AccessType valueOf(boolean value)
    {
        return value ? ACCESS_ALLOWED : ACCESS_DENIED;
    }

    /**
     * Conjunction in ternary Kleene logic with ALLOWED being true,
     * DENIED being false, and UNDEFINED being unknown.
     */
    public AccessType and(AccessType other)
    {
        if (this == ACCESS_ALLOWED && other == ACCESS_ALLOWED) {
            return ACCESS_ALLOWED;
        }

        if (this == ACCESS_DENIED || other == ACCESS_DENIED) {
            return ACCESS_DENIED;
        }

        return ACCESS_UNDEFINED;
    }

}
