package org.dcache.acl.enums;

/**
 * The OPEN operation opens a regular file in a directory with the provided name or filehandle.
 * Specification whether a file is be created or not, and the method of creation is via the opentype
 * (OPEN4_NOCREATE or OPEN4_CREATE) parameter.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum OpenType {
    /**
     * file is be not created
     */
    OPEN4_NOCREATE(0),

    /**
     * file is be created
     */
    OPEN4_CREATE(1);

    private final int _value;

    private OpenType(int value) {
        _value = value;
    }

    public int getValue() {
        return _value;
    }

    public boolean equals(int value) {
        return _value == value;
    }

    /**
     * @param value
     * @return OpenType
     */
    public static OpenType valueOf(int value) throws IllegalArgumentException {
        for (OpenType type : OpenType.values()) {
            if (type._value == value) {
                return type;
            }
        }

        throw new IllegalArgumentException("Illegal argument (value of opentype): " + value);
    }

}
