package org.dcache.acl.enums;

/**
 * Access Attributes.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum AccessAttribute {
    /**
     * Read data from file or read a directory.
     */
    ACCESS4_READ(0x00000001, 'r'),

    /**
     * Look up a name in a directory (no meaning for non-directory objects).
     */
    ACCESS4_LOOKUP(0x00000002, 'l'),

    /**
     * Rewrite existing file data or modify existing directory entries.
     */
    ACCESS4_MODIFY(0x00000004, 'm'),

    /**
     * Write new data or add directory entries.
     */
    ACCESS4_EXTEND(0x00000008, 'e'),

    /**
     * Delete an existing directory entry.
     */
    ACCESS4_DELETE(0x00000010, 'd'),

    /**
     * Execute file (no meaning for a directory).
     */
    ACCESS4_EXECUTE(0x00000020, 'x');

    private final int _value;

    private final char _abbreviation;

    private AccessAttribute(int value, char abbreviation) {
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

    public boolean matches(int flags) {
        return (_value & flags) == _value;
    }

    /**
     * @param attribute
     * @return AccessAttribute
     */
    public static AccessAttribute valueOf(int attribute) throws IllegalArgumentException {
        for (AccessAttribute attr : AccessAttribute.values()) {
            if (attr._value == attribute) {
                return attr;
            }
        }

        throw new IllegalArgumentException("Illegal argument (value of access attribute): " + attribute);
    }

    /**
     * @param attributes
     *            Access attributes bit mask
     * @return Return string representation of access attributes mask
     */
    public static String asString(int attributes) {
        // no flags
        if ( attributes == 0 ) {
            return "0";
        }

        StringBuilder sb = new StringBuilder();
        for (AccessAttribute attribute : AccessAttribute.values()) {
            if (attribute.matches(attributes)) {
                sb.append(attribute.getAbbreviation());
            }
        }

        return sb.toString();
    }

}
