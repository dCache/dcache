package org.dcache.acl.enums;

/**
 * The AceType is an enumeration (to be implemented as a bit mask).
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum AceType {
    /**
     * Explicitly grants the access defined in the access mask to the
     * file or directory.
     */
    ACCESS_ALLOWED_ACE_TYPE(0x00000000, 'A'),

    /**
     * Explicitly denies the access defined in the access mask to the
     * file or directory.
     */
    ACCESS_DENIED_ACE_TYPE(0x00000001, 'D'),

    /**
     * Log (in a system dependent way) any access attempt to a file or
     * directory that uses any of the access methods specified in access
     * mask.
     */
    ACCESS_AUDIT_ACE_TYPE(0x00000002, 'U'),

    /**
     * Generate an alarm (in a system-dependent way) when any access
     * attempt is made to a file or directory for the access methods
     * specified in access mask.
     */
    ACCESS_ALARM_ACE_TYPE(0x00000003, 'L');

    private final int _value;

    private final char _abbreviation;

    private AceType(int value, char abbreviation) {
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

    public boolean equals(char abbreviation) {
        return _abbreviation == abbreviation;
    }

    public boolean equalsIgnoreCase(char abbreviation) {
        return Character.toUpperCase(_abbreviation) == Character.toUpperCase(abbreviation);
    }

    @Override
    public String toString() {
        return String.valueOf(_abbreviation);
    }

    public static AceType fromAbbreviation(char abbreviation) throws IllegalArgumentException {
        for (AceType type : values()) {
            if (type.equalsIgnoreCase(abbreviation)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid ACE type abbreviation: " + abbreviation);
    }

    public static AceType fromAbbreviation(String abbreviation) throws IllegalArgumentException {
        if ( abbreviation == null || abbreviation.length() != 1 ) {
            throw new IllegalArgumentException(abbreviation == null ? "ACE type abbreviation is NULL"
                    : (abbreviation
                    .length() == 0 ? "ACE type abbreviation is Empty" : "Invalid ACE type abbreviation: " + abbreviation));
        }

        return fromAbbreviation(abbreviation.charAt(0));
    }

    public static AceType valueOf(int value) throws IllegalArgumentException {
        for (AceType type : values()) {
            if (type.getValue() == value) {
                return type;
            }
        }

        throw new IllegalArgumentException("Illegal value of ACE type): " + value);
    }
}
