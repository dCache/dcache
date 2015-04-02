package org.dcache.acl.enums;

/**
 * The AceType is an enumeration (to be implemented as a bit mask).
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum AceType {
    /**
     * Explicitly grants the access
     */
    ACCESS_ALLOWED_ACE_TYPE(0x00000000, 'A'),

    /**
     * Explicitly denies the access
     */
    ACCESS_DENIED_ACE_TYPE(0x00000001, 'D');

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

    public String toString() {
        return String.valueOf(_abbreviation);
    }

    public static AceType fromAbbreviation(char abbreviation) throws IllegalArgumentException {
        if ( ACCESS_ALLOWED_ACE_TYPE.equalsIgnoreCase(abbreviation) ) {
            return ACCESS_ALLOWED_ACE_TYPE;
        } else if ( ACCESS_DENIED_ACE_TYPE.equalsIgnoreCase(abbreviation) ) {
            return ACCESS_DENIED_ACE_TYPE;
        } else {
            throw new IllegalArgumentException("Invalid ACE type abbreviation: " + abbreviation);
        }
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
        for (AceType type : AceType.values()) {
            if (type._value == value) {
                return type;
            }
        }

        throw new IllegalArgumentException("Illegal value of ACE type): " + value);
    }
}
