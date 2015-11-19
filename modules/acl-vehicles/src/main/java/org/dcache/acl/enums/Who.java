package org.dcache.acl.enums;

/**
 * The enumeration Who allows to identify different kind of subjects.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum Who {

    /**
     * The user identified by the virtual user ID.
     */
    USER(0x00000000, "USER"),

    /**
     * The group identified by the virtual group ID.
     */
    GROUP(0x00000001, "GROUP"),

    /**
     * The owner of resource.
     */
    OWNER(0x00000002, "OWNER@"),

    /**
     * The group associated with the owner of resource.
     */
    OWNER_GROUP(0x00000003, "GROUP@"),

    /**
     * The world, including the owner and owning group.
     */
    EVERYONE(0x00000004, "EVERYONE@"),

    /**
     * Accessed without any authentication.
     */
    ANONYMOUS(0x00000005, "ANONYMOUS@"),

    /**
     * Any authenticated user (opposite of ANONYMOUS).
     */
    AUTHENTICATED(0x00000006, "AUTHENTICATED@");

    private final int _value;

    private final String _abbreviation;

    Who(int value, String abbreviation) {
        _value = value;
        _abbreviation = abbreviation;
    }

    public int getValue() {
        return _value;
    }

    public String getAbbreviation() {
        return _abbreviation;
    }

    public boolean equals(int value) {
        return _value == value;
    }

    public boolean equals(String abbreviation) {
        return _abbreviation.equals(abbreviation);
    }

    public boolean equalsIgnoreCase(String abbreviation) {
        return _abbreviation.equalsIgnoreCase(abbreviation);
    }

    public static Who valueOf(int value) throws IllegalArgumentException {
        for (Who who : Who.values()) {
            if (who._value == value) {
                return who;
            }
        }

        throw new IllegalArgumentException("Illegal argument (value of who): " + value);
    }

    public static Who fromAbbreviation(String abbreviation) throws IllegalArgumentException {
        if ( abbreviation == null || abbreviation.length() == 0 ) {
            throw new IllegalArgumentException("Who abbreviation is " + (abbreviation == null ? "NULL" : "Empty"));
        }

        if ( USER.equalsIgnoreCase(abbreviation) ) {
            return USER;
        } else if ( GROUP.equalsIgnoreCase(abbreviation) ) {
            return GROUP;
        } else if ( OWNER.equalsIgnoreCase(abbreviation) ) {
            return OWNER;
        } else if ( OWNER_GROUP.equalsIgnoreCase(abbreviation) ) {
            return OWNER_GROUP;
        } else if ( EVERYONE.equalsIgnoreCase(abbreviation) ) {
            return EVERYONE;
        } else if ( ANONYMOUS.equalsIgnoreCase(abbreviation) ) {
            return ANONYMOUS;
        } else if ( AUTHENTICATED.equalsIgnoreCase(abbreviation) ) {
            return AUTHENTICATED;
        } else if ( abbreviation.endsWith("@") ) {
            throw new IllegalArgumentException("Invalid who abbreviation: " + abbreviation);
        }

        return null;
    }

    /**
     * @return Returns true is who is a special subject
     */
    public boolean isSpecial() {
        return (this != USER && this != GROUP);
    }
}
