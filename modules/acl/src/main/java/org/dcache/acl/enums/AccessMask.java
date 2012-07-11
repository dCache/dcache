package org.dcache.acl.enums;

/**
 * This object consists of a enumeration (implemented as bit mask) of possible
 * access permissions.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum AccessMask {
    /**
     * Permission to read the data of the file.
     */
    READ_DATA(0x00000001, 'r', RsType.FILE), // 0000 0000 0000 0001

    /**
     * Permission to list the contents of a directory.
     */
    LIST_DIRECTORY(0x00000001, 'l', RsType.DIR), // 0000 0000 0000 0001

    /**
     * Permission to modify a file's data anywhere in the file's offset range.
     */
    WRITE_DATA(0x00000002, 'w', RsType.FILE), // 0000 0000 0000 0010

    /**
     * Permission to add a new file in a directory.
     */
    ADD_FILE(0x00000002, 'f', RsType.DIR), // 0000 0000 0000 0010

    /**
     * The ability to modify a file's data, but only starting at EOF.
     */
    APPEND_DATA(0x00000004, 'a', RsType.FILE), // 0000 0000 0000 0100

    /**
     * Permission to create a sub directory in a directory.
     */
    ADD_SUBDIRECTORY(0x00000004, 's', RsType.DIR), // 0000 0000 0000 0100

    /**
     * Permission to read the named attributes of a file or to lookup the named
     * attributes directory.
     */
    READ_NAMED_ATTRS(0x00000008, 'n'), // 0000 0000 0000 1000

    /**
     * Permission to write the named attributes of a file or to create a named
     * attribute directory.
     */
    WRITE_NAMED_ATTRS(0x00000010, 'N'), // 0000 0000 0001 0000

    /**
     * Permission to execute a file or traverse/search a directory.
     */
    EXECUTE(0x00000020, 'x'), // 0000 0000 0010 0000

    /**
     * Permission to delete a file or directory within a directory.
     */
    DELETE_CHILD(0x00000040, 'D'), // 0000 0000 0100 0000

    /**
     * The ability to read basic attributes (non-ACLs) of a file.
     */
    READ_ATTRIBUTES(0x00000080, 't'), // 0000 0000 1000 0000

    /**
     * Permission to change the times associated with a file or directory to an
     * arbitrary value.
     */
    WRITE_ATTRIBUTES(0x00000100, 'T'), // 0000 0001 0000 0000

    /**
     * Permission to delete the file or directory.
     */
    DELETE(0x00010000, 'd'), // 0001 0000 0000 0000 0000

    /**
     * Permission to read the ACL.
     */
    READ_ACL(0x00020000, 'c'), // 0010 0000 0000 0000 0000

    /**
     * Permission to write the ACL and mode attributes.
     */
    WRITE_ACL(0x00040000, 'C'), // 0100 0000 0000 0000 0000

    /**
     * Permission to write the owner and owner_group attributes.
     */
    WRITE_OWNER(0x00080000, 'o'), // 1000 0000 0000 0000 0000

    /**
     * Permission to write the owner and owner_group attributes.
     */
    SYNCHRONIZE(0x00100000, 'y'); // 0001 0000 0000 0000 0000 0000

    // ALL ALLOWED: 11111111111111111111 = FFFFF = 1048575

    private final int _value;

    private final char _abbreviation;

    private final RsType _type;

    private AccessMask(int value, char abbreviation) {
        _value = value;
        _abbreviation = abbreviation;
        _type = null;
    }

    AccessMask(int value, char abbreviation, RsType type) {
        _value = value;
        _abbreviation = abbreviation;
        _type = type;
    }

    public int getValue() {
        return _value;
    }

    public char getAbbreviation() {
        return _abbreviation;
    }

    public RsType getType() {
        return _type;
    }

    public boolean matches(int accessMask) {
        return (_value & accessMask) == _value;
    }

    /**
     * @param accessMask
     *            ACE access bit mask
     * @return Return string representation of access bit mask
     */
    public static String asString(int accessMask) throws IllegalArgumentException {
        StringBuilder sb = new StringBuilder();
        for (AccessMask accessMsk : AccessMask.values()) {
            if (accessMsk.matches(accessMask)) {
                sb.append(accessMsk.getAbbreviation());
            }
        }

        return sb.toString();
    }

    /**
     * @param accessMask
     *            ACE access bit mask
     * @param type
     *            Type of resource
     * @return Return string representation of access bit mask
     */
    public static String asString(int accessMask, RsType type) throws IllegalArgumentException {
        StringBuilder sb = new StringBuilder();
        for (AccessMask accessMsk : AccessMask.values()) {
            if ((accessMsk._type == null || type == null || accessMsk._type == type) && accessMsk
                    .matches(accessMask)) {
                sb.append(accessMsk.getAbbreviation());
            }
        }

        return sb.toString();
    }

    /**
     * @param strAccessMask
     *            String representation of the accessMask
     * @return accessMask
     * @throws IllegalArgumentException
     */
    public static int parseInt(String strAccessMask) throws IllegalArgumentException {
        if ( strAccessMask == null || strAccessMask.length() == 0 ) {
            throw new IllegalArgumentException("accessMask is " + (strAccessMask == null ? "NULL" : "Empty"));
        }

        int mask = 0;
        char[] chars = strAccessMask.toCharArray();
        for (char ch : chars) {
            mask |= fromAbbreviation(ch)._value;
        }

        return mask;
    }

    /**
     * @param abbreviation
     *            of the AccessMask
     * @return AccessMask
     * @throws IllegalArgumentException
     */
    public static AccessMask fromAbbreviation(char abbreviation) throws IllegalArgumentException {
        switch (abbreviation) {
        case 'r':
            return READ_DATA;
        case 'l':
            return LIST_DIRECTORY;
        case 'w':
            return WRITE_DATA;
        case 'f':
            return ADD_FILE;
        case 'a':
            return APPEND_DATA;
        case 's':
            return ADD_SUBDIRECTORY;
        case 'n':
            return READ_NAMED_ATTRS;
        case 'N':
            return WRITE_NAMED_ATTRS;
        case 'x':
            return EXECUTE;
        case 'D':
            return DELETE_CHILD;
        case 't':
            return READ_ATTRIBUTES;
        case 'T':
            return WRITE_ATTRIBUTES;
        case 'd':
            return DELETE;
        case 'c':
            return READ_ACL;
        case 'C':
            return WRITE_ACL;
        case 'o':
            return WRITE_OWNER;
        case 'y':
            return SYNCHRONIZE;
        default:
            throw new IllegalArgumentException("Invalid access mask abbreviation: " + abbreviation);
        }
    }

}
