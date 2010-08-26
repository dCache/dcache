package org.dcache.acl.unix;

/**
 * Unix Access Mask.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public enum AMUnix {

    /**
     * Permission to read.
     */
    READ(0x00000004, 'r'),

    /**
     * Permission to write.
     */
    WRITE(0x00000002, 'w'),

    /**
     * Permission to execute.
     */
    EXECUTE(0x00000001, 'x');

    // /**
    // *
    // */
    // ADD (0x00000008, 'a'),

    // /**
    // *
    // */
    // DELETE (0x00000010, 'd');

    private final int _value;

    private final char _abbreviation;

    AMUnix(int value, char abbreviation) {
        _value = value;
        _abbreviation = abbreviation;
    }

    public int getValue() {
        return _value;
    }

    public char getAbbreviation() {
        return _abbreviation;
    }

    public boolean matches(int accessMask) {
        return (_value & accessMask) == _value;
    }

    /**
     * @param accessMask
     *            ACE access bit mask
     * @return Return string representaion of access bit mask
     */
    public static String toUnixString(int accessMask) {
        if ( accessMask == 0 )
            return "---";

        StringBuilder sb = new StringBuilder();
        for (AMUnix accessMsk : AMUnix.values())
            if ( accessMsk.matches(accessMask) )
                sb.append(accessMsk.getAbbreviation());

        return sb.toString();
    }

    /**
     * @param accessMask
     *            ACE access bit mask
     * @return Return string representaion of access bit mask
     */
    public static String toString(int accessMask) {
        StringBuilder sb = new StringBuilder();
        if ( accessMask != 0 ) {
            for (AMUnix accessMsk : AMUnix.values())
                if ( accessMsk.matches(accessMask) )
                    sb.append(accessMsk.getAbbreviation());
                else
                    sb.append("-");
        } else
            sb.append("---");

        return sb.toString();
    }
}
