package org.dcache.acl.enums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACE flags bit mask.
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public enum AceFlags {

    /**
     * Can be placed on a directory and indicates that this ACE should be
     * added to each new non-directory file created.
     */
    FILE_INHERIT_ACE(0x00000001, 'f'),

    /**
     * Can be placed on a directory and indicates that this ACE should be
     * added to each new directory created.
     */
    DIRECTORY_INHERIT_ACE(0x00000002, 'd'),

    /**
     * Can be placed on a directory but does not apply to the directory;
     * ALLOW and DENY ACEs with this bit set do not affect access to the directory,
     * and AUDIT and ALARM ACEs with this bit set do not trigger log or alarm events.
     * Such ACEs only take effect once they are applied (with this bit cleared) to newly
     * created files and directories as specified by the above two flags.
     */
    INHERIT_ONLY_ACE_LEGACY(0x00000008, 'o'),
    INHERIT_ONLY_ACE(0x00000008, 'r'),

    /**
     * Indicates that the "who" refers to a GROUP.
     */
    IDENTIFIER_GROUP(0x00000040, 'g');

    // Logger
    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AceFlags.class);

    private final int _value;

    private final char _abbreviation;

    AceFlags(int value, char abbreviation) {
        _value = value;
        _abbreviation = abbreviation;
    }

    public int getValue() {
        return _value;
    }

    public char getAbbreviation() {
        return _abbreviation;
    }

    public boolean matches(int flags) {
        return (_value & flags) == _value;
    }

    /**
     * @param flags
     *            ACE flags bit mask
     * @return Return string representation of flags bit mask
     */
    public static String asString(int flags) {
        // no flags
        if ( flags == 0 ) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (AceFlags flag : AceFlags.values()) {
            if (flag.matches(flags)) {
                sb.append(flag.getAbbreviation());
            }
        }

        return sb.toString();
    }

    /**
     * Check for valid flags bit mask
     *
     * @param flags
     *            ACE flags bit mask
     * @param isDir
     *            true if resource is a directory
     * @return valid flags bit mask
     */
    public static int validate(int flags, boolean isDir) {
        int res = 0;
        if ( isDir ) {
            if ( FILE_INHERIT_ACE.matches(flags) ) {
                res = FILE_INHERIT_ACE._value;
            }

            if ( DIRECTORY_INHERIT_ACE.matches(flags) ) {
                res += DIRECTORY_INHERIT_ACE._value;
            }

            if ( INHERIT_ONLY_ACE.matches(flags) ) {
                if ( res == 0 ) {
                    logger.warn("Unsupported flags of directory: " + flags);
                } else {
                    res += INHERIT_ONLY_ACE._value;
                }
            }

        } else if ( flags != 0 && flags != IDENTIFIER_GROUP._value ) {
            logger.warn("Unsupported flags of file: " + flags);
        }

        if ( IDENTIFIER_GROUP.matches(flags) ) {
            res += IDENTIFIER_GROUP._value;
        }

        return res;
    }

    /**
     * @param strFlags
     *            String representation of the flags
     * @return flags
     * @throws IllegalArgumentException
     */
    public static int parseInt(String strFlags) throws IllegalArgumentException {
        if ( strFlags == null || strFlags.length() == 0 ) {
            throw new IllegalArgumentException("Ace flags string is " + (strFlags == null ? "NULL" : "Empty"));
        }

        int mask = 0;
        char[] chars = strFlags.toCharArray();
        for (char ch : chars) {
            mask |= fromAbbreviation(ch)._value;
        }

        return mask;
    }

    /**
     * @param abbreviation
     *            of AceFlag
     * @return AceFlag
     * @throws IllegalArgumentException
     */
    public static AceFlags fromAbbreviation(char abbreviation) throws IllegalArgumentException {
        switch (abbreviation) {
        case 'f':
            return FILE_INHERIT_ACE;
        case 'd':
            return DIRECTORY_INHERIT_ACE;
        case 'o':
        case 'i':
            return INHERIT_ONLY_ACE;
        case 'g':
            return IDENTIFIER_GROUP;
        default:
            throw new IllegalArgumentException("Invalid ace flag abbreviation: " + abbreviation);
        }
    }
}
