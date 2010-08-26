package org.dcache.acl.unix;

/**
 * ACE tags bit mask.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum AceTag {

    /**
     * undefined tag
     */
    UNDEFINED_TAG(0x00000000, ""),

    /**
     * default tag
     */
    DEFAULT(0x00001000, "default"),

    /**
     * object owner
     */
    USER_OBJ(0x00000001, "user"),

    /**
     * owning group of the object
     */
    GROUP_OBJ(0x00000004, "group"),

    /**
     * other entry for the object
     */
    OTHER_OBJ(0x00000020, "other");

    private static final String SEPARATOR = ":";

    private final int _value;

    private final String _abbreviation;

    AceTag(int value, String abbreviation) {
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

    public boolean matches(int tags) {
        return (_value & tags) == _value;
    }

    /**
     * @param tags
     *            ACE Unix tags bit mask
     * @return Return string representaion of tags bit mask
     */
    public static String toString(int tags) throws IllegalArgumentException {
        StringBuilder sb = new StringBuilder();
        for (AceTag tag : AceTag.values())
            if ( tag.matches(tags) ) {
                if ( sb.length() != 0 )
                    sb.append(SEPARATOR);
                sb.append(tag.getAbbreviation());
            }

        return sb.toString();
    }

}
