package org.dcache.util;

public class Booleans {

    private static final String[] trueStrings = {"true", "yes", "on", "enable"};
    private static final String[] falseStrings = {"false", "no", "off", "disable"};

    private Booleans() {
    }

    /**
     * Convert boolean value into Strings "yes" or "no"
     * @param value
     * @return "yes" if <code>value</code> is <code>true</code> and "no" otherwise.
     */
    public static String toYesNoString(boolean value) {
        return toPredefinedString(value, "yes", "no");
    }

    /**
     * Convert boolean value into Strings "on" or "off"
     * @param value
     * @return "on" if <code>value</code> is <code>true</code> and "off" otherwise.
     */
    public static String toOnOffString(boolean value) {
        return toPredefinedString(value, "on", "off");
    }

    /**
     * Convert boolean value into Strings "true" or "false"
     * @param value
     * @return "true" if <code>value</code> is <code>true</code> and "false" otherwise.
     */
    public static String toTrueFalseString(boolean value) {
        return toPredefinedString(value, "true", "false");
    }

        /**
     * Convert boolean value into Strings "enable" or "disable"
     * @param value
     * @return "enable" if <code>value</code> is <code>true</code> and "disable" otherwise.
     */
    public static String toEnableDisableString(boolean value) {
        return toPredefinedString(value, "enable", "disable");
    }

    /**
     * Convert boolean value into corresponding string.
     * @param value
     * @param ifTrue String representation for <code>true</code>
     * @param ifFalse String representation for <code>false</code>
     * @return value of <code>ifTrue</code> if value is <code>true</code> and ifFalse otherwise.
     */
    public static String toPredefinedString(boolean value, String ifTrue, String ifFalse) {
        return value ? ifTrue : ifFalse;
    }

    /**
     * Get boolean value of specified string.
     * @param s string to check
     * @throws IllegalArgumentException if provided string do not corresponds to
     * any of <code>true</code> or <code>false</code> string.
     * @return true if <code>s</code> has corresponding value.
     */
    public static boolean of(String s) {
        if (contains(trueStrings, s, true)) {
            return true;
        }
        if (contains(falseStrings, s, true)) {
            return false;
        }
        throw new IllegalArgumentException();
    }

    /**
     * Get boolean value of specified string. If provided string is <code>null</code>
     * then <code>defaultValue</code> is returned.
     * @param s string to check
     * @param defaultValue
     * @throws IllegalArgumentException if provided string do not corresponds to
     * any of <code>true</code> or <code>false</code> string.
     * @return true if <code>s</code> has corresponding value.
     */
    public static boolean of(String s, boolean defaultValue) {
        if( s == null) {
            return defaultValue;
        }

        if (contains(trueStrings, s, true)) {
            return true;
        }
        if (contains(falseStrings, s, true)) {
            return false;
        }
        throw new IllegalArgumentException(s + " is not in supported boolean identifier");
    }

    /**
     * Test the array.
     * @param array where to test
     * @param value to test
     * @param ignoreCase
     * @return <code>true</code> is <code>array</code> contains <code>value</code>
     * and <code>false</code> otherwise.
     */
    private static boolean contains(String[] array, String value, boolean ignoreCase) {
        for (String s : array) {
            if (ignoreCase && s.equalsIgnoreCase(value) || s.equals(value)) {
                return true;
            }
        }
        return false;
    }
}
