package org.dcache.webadmin.model.util;

/**
 * An utility version of Retention Policy to easily convert Strings and Policies
 * back and forth
 */
public enum RetentionPolicy {

    REPLICA("REPLICA", "R"), OUTPUT("OUTPUT", "P"), CUSTODIAL("CUSTODIAL", "C");
    private final String _name;
    private final String _shortcut;

    RetentionPolicy(String name, String shortcut) {
        _name = name;
        _shortcut = shortcut;
    }

    public String getName() {
        return _name;
    }

    public String getShortcut() {
        return _shortcut;
    }

    /**
     * Look up the RetentionPolicy that matches given String
     *
     * @param name
     * @return the corresponding RetentionPolicy, if valid, null otherwise.
     */
    public static RetentionPolicy parseStringValue(String name) {
        for (RetentionPolicy policy : RetentionPolicy.values()) {
            if (policy.getName().equals(name)) {
                return policy;
            }
        }
        return null;
    }
}
