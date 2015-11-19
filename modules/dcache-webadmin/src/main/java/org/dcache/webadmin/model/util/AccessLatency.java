package org.dcache.webadmin.model.util;

/**
 * An utility version of Access Latency to easily convert Strings and Latencies
 * back and forth
 */
public enum AccessLatency {

    ONLINE("ONLINE", "O"), NEARLINE("NEARLINE", "N"), OFFLINE("OFFLINE", "F");
    private final String _name;
    private final String _shortcut;

    AccessLatency(String name, String shortcut) {
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
     * Look up the AccessLatency that matches given String
     *
     * @param name
     * @return the corresponding AccessLatency, if valid, null otherwise.
     */
    public static AccessLatency parseStringValue(String name) {
        for (AccessLatency accessLatency : AccessLatency.values()) {
            if (accessLatency.getName().equals(name)) {
                return accessLatency;
            }
        }
        return null;
    }
}
