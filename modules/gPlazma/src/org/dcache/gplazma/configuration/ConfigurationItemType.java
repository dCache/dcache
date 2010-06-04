package org.dcache.gplazma.configuration;

/**
 *
 * @author timur
 */
public enum  ConfigurationItemType {
    AUTHENTICATION("auth"),
    MAPPING("map"),
    ACCOUNT("account"),
    SESSION("session");

    private final String name;
    private ConfigurationItemType(String name) {
        this.name = name;
    }
    /** this package visible method is used to restore the State from
     * the database
     */
    public static ConfigurationItemType getConfigurationItemType(String name)
            throws IllegalArgumentException {
        if(name == null) {
            throw new NullPointerException(" null name ");
        }

        for(ConfigurationItemType aConfigurationItemType: values()) {
            if(aConfigurationItemType.name.equalsIgnoreCase(name)) {
                return aConfigurationItemType;
            }
        }
        throw new IllegalArgumentException("Unknown Name ConfigurationItemType:"+name);
    }

    @Override
    public String toString() {
        return name;
    }
}
