package org.dcache.gplazma.configuration;

import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

/**
 *
 * @author timur
 */
public enum  ConfigurationItemType {
    AUTHENTICATION("auth",GPlazmaAuthenticationPlugin.class),
    MAPPING("map",GPlazmaMappingPlugin.class),
    ACCOUNT("account",GPlazmaAccountPlugin.class),
    SESSION("session",GPlazmaSessionPlugin.class),
    IDENTITY("identity",GPlazmaIdentityPlugin.class);

    private final String name;
    private final Class <? extends GPlazmaPlugin> type;

    ConfigurationItemType(String name, Class<? extends GPlazmaPlugin> type) {
        this.name = name;
        this.type = type;
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

    /**
     * @return the type
     */
    public Class<? extends GPlazmaPlugin> getType() {
        return type;
    }
}
