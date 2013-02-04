package org.dcache.gplazma.configuration;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Properties;

/**
 *
 * @author timur
 */
public class ConfigurationItem {
    private final ConfigurationItemType type;
    private final ConfigurationItemControl control;
    private final String pluginName;
    private final Properties pluginProperties;

    public ConfigurationItem(ConfigurationItemType type,
            ConfigurationItemControl control,
            String pluginName,
            Properties pluginProperties) {

        this.type = checkNotNull(type, "type is null");
        this.control = checkNotNull(control, "control is null");
        this.pluginName = checkNotNull(pluginName, "pluginName is null");
        this.pluginProperties = checkNotNull(pluginProperties, "pluginProperties is null");
    }

    /**
     * @return the type
     */
    public ConfigurationItemType getType() {
        return type;
    }

    /**
     * @return the control
     */
    public ConfigurationItemControl getControl() {
        return control;
    }

    /**
     * @return the pluginName
     */
    public String getPluginName() {
        return pluginName;
    }

    /**
     * @return the pluginConfiguration
     */
    public Properties getPluginConfiguration() {
        return pluginProperties;
    }

    @Override
    public boolean equals(Object anObject)	{

        if( !(anObject instanceof ConfigurationItem)) {
            return false;
        }

        ConfigurationItem anItem = (ConfigurationItem) anObject;
        if( !type.equals(anItem.type) ||
            !control.equals(anItem.control) ||
            !pluginName.equals(anItem.pluginName) ) {
            return false;
        }

        return Objects.equal(pluginProperties, anItem.pluginProperties);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + type.hashCode();
        hash = 83 * hash + control.hashCode();
        hash = 83 * hash + pluginName.hashCode();
        hash = 83 * hash + pluginProperties.hashCode();
        return hash;
    }

    @Override
    public String toString() {
        return type + " " + control + " " + pluginName + " " + pluginProperties;
    }


}
