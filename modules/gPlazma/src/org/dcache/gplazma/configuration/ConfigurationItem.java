package org.dcache.gplazma.configuration;

/**
 *
 * @author timur
 */
public class ConfigurationItem {
    private ConfigurationItemType type;
    private ConfigurationItemControl control;
    private String pluginName;
    private String pluginConfiguration;

    public ConfigurationItem(ConfigurationItemType type,
            ConfigurationItemControl control,
            String pluginName,
            String pluginConfiguration) {
        if(type == null) throw new NullPointerException("type is null");
        if(control == null) throw new NullPointerException("control is null");
        if(pluginName == null) throw new NullPointerException("pluginName is null");
        this.type = type;
        this.control = control;
        this.pluginName = pluginName;
        this.pluginConfiguration = pluginConfiguration;
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
    public String getPluginConfiguration() {
        return pluginConfiguration;
    }

    @Override
    public boolean equals(Object anObject)	{

        if( !(anObject instanceof ConfigurationItem)) {
            return false;
        }

        ConfigurationItem anItem = (ConfigurationItem) anObject;
        if( !type.equals(anItem.type) ||
            ! control.equals(anItem.control) ||
            !pluginName.equals(anItem.pluginName) ) {
            return false;
        }

        if(pluginConfiguration == null) {
            return anItem.pluginConfiguration == null;
        }

        return pluginConfiguration.equals( anItem.pluginConfiguration);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + type.hashCode();
        hash = 83 * hash + control.hashCode();
        hash = 83 * hash + pluginName.hashCode();
        hash = 83 * hash + (this.pluginConfiguration != null ? this.pluginConfiguration.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append(' ');
        sb.append(control).append(' ');
        sb.append(pluginName);
        if(pluginConfiguration != null) {
            sb.append(' ').append(pluginConfiguration);
        }
        return sb.toString();
    }


}
