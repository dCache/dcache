package org.dcache.gplazma.strategies;

import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.plugins.GPlazmaPlugin;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class holds the information from a PAM-style configuration line.
 */
public class GPlazmaPluginElement<T extends GPlazmaPlugin>
{
    private final T _plugin;
    private final ConfigurationItemControl _control;
    private final String _name;

    public GPlazmaPluginElement(T plugin, String name,
            ConfigurationItemControl control)
    {
        checkNotNull(plugin, "plugin is null");
        checkNotNull(control, "control is null");
        checkNotNull(name, "name is null");

        _plugin = plugin;
        _control = control;
        _name = name;
    }

    /**
     * @return the plugin
     */
    public T getPlugin()
    {
        return _plugin;
    }

    /**
     * Obtain the name used to configure which plugin to load.
     */
    public String getName()
    {
        return _name;
    }

    /**
     * @return the control
     */
    public ConfigurationItemControl getControl()
    {
        return _control;
    }

    @Override
    public String toString()
    {
        return "GPlazmaPluginElement["+_plugin+","+_control+"]";
    }

    @Override
    public boolean equals(Object anObject)
    {
        if (anObject == null) {
            return false;
        }

        if (getClass().equals(anObject.getClass())) {
            return false;
        }

        GPlazmaPluginElement<?> aPluginElement =
            (GPlazmaPluginElement<?>) anObject;
        return (_plugin.equals(aPluginElement._plugin) &&
                _control.equals(aPluginElement._control));
    }

    @Override
    public int hashCode()
    {
        return _plugin.hashCode() ^ _control.hashCode();
    }
}
