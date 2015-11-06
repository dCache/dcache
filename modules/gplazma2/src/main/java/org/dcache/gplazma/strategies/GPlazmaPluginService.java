package org.dcache.gplazma.strategies;

import com.google.common.util.concurrent.AbstractService;

import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.plugins.GPlazmaPlugin;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class wraps a GPlazmaPlugin instance and controls the lifecycle calls to the
 * plugin.
 */
public class GPlazmaPluginService<T extends GPlazmaPlugin> extends AbstractService
{
    private final T _plugin;
    private final ConfigurationItemControl _control;
    private final String _name;

    public GPlazmaPluginService(T plugin, String name, ConfigurationItemControl control)
    {
        checkNotNull(plugin, "plugin is null");
        checkNotNull(control, "control is null");
        checkNotNull(name, "name is null");

        _plugin = plugin;
        _control = control;
        _name = name;
    }

    @Override
    protected void doStart()
    {
        try {
            _plugin.start();
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop()
    {
        try {
            _plugin.stop();
            notifyStopped();
        } catch (Exception e) {
            notifyFailed(e);
        }
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

        GPlazmaPluginService<?> aPluginElement =
            (GPlazmaPluginService<?>) anObject;
        return (_plugin.equals(aPluginElement._plugin) &&
                _control.equals(aPluginElement._control));
    }

    @Override
    public int hashCode()
    {
        return _plugin.hashCode() ^ _control.hashCode();
    }
}
