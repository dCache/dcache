package org.dcache.gplazma.strategies;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.dcache.gplazma.configuration.ConfigurationItemControl;

/**
 *
 * @author timur
 */
public class GPlazmaPluginElement<T extends GPlazmaPlugin> {
    private T plugin;
    private ConfigurationItemControl control;


    public GPlazmaPluginElement(
            T plugin,
            ConfigurationItemControl control) {
        if(plugin  == null) {
            throw new NullPointerException("plugin is null");
        }
        if(control  == null) {
            throw new NullPointerException("control is null");
        }
        this.plugin = plugin;
        this.control = control;
    }

    /**
     * @return the plugin
     */
    public T getPlugin() {
        return plugin;
    }

    /**
     * @return the control
     */
    public ConfigurationItemControl getControl() {
        return control;
    }

    @Override
    public String toString() {
        return "GPlazmaPluginElement["+plugin+","+control+"]";
    }

    @Override
    public boolean equals(Object anObject) {
        if(anObject == null) {
            return false;
        }

        if( getClass()  != anObject.getClass() ) {
			return false;
        }

		GPlazmaPluginElement<T> aPluginElement = (GPlazmaPluginElement<T>) anObject;
        if( ! plugin.equals(aPluginElement.plugin) ||
            ! control.equals(aPluginElement.control)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return plugin.hashCode() ^ control.hashCode();
    }

}
