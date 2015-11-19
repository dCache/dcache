package org.dcache.gplazma.loader;

import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * Classes that implement the PluginLoader Interface provide the ability to
 * obtain GPlazmaPlugin objects from some set of possible plugin types. The
 * set of available plugins types is under the control of the implementing
 * class. The set may be fixed or it may be discoverable at run-time.
 * <p>
 * The PluginLoader is responsible for creating the plugin object. The
 * {@link newPluginByName} methods are available. There are two versions of
 * this method: with and without arguments.
 * <p>
 * Classes implementing PluginLoad may require initialisation. To support
 * this, the {@link init} method must be called before any call to
 * {@link newPluginByName}.
 */
public interface PluginLoader
{
    /**
     * Sets the factory used to generate a plugin.  The PluginFactory will have
     * a default factory.  This method must only be called before init.
     */
    void setPluginFactory(PluginFactory factory);


    /**
     * Calling init method instructs the PluginLoader to initialise any
     * resource. It must be called precisely once and before calling any
     * other method of the PluginLoader.
     */
    void init();

    /**
     * Obtain a new GPlazmaPlugin object corresponding to the supplied plugin
     * name. The semantics of plugin name is implementation-specific and
     * should be documented by the implementing class.
     * <p>
     * The new plugin is created and supplied with an empty array of String
     * values. This requires the plugin to have a constructor that accepts an
     * array of strings. If the plugin has no such constructor then
     * IllegalArgumentException is thrown.
     * <p>
     * Successive calls to this method with the same plugin name will return
     * different objects.
     *
     * @param name the name of the plugin to create
     * @return an instance of the plugin
     * @throws IllegalStateException if {@link #init} has not been called.
     * @throws PluginLoadingException if the name is unknown or the
     *             corresponding plugin cannot be created.
     */
    GPlazmaPlugin newPluginByName(String name)
            throws PluginLoadingException;

    /**
     * Obtain a new GPlazmaPlugin object corresponding to the supplied plugin
     * name. The semantics of plugin name is implementation-specific and
     * should be documented by the implementing class.
     * <p>
     * The new plugin is created and supplied with the arguments as an array
     * of String values. This requires the plugin to have a constructor that
     * accepts an array of strings. If the plugin has no such constructor
     * then IllegalArgumentException is thrown.
     * <p>
     * Successive calls to this method with the same plugin name will return
     * different objects.
     *
     * @param name the name of the plugin to create
     * @param properties an array of Strings to adjust the plugin's behaviour
     * @return an instance of the plugin
     * @throws IllegalStateException if {@link #init} has not been called.
     * @throws PluginLoadingException if the name is unknown or the
     *             corresponding plugin cannot be created.
     */
    GPlazmaPlugin newPluginByName(String name, Properties properties)
            throws PluginLoadingException;
}
