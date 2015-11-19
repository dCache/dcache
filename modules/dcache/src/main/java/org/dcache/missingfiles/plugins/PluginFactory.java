package org.dcache.missingfiles.plugins;

import org.dcache.util.ConfigurationProperties;


/**
 *  All plugins must also implement a PluginFactory, which must be registered
 *  for the ServiceLoader to discover it.
 */
public interface PluginFactory
{
    /**
     *  The name of the plugin, as used by the dCache adminstrator to
     *  configure the list of active plugins.
     */
    String getName();


    /**
     *  Create a new instance of the plugin.  The environment parameter is a
     *  collection of keyword-value pairs taken from the dCache
     *  configuration.
     */
    Plugin createPlugin(ConfigurationProperties properties);

}
