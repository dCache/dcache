package org.dcache.missingfiles.plugins;

import org.dcache.util.ConfigurationProperties;


/**
 *  Factory class for the SEMsg plugin
 */
public class SEMsgPluginFactory implements PluginFactory
{
    @Override
    public String getName()
    {
        return "semsg";
    }

    @Override
    public Plugin createPlugin(ConfigurationProperties properties)
    {
        return new SEMsgPlugin(properties);
    }
}
