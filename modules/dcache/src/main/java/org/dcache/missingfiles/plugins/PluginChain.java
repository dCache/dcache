package org.dcache.missingfiles.plugins;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

import dmg.cells.nucleus.EnvironmentAware;

import org.dcache.commons.util.NDC;
import org.dcache.util.ConfigurationProperties;

import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.find;

/**
 *  This class represents one or more plugins that are used in concert to
 *  process missing file notification.
 */
public class PluginChain implements EnvironmentAware
{
    private static final Logger _log = LoggerFactory.getLogger(PluginChain.class);

    private static final ServiceLoader<PluginFactory> _factories =
            ServiceLoader.load(PluginFactory.class);

    private final List<PluginInstance> _plugins = new ArrayList<>();

    private String _pluginList;

    private final ConfigurationProperties _properties =
            new ConfigurationProperties();


    /**
     *  Accept a comma-separated list of plugin names.  These will be
     *  instantiated to form the chain of plugins to process.
     */
    @Required
    public void setPluginList(String plugins)
    {
        _pluginList = plugins;
    }


    public void init()
    {
        _plugins.clear();

        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();

        for(String name : splitter.split(_pluginList)) {
            NDC ndc = NDC.cloneNdc();
            NDC.push(name);
            try {
                createPlugin(name);
            } finally {
                NDC.set(ndc);
            }
        }
    }

    private void createPlugin(String name)
    {
        try {
            PluginFactory factory = find(_factories, compose(equalTo(name),
                                                             PluginFactory::getName));
            Plugin plugin = factory.createPlugin(_properties);
            PluginInstance pi = new PluginInstance(name, plugin);
            _plugins.add(pi);
        } catch(NoSuchElementException e) {
            _log.error("Unknown plugin");
        } catch(RuntimeException e) {
            _log.error("Failed to instantiate plugin: {}", e.getMessage());
        }
    }


    @Required
    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _properties.clear();

        for(Map.Entry<String,Object> entry : environment.entrySet()) {
            Object value = entry.getValue();
            if (value != null) {
                _properties.put(entry.getKey(), value.toString());
            }
        }
    }


    public void accept(PluginVisitor visitor)
    {
        for(PluginInstance pi : _plugins) {
            NDC ndc = NDC.cloneNdc();
            NDC.push(pi.getName());
            try {
                if(!visitor.visit(pi.getPlugin())) {
                    break;
                }
            } finally {
                NDC.set(ndc);
            }
        }
    }



    /**
     *  Simple class to hold metadata about an instantiated plugin.
     */
    private static class PluginInstance
    {
        private final Plugin _plugin;
        private final String _name;

        PluginInstance(String name, Plugin plugin)
        {
            _plugin = plugin;
            _name = name;
        }

        public String getName()
        {
            return _name;
        }

        public Plugin getPlugin()
        {
            return _plugin;
        }
    }
}
