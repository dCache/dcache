package org.dcache.gplazma.loader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * PluginRepository is a container class that provides information about zero
 * or more plugins. For each plugin, a {@link PluginMetadata} object is
 * stored, which stores the static metadata for this plugin.
 * <p>
 * The class allows the discovery of the number of stored plugins, discovery
 * of a plugin by name and support an arbitrary processing of stored plugins
 * via the PluginMetadataProcessor interface.
 */
public class PluginRepository {

    private final Set<PluginMetadata> _plugins = new HashSet<PluginMetadata>();

    private final Map<String, PluginMetadata> _pluginByName =
            new HashMap<String, PluginMetadata>();

    public void addPlugin( PluginMetadata metadata) {
        _plugins.add( metadata);
        for( String name : metadata.getPluginNames()) {
            _pluginByName.put( name, metadata);
        }
    }

    public boolean hasPluginWithName( String name) {
        return _pluginByName.containsKey( name);
    }

    public PluginMetadata getPlugin( String name) {
        PluginMetadata metadata = _pluginByName.get( name);

        if( metadata == null) {
            throw new IllegalArgumentException( "No plugin with name " + name);
        }

        return metadata;
    }

    public void processPluginsWith( PluginMetadataProcessor processor) {
        for( PluginMetadata plugin : _plugins) {
            processor.process( plugin);
        }
    }

    public int size() {
        return _plugins.size();
    }

    /**
     * Classes that implement this method accept each stored PluginMetadata,
     * one at a time, allowing arbitrary processing of that plugin's static
     * metadata.
     */
    public static interface PluginMetadataProcessor {
        public void process( PluginMetadata plugin);
    }
}
