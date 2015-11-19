package org.dcache.gplazma.loader;

/**
 * Classes that implement this interface provide information about a set of
 * plugins as a {@link PluginRepository} object. The returned PluginRepository may
 * represent the plugins discovered as part of some auto-discovery process.
 */
public interface PluginRepositoryFactory {
    PluginRepository newRepository();
}
