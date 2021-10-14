package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides support for the MAP phase of logging in.  It tries the first plugin.  For
 * each plugin, it either tries the following plugin (if one is available) or returns depending on
 * the plugin's result and the configured control (OPTIONAL, REQUIRED, etc).
 */
public class DefaultMappingStrategy implements MappingStrategy {

    private static final Logger logger =
          LoggerFactory.getLogger(DefaultMappingStrategy.class);

    private volatile PAMStyleStrategy<GPlazmaMappingPlugin> pamStyleMappingStrategy;

    @Override
    public void setPlugins(List<GPlazmaPluginService<GPlazmaMappingPlugin>> plugins) {
        pamStyleMappingStrategy = new PAMStyleStrategy<>(plugins);
    }

    /**
     * Delegates execution of the {@link GPlazmaMappingPlugin#map(Set<Principal>)
     * GPlazmaMappingPlugin.map} methods of the plugins supplied by {@link
     * GPlazmaStrategy#setPlugins(List< GPlazmaPluginService <T>>) GPlazmaStrategy.setPlugins} to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the {@link PluginCaller#call(GPlazmaPlugin)
     * PluginCaller} interface.
     *
     * @param monitor
     * @param principals
     * @throws AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    @Override
    public void map(final LoginMonitor monitor,
          final Set<Principal> principals)
          throws AuthenticationException {
        pamStyleMappingStrategy.callPlugins(service -> {
            monitor.mapPluginBegins(service.getName(), service.getControl(), principals);

            GPlazmaMappingPlugin plugin = service.getPlugin();

            Result result = Result.FAIL;
            String error = null;
            try {
                plugin.map(principals);
                result = Result.SUCCESS;
            } catch (AuthenticationException e) {
                error = e.getMessage();
                throw e;
            } finally {
                monitor.mapPluginEnds(service.getName(), service.getControl(), result,
                      error, principals);
            }
        });
    }
}
