package org.dcache.gplazma.strategies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

/**
 * This class provides support for the SESSION phase of logging in.  It tries
 * the first plugin.  For each plugin, it either tries the following plugin (if
 * one is available) or returns depending on the plugin's result and the
 * configured control (OPTIONAL, REQUIRED, etc).
 */
public class DefaultSessionStrategy implements SessionStrategy
{
    private static final Logger logger =
            LoggerFactory.getLogger(DefaultSessionStrategy.class);

    private volatile PAMStyleStrategy<GPlazmaSessionPlugin> pamStyleSessionStrategy;

    /**
     *
     * @param plugins
     */
    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaSessionPlugin>> plugins)
    {
        pamStyleSessionStrategy = new PAMStyleStrategy<>(plugins);
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaSessionPlugin#session(SessionID, Set<Principal>,Set<Object>) GPlazmaSessionPlugin.session}
     * methods of the plugins supplied by
     * {@link GPlazmaStrategy#setPlugins(List<GPlazmaPluginElement<T>>) GPlazmaStrategy.setPlugins}
     *  to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the
     * {@link PluginCaller#call(GPlazmaPlugin) PluginCaller}
     * interface.
     *
     * @param sessionID
     * @param authorizedPrincipals
     * @param attrib
     * @throws AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    @Override
    public void session(final LoginMonitor monitor,
            final Set<Principal> authorizedPrincipals,
            final Set<Object> attrib) throws AuthenticationException
    {
        pamStyleSessionStrategy.callPlugins( new PluginCaller<GPlazmaSessionPlugin>()
        {
            @Override
            public void call(GPlazmaPluginElement<GPlazmaSessionPlugin> pe)
                    throws AuthenticationException
            {
                monitor.sessionPluginBegins(pe.getName(), pe.getControl(),
                        authorizedPrincipals, attrib);

                GPlazmaSessionPlugin plugin = pe.getPlugin();
                String error = null;
                Result result = Result.FAIL;
                try {
                    plugin.session(authorizedPrincipals, attrib);
                    result = Result.SUCCESS;
                } catch(AuthenticationException e) {
                    error = e.getMessage();
                    throw e;
                } finally {
                    monitor.sessionPluginEnds(pe.getName(), pe.getControl(),
                            result, error, authorizedPrincipals, attrib);
                }
            }
        });
    }
}
