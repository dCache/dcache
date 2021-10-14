package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides support for the ACCOUNT phase of logging in.  It tries the first plugin.  For
 * each plugin, it either tries the following plugin (if one is available) or returns depending on
 * the plugin's result and the configured control (OPTIONAL, REQUIRED, etc).
 */
public class DefaultAccountStrategy implements AccountStrategy {

    private static final Logger logger =
          LoggerFactory.getLogger(DefaultAccountStrategy.class);

    private volatile PAMStyleStrategy<GPlazmaAccountPlugin> pamStyleAccountStrategy;

    @Override
    public void setPlugins(List<GPlazmaPluginService<GPlazmaAccountPlugin>> plugins) {
        pamStyleAccountStrategy = new PAMStyleStrategy<>(plugins);
    }

    /**
     * Devegates execution of the {@link GPlazmaAccountPlugin#account(SessionID, Set<Principal>)
     * GPlazmaAccountPlugin.account} methods of the plugins supplied by {@link
     * GPlazmaStrategy#setPlugins(List< GPlazmaPluginService <T>>) GPlazmaStrategy.setPlugins} to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the {@link PluginCaller#call(GPlazmaPlugin)
     * PluginCaller} interface.
     *
     * @param sessionID
     * @param authorizedPrincipals
     * @throws AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    @Override
    public void account(final LoginMonitor monitor,
          final Set<Principal> authorizedPrincipals)
          throws AuthenticationException {
        pamStyleAccountStrategy.callPlugins(service -> {
            monitor.accountPluginBegins(service.getName(), service.getControl(),
                  authorizedPrincipals);

            GPlazmaAccountPlugin plugin = service.getPlugin();

            Result result = Result.FAIL;
            String error = null;
            try {
                plugin.account(authorizedPrincipals);
                result = Result.SUCCESS;
            } catch (AuthenticationException e) {
                error = e.getMessage();
                throw e;
            } finally {
                monitor.accountPluginEnds(service.getName(), service.getControl(),
                      result, error, authorizedPrincipals);
            }
        });
    }
}
