package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class DefaultSessionStrategy  implements SessionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSessionStrategy.class);

    private PAMStyleStrategy<GPlazmaSessionPlugin> pamStyleSessionStrategy;

    /**
     *
     * @param plugins
     */
    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaSessionPlugin>> plugins) {
        pamStyleSessionStrategy = new PAMStyleStrategy<GPlazmaSessionPlugin>(plugins);
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaSessionPlugin#session(SessionID, Set<Principal>,Set<Object>) GPlazmaSessionPlugin.session}
     * methods of the plugins supplied by
     * {@link GPlazmaStrategy#setPlugins(List<GPlazmaPluginElement<T>>) GPlazmaStrategy.setPlugins}
     *  to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the
     * {@link PluginCaller#call(org.dcache.gplazma.plugins.GPlazmaPlugin) PluginCaller}
     * interface.
     *
     * @param sessionID
     * @param authorizedPrincipals
     * @param attrib
     * @throws org.dcache.gplazma.AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    @Override
    public synchronized void session(
            final SessionID sessionID,
            final Set<Principal> authorizedPrincipals,
            final Set<Object> attrib) throws AuthenticationException
    {
        logger.debug("call to session");
        pamStyleSessionStrategy.callPlugins( new PluginCaller<GPlazmaSessionPlugin>() {
            @Override
            public void call(GPlazmaSessionPlugin plugin) throws AuthenticationException {
                plugin.session(sessionID, authorizedPrincipals, attrib);
            }
        });

    }
}
