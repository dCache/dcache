package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides support for the AUTH phase of logging in.  It tries
 * the first plugin.  For each plugin, it either tries the following plugin (if
 * one is available) or returns depending on the plugin's result and the
 * configured control (OPTIONAL, REQUIRED, etc).
 */
public class DefaultAuthenticationStrategy implements AuthenticationStrategy
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DefaultAuthenticationStrategy.class);

    private PAMStyleStrategy<GPlazmaAuthenticationPlugin> pamStyleAuthentiationStrategy;

    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> plugins)
    {
        pamStyleAuthentiationStrategy = new PAMStyleStrategy<GPlazmaAuthenticationPlugin>(plugins);
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaAuthenticationPlugin#authenticate(SessionID, Set<Object>,Set<Object>, Set<Principal>) GPlazmaAuthenticationPlugin.authenticate}
     * methods of the plugins supplied by
     * {@link GPlazmaStrategy#setPlugins(List<GPlazmaPluginElement<T>>) GPlazmaStrategy.setPlugins}
     *  to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the
     * {@link PluginCaller#call(org.dcache.gplazma.plugins.GPlazmaPlugin) PluginCaller}
     * interface.
     *
     * @param sessionID
     * @param publicCredential
     * @param privateCredential
     * @param identifiedPrincipals
     * @throws org.dcache.gplazma.AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    @Override
    public synchronized void authenticate(final SessionID sessionID,
            final Set<Object> publicCredential,
            final Set<Object> privateCredential,
            final Set<Principal> identifiedPrincipals)
            throws AuthenticationException
    {
        pamStyleAuthentiationStrategy.callPlugins(new PluginCaller<GPlazmaAuthenticationPlugin>()
        {
            @Override
            public void call(GPlazmaAuthenticationPlugin plugin) throws AuthenticationException
            {
                LOGGER.debug("calling (publicCred: {}, privateCred: {}, " +
                        "principals: {})", new Object[] {publicCredential,
                        privateCredential, identifiedPrincipals});

                plugin.authenticate(sessionID, publicCredential,
                        privateCredential, identifiedPrincipals);
            }
        });
    }
}
