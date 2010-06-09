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
 *
 * @author timur
 */
public class DefaultAuthenticationStrategy implements AuthenticationStrategy {

    private static final Logger logger =
            LoggerFactory.getLogger(DefaultAuthenticationStrategy.class);

    private PAMStyleStrategy<GPlazmaAuthenticationPlugin> pamStyleAuthentiationStrategy;

    public void setPlugins(List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> plugins) {
        pamStyleAuthentiationStrategy = new PAMStyleStrategy(plugins);
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
    public synchronized void authenticate(
            final SessionID sessionID,
            final Set<Object> publicCredential,
            final Set<Object> privateCredential,
            final Set<Principal> identifiedPrincipals) throws AuthenticationException {
       pamStyleAuthentiationStrategy.callPlugins( new PluginCaller<GPlazmaAuthenticationPlugin>() {
            public boolean call(GPlazmaAuthenticationPlugin plugin) throws AuthenticationException {
                plugin.authenticate(
                        sessionID,
                        publicCredential,
                        privateCredential,
                        identifiedPrincipals);
                return  haveAllRequiredPrincipalsIdentified(identifiedPrincipals);
            }
        });

        if(identifiedPrincipals.isEmpty()) {
            throw new AuthenticationException("all authentication plugins ran, " +
                    "no principals were identified");
        }
    }

    /**
     *
     * @param authorizedPrincipals
     * @return false
     */
   private static boolean haveAllRequiredPrincipalsIdentified(
           Set<Principal> authorizedPrincipals) {
       //Timur: I have no idea how to tell if all the principals needed were identified,
       // so I return false here, which means that even if there are sufficient
       // authentication plugins
       // we will iterate over all authentication plugins
       return false;
    }

}
