package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class DefaultAccountStrategy  implements AccountStrategy {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAccountStrategy.class);

    private PAMStyleStrategy<GPlazmaAccountPlugin> pamStyleAccountStrategy;

    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaAccountPlugin>> plugins) {
        pamStyleAccountStrategy = new PAMStyleStrategy<GPlazmaAccountPlugin>(plugins);
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaAccountPlugin#account(SessionID, Set<Principal>) GPlazmaAccountPlugin.account}
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
     * @throws org.dcache.gplazma.AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    @Override
    public synchronized void account(
            final SessionID sessionID,
            final Set<Principal> authorizedPrincipals) throws AuthenticationException {
       logger.debug("call to account");
       pamStyleAccountStrategy.callPlugins( new PluginCaller<GPlazmaAccountPlugin>() {
           @Override
           public void call(GPlazmaAccountPlugin plugin) throws AuthenticationException {
               plugin.account(sessionID, authorizedPrincipals);
           }
        });
    }


}
