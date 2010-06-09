package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.HomeDirectory;
import org.dcache.gplazma.RootDirectory;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class DefaultSessionStrategy  implements SessionStrategy{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSessionStrategy.class);

    private PAMStyleStrategy<GPlazmaSessionPlugin> pamStyleSessionStrategy;

    /**
     *
     * @param plugins
     */
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaSessionPlugin>> plugins) {
        pamStyleSessionStrategy = new PAMStyleStrategy(plugins);
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaSessionPlugin#session(SessionID, Set<Principal>,Set<SessionAttribute>) GPlazmaSessionPlugin.session}
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
    public synchronized void session(
            final SessionID sessionID,
            final Set<Principal> authorizedPrincipals,
            final Set<SessionAttribute> attrib) throws AuthenticationException {

        pamStyleSessionStrategy.callPlugins( new PluginCaller<GPlazmaSessionPlugin>() {
            public boolean call(GPlazmaSessionPlugin plugin) throws AuthenticationException {
                plugin.session(sessionID, authorizedPrincipals, attrib);
                return haveAllRequiredAttributes(attrib);
            }
        });

        if(! haveAllRequiredAttributes(attrib) ) {
            throw new AuthenticationException("all session plugins are executed, " +
                    "not all required " +
                    "attributes are present in attrib");
        }

    }

    /**
     *
     * @param attributes
     * @return true iff {@link attributes} has at least one HomeDirectory and
     * one RootDirectory
     */
    private static boolean haveAllRequiredAttributes(Set<SessionAttribute> attributes) {
        boolean homeDirectoryFound = false;
        boolean rootDirectoryFound = false;
        for(SessionAttribute attribute:attributes) {
            if(attribute instanceof HomeDirectory) {
                homeDirectoryFound = true;
            }
            if(attribute instanceof RootDirectory) {
                rootDirectoryFound = true;
            }
        }
        return homeDirectoryFound && rootDirectoryFound;
    }
}
