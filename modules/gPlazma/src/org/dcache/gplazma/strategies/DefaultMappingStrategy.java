package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class DefaultMappingStrategy implements MappingStrategy {

    private static final Logger logger =
            LoggerFactory.getLogger(DefaultMappingStrategy.class);

    private PAMStyleStrategy<GPlazmaMappingPlugin> pamStyleMappingStrategy;

    public void setPlugins(List<GPlazmaPluginElement<GPlazmaMappingPlugin>> plugins) {
        pamStyleMappingStrategy = new PAMStyleStrategy(plugins);
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaMappingPlugin#map(SessionID, Set<Principal>, Set<Principal>) GPlazmaMappingPlugin.map}
     * methods of the plugins supplied by
     * {@link GPlazmaStrategy#setPlugins(List<GPlazmaPluginElement<T>>) GPlazmaStrategy.setPlugins}
     *  to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the
     * {@link PluginCaller#call(org.dcache.gplazma.plugins.GPlazmaPlugin) PluginCaller}
     * interface.
     * @param sessionID
     * @param principals
     * @param authorizedPrincipals
     * @throws org.dcache.gplazma.AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    public synchronized void map(
            final SessionID sessionID,
            final Set<Principal> principals,
            final Set<Principal> authorizedPrincipals)
            throws AuthenticationException {

        pamStyleMappingStrategy.callPlugins( new PluginCaller<GPlazmaMappingPlugin>() {
            public boolean call(GPlazmaMappingPlugin plugin) throws AuthenticationException {
                plugin.map(sessionID, principals, authorizedPrincipals);
                return haveAllRequiredAuthorizedPrincipals(authorizedPrincipals);
            }
        });

        if(! haveAllRequiredAuthorizedPrincipals(authorizedPrincipals) ) {
            throw new AuthenticationException("all mappingPlugins are executed, " +
                    "not all required " +
                    "principals are present in authorizedPrincipals");
        }
    }

    /**
     * Devegates execution of the
     * {@link GPlazmaMappingPlugin.reverseMap(SessionID, Set<Principal>, Set<Principal>) map}
     * methods of the plugins supplied by
     * {@link GPlazmaStrategy.setPlugins(List<GPlazmaPluginElement<T>>) GPlazmaStrategy.setPlugins}
     *  to
     * {@link  PAMStyleStrategy#callPlugins(PluginCaller<T>) PAMStyleStrategy.callPlugins(PluginCaller<T>)}
     * by providing anonymous implementation of the
     * {@link PluginCaller#call(org.dcache.gplazma.plugins.GPlazmaPlugin) PluginCaller}
     * interface.
     * @param sessionID
     * @param sourcePrincipal
     * @param principals
     * @throws org.dcache.gplazma.AuthenticationException
     * @see PAMStyleStrategy
     * @see PluginCaller
     */
    public synchronized void reverseMap(
            final SessionID sessionID,
            final Principal sourcePrincipal,
            final Set<Principal> principals)
            throws AuthenticationException {
        pamStyleMappingStrategy.callPlugins( new PluginCaller<GPlazmaMappingPlugin>() {
            public boolean call(GPlazmaMappingPlugin plugin) throws AuthenticationException {
                plugin.reverseMap(sessionID, sourcePrincipal, principals);
                return !principals.isEmpty();
            }
        });

        if( principals.isEmpty() ) {
            throw new AuthenticationException("all mappingPlugins are executed, " +
                    "no reverseMaped principalas were found ");
        }

    }

    /**
     * Check if the authorizedPrincipals has all required types of principals
     * mapped by MappingPlugin
     * @param authorizedPrincipals
     * @return true if authorizedPrincipals set contain at least one instance of
     *   each type of {@link UidPrincipal UidPrincipal},
     * {@link GidPrincipal GidPrincipal} and
     * {@link UserNamePrincipal UserNamePrincipal}
     */
    private static boolean haveAllRequiredAuthorizedPrincipals(Set<Principal> authorizedPrincipals) {
        boolean userNamePrincipalFound = false;
        boolean uidPrincipalFound = false;
        boolean gidPrincipalFound = false;
        for(Principal authorizedPrincipal:authorizedPrincipals) {
            if(authorizedPrincipal instanceof UserNamePrincipal) {
                userNamePrincipalFound = true;
                continue;
            }
            if(authorizedPrincipal instanceof UidPrincipal) {
                uidPrincipalFound = true;
                continue;
            }
            if(authorizedPrincipal instanceof GidPrincipal) {
                gidPrincipalFound = true;
                continue;
            }
        }
        return userNamePrincipalFound && uidPrincipalFound && gidPrincipalFound;
    }


}
