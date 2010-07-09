package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;
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

    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaMappingPlugin>> plugins) {
        pamStyleMappingStrategy = new PAMStyleStrategy<GPlazmaMappingPlugin>(plugins);
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
    @Override
    public synchronized void map(
            final SessionID sessionID,
            final Set<Principal> principals,
            final Set<Principal> authorizedPrincipals)
            throws AuthenticationException {
        logger.debug("call to map");
        pamStyleMappingStrategy.callPlugins( new PluginCaller<GPlazmaMappingPlugin>() {
            @Override
            public void call(GPlazmaMappingPlugin plugin) throws AuthenticationException {
                plugin.map(sessionID, principals, authorizedPrincipals);
            }
        });
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
    @Override
    public synchronized void reverseMap(
            final SessionID sessionID,
            final Principal sourcePrincipal,
            final Set<Principal> principals)
            throws AuthenticationException {
        logger.debug("call to reverse-map");
        pamStyleMappingStrategy.callPlugins( new PluginCaller<GPlazmaMappingPlugin>() {
            @Override
            public void call(GPlazmaMappingPlugin plugin) throws AuthenticationException {
                plugin.reverseMap(sessionID, sourcePrincipal, principals);
            }
        });
    }
}
