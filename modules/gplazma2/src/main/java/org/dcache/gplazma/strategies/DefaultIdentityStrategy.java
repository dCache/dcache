package org.dcache.gplazma.strategies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;

/**
 * The DefaultIdentityStrategy tries each of the supplied plugins in turn when
 * answering either a map or a reverseMap operation.  The reply from the first
 * plugin that maps or reverseMaps the supplied principal is used.
 */
public class DefaultIdentityStrategy implements IdentityStrategy {
    private static final Logger _log =
            LoggerFactory.getLogger(DefaultIdentityStrategy.class);

    private volatile List<GPlazmaPluginElement<GPlazmaIdentityPlugin>> ideStyleStrategies;

    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException {
        for(GPlazmaPluginElement<GPlazmaIdentityPlugin> ideStyleStrategy: ideStyleStrategies) {
            try {
                return ideStyleStrategy.getPlugin().map(principal);
            } catch(RuntimeException e) {
                _log.error("Bug in plugin " + ideStyleStrategy.getName(), e);
            } catch (NoSuchPrincipalException e) {
                // NOP
            }
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException {
        for(GPlazmaPluginElement<GPlazmaIdentityPlugin> ideStyleStrategy: ideStyleStrategies) {
            try {
                return ideStyleStrategy.getPlugin().reverseMap(principal);
            } catch(RuntimeException e) {
                _log.error("Bug in plugin " + ideStyleStrategy.getName(), e);
            } catch(NoSuchPrincipalException e) {
                // NOP
            }
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaIdentityPlugin>> plugins) {
        ideStyleStrategies = plugins;
    }

}
