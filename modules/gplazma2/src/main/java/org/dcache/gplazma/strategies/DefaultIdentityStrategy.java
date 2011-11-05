package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;

/**
 *
 */
public class DefaultIdentityStrategy implements IdentityStrategy {

    private List<GPlazmaPluginElement<GPlazmaIdentityPlugin>> ideStyleStrategies;

    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException {
        for(GPlazmaPluginElement<GPlazmaIdentityPlugin> ideStyleStrategy: ideStyleStrategies) {
            try {
                return ideStyleStrategy.getPlugin().map(principal);
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
            }catch(NoSuchPrincipalException e) {
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
