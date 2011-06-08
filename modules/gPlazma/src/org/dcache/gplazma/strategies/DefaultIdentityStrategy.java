package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DefaultIdentityStrategy implements IdentityStrategy {

    private static final Logger logger =
            LoggerFactory.getLogger(DefaultIdentityStrategy.class);
    private List<GPlazmaPluginElement<GPlazmaIdentityPlugin>> ideStyleStrategies;

    @Override
    public Principal map(Principal principal) throws AuthenticationException {
        for(GPlazmaPluginElement<GPlazmaIdentityPlugin> ideStyleStrategy: ideStyleStrategies) {
            Principal p = ideStyleStrategy.getPlugin().map(principal);
            if( p != null )
                return p;
        }
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws AuthenticationException {
        for(GPlazmaPluginElement<GPlazmaIdentityPlugin> ideStyleStrategy: ideStyleStrategies) {
            Set<Principal> p = ideStyleStrategy.getPlugin().reverseMap(principal);
            if( p != null && !p.isEmpty())
                return p;
        }
        return Collections.EMPTY_SET;
    }

    @Override
    public void setPlugins(List<GPlazmaPluginElement<GPlazmaIdentityPlugin>> plugins) {
        ideStyleStrategies = plugins;
    }

}
