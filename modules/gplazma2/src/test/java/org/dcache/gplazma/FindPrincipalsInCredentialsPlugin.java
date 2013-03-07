package org.dcache.gplazma;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;

/**
 *
 * @author timur
 */
public class FindPrincipalsInCredentialsPlugin implements GPlazmaAuthenticationPlugin {
    public static final Logger LOGGER =
            LoggerFactory.getLogger(FindPrincipalsInCredentialsPlugin.class);

    public FindPrincipalsInCredentialsPlugin(Properties properties) {
        //
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
            Set<Object> privateCredentials,
            Set<Principal> identifiedPrincipals) throws AuthenticationException {
        LOGGER.trace("authenticate called");
        for(Object credential:publicCredentials) {
             LOGGER.trace("found pub credential : " , credential);
            if(credential instanceof Principal) {
                LOGGER.trace("found Principal: {}" , credential);
                identifiedPrincipals.add((Principal)credential);
            }
        }
        for(Object credential:privateCredentials) {
            LOGGER.trace("found private credential : " , credential);
            if(credential instanceof Principal) {
                LOGGER.trace("found Principal: {}" , credential);
                identifiedPrincipals.add((Principal)credential);
            }
        }
    }


}
