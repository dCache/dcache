package org.dcache.gplazma;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void authenticate(SessionID sID, Set<Object> publicCredentials,
            Set<Object> privateCredentials,
            Set<Principal> identifiedPrincipals) throws AuthenticationException {
        LOGGER.debug("authenticate called");
        for(Object credential:publicCredentials) {
             LOGGER.debug("found pub credential : " , credential);
            if(credential instanceof Principal) {
                LOGGER.debug("found Principal: {}" , credential);
                identifiedPrincipals.add((Principal)credential);
            }
        }
        for(Object credential:privateCredentials) {
            LOGGER.debug("found private credential : " , credential);
            if(credential instanceof Principal) {
                LOGGER.debug("found Principal: {}" , credential);
                identifiedPrincipals.add((Principal)credential);
            }
        }
    }


}
