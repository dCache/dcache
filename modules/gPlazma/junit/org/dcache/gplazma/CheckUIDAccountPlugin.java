package org.dcache.gplazma;
import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.auth.UidPrincipal;

/**
 * This account plugin succeeds if the specified uid principal is present
 * fails otherwise
 * @author timur
 */
public class CheckUIDAccountPlugin implements GPlazmaAccountPlugin {
    public static final Logger LOGGER =
            LoggerFactory.getLogger(CheckUIDAccountPlugin.class);
    private static boolean called = false;

    private final UidPrincipal uid;


    public CheckUIDAccountPlugin(String[] args) {
        if(args==null || args.length != 1) {
            throw new IllegalArgumentException("I need 1 argument: uid");
        }
        uid = new UidPrincipal(args[0]);

    }

    @Override
    public void account(SessionID sID, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        LOGGER.debug("account is called");
        for(Principal principal:authorizedPrincipals ) {
            if(principal.equals(uid)) {
                called = true;
                return;
            }
        }
        throw new AuthenticationException("uid "+uid+" was not present in authorizedPrincipals");
    }

    /**
     * thread unsafe way of checking if account method of any instance
     * of the CheckUIDAccountPlugin was called since the last reset was called
     * @return the called
     */
    public static boolean isCalled() {
        return called;
    }

    /**
     * resets the value of called to false
     */
    public static void reset() {
        called = false;
    }

}
