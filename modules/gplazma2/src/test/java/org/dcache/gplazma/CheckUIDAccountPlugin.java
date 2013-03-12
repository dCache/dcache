package org.dcache.gplazma;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.UidPrincipal;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * This account plugin succeeds if the specified uid principal is present
 * fails otherwise
 * @author timur
 */
public class CheckUIDAccountPlugin implements GPlazmaAccountPlugin {
    public static final Logger LOGGER = LoggerFactory.getLogger(CheckUIDAccountPlugin.class);

    private static boolean _called;

    private final UidPrincipal _uid;


    public CheckUIDAccountPlugin(Properties properties) {
        checkArgument(properties.getProperty("uid")!=null, "UID must be set.");

        _uid = new UidPrincipal(properties.getProperty("uid"));
    }

    @Override
    public void account(Set<Principal> authorizedPrincipals) throws AuthenticationException {
        LOGGER.debug("account is called");

        checkAuthentication(authorizedPrincipals.contains(_uid),
                "uid "+_uid+" was not present in authorizedPrincipals");

        _called = true;
    }

    /**
     * thread unsafe way of checking if account method of any instance
     * of the CheckUIDAccountPlugin was called since the last reset was called
     * @return the called
     */
    public static boolean isCalled() {
        return _called;
    }

    /**
     * resets the value of called to false
     */
    public static void reset() {
        _called = false;
    }

}
