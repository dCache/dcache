package org.dcache.acl.matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclTestMatcher extends AclMatcher {
    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AclTestMatcher.class.getName());

    private static AclTestMatcher _SINGLETON;
    static {
        _SINGLETON = new AclTestMatcher();
    }

    private AclTestMatcher() {
    }

    public static AclTestMatcher instance() {
        return _SINGLETON;
    }

    public static void refresh() {
        _SINGLETON = new AclTestMatcher();
    }

    /**
     * @deprecated
     * @param defMask
     *            Defined mask the flags which have been set to either “allow”
     *            or “deny”
     * @param allowMask
     *            Defines the flags which have been set to “allow”
     * @param accessMask
     *            Access mask
     * @param debug_msg
     *            debug message, i.e. specifies the name of operation (can be
     *            null)
     * @return Returns null if there is no access right definition, true if
     *         access is allowed, and false if access is denied
     */
    public static Boolean isAllowed(int defMask, int allowMask, org.dcache.acl.enums.AccessMask accessMask, String debug_msg) {
        Boolean allowed = isAllowed(defMask, allowMask, accessMask);
        if ( logger.isDebugEnabled() ) {
            String str = (debug_msg == null) ? "Access is " : debug_msg + " => ";
            if ( allowed == null )
                logger.debug(str + UNDEFINED);
            else
                logger.debug(str + (allowed ? ALLOWED : DENIED));
        }
        return allowed;
    }

}
