package org.dcache.acl.matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.Permission;
import org.dcache.acl.enums.AccessMask;

/**
 * Component matches a access request and the access masks defMsk and allowMsk and returns true if
 * access is allowed.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class AclNFSv4Matcher extends AclMatcher {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AclNFSv4Matcher.class.getName());

    private AclNFSv4Matcher() {
    }

    /**
     * @param perm
     *            Permission which contains defMask and allowMask
     * @param action
     *            LINK / LOOKUPP / LOOKUP / READER / READLINK / WRITE
     * @return
     *            <li><code>TRUE</code> if access is allowed
     *            <li><code>FALSE</code> if access is denied
     *            <li><code>null</code> if there is no access right definition
     */
    public static Boolean isAllowed(Permission perm, AccessMask accessMask) {

        int defMask = perm.getDefMsk(), allowMask = perm.getAllowMsk();

        Boolean allowed = isAllowed(defMask, allowMask, accessMask.getValue());

        logger.debug("acccess mask: {} : {}", accessMask,
                (allowed == null) ? UNDEFINED : (allowed ? ALLOWED : DENIED));

        return allowed;
    }
}
