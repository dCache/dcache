package org.dcache.acl.matcher;

import org.dcache.acl.enums.AccessType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.acl.Permission;
import org.dcache.acl.enums.AccessMask;

/**
 * Component matches a access request and the access masks defMsk and allowMsk and returns true if
 * access is allowed.
 *
 * @author David Melkumyan, DESY Zeuthen
 * @author Anupam Ashish, DESY Hamburg
 *
 */
public class AclMatcher {

    private static final Logger LOG = LoggerFactory.getLogger(AclMatcher.class);

    private AclMatcher() {
    }

    /**
     * @param perm
     *            Permission which contains defMask and allowMask
     * @param access
     *            Access bit mask
     * @return
     *            <li><code>ACCESS_ALLOWED</code> if access is allowed
     *            <li><code>ACCESS_DENIED</code> if access is denied
     *            <li><code>ACCESS_UNDEFINED</code> if there is no access right definition
     */
    public static AccessType isAllowed(Permission perm, AccessMask access) {
        int definedMask = perm.getDefMsk();
        int allowedMask = perm.getAllowMsk();
        int accessMask = access.getValue();

        AccessType allowed;
        if ( (accessMask & definedMask) == 0 ) {
            allowed = AccessType.ACCESS_UNDEFINED;
        } else if ( (accessMask & allowedMask) == accessMask ) {
            allowed = AccessType.ACCESS_ALLOWED;
        } else {
            allowed = AccessType.ACCESS_DENIED;
        }

        LOG.debug("acccess mask: {} : {}", access, allowed);
        return allowed;
    }
}
