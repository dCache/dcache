package org.dcache.acl.matcher;

import org.dcache.acl.enums.AccessMask;

/**
 * Component matches a access request and the access masks defMsk and allowMsk and returns true if
 * access is allowed.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public abstract class AclMatcher {

    protected static final String UNDEFINED = "UNDEFINED";

    protected static final String ALLOWED = "ALLOWED";

    protected static final String DENIED = "DENIED";

    /**
     * use instead method isAllowed(int, int, int)
     *
     * @param defMask
     *            Defined bit mask
     * @param allowMask
     *            Allowed bit mask
     * @param accessMask
     *            AccessmMask object
     * @return Returns null if there is no access right definition, true if access is allowed, and
     *         false if access is denied
     */
    @Deprecated
    protected static Boolean isAllowed(int defMask, int allowMask, AccessMask accessMask) {
        if ( accessMask.matches(defMask) ) {
            return accessMask.matches(allowMask);
        }
        return null;
    }

    /**
     * @param defMask
     *            Defined bit mask
     * @param allowMask
     *            Allowed bit mask
     * @param accessMask
     *            Access bit mask
     * @return
     *            <ul>
     *            <li><code>TRUE</code> if accessMask in defMask/allowMask
     *            <li><code>NULL</code> if no bit from accessMask in defMask
     *            <li>otherwise <code>FALSE</code>
     */
    protected static Boolean isAllowed(int defMask, int allowMask, int accessMask) {
        if ( (accessMask & defMask) == 0 ) {
            return null;
        }
        return (accessMask & allowMask) == accessMask;
    }
}
