package org.dcache.acl.matcher;

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
