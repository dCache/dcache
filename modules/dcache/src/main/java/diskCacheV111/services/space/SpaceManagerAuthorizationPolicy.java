/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package diskCacheV111.services.space;

import javax.security.auth.Subject;

import diskCacheV111.util.VOInfo;

/**
 *
 * @author timur
 */
public interface SpaceManagerAuthorizationPolicy {

    public void checkReleasePermission(Subject subject, Space space)
        throws SpaceAuthorizationException;
    public VOInfo checkReservePermission(Subject subject, LinkGroup linkGroup)
        throws SpaceAuthorizationException;

}
