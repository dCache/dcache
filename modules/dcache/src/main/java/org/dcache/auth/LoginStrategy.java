package org.dcache.auth;

import java.util.Set;
import java.security.Principal;
import javax.security.auth.Subject;

import diskCacheV111.util.CacheException;

/**
 * LoginStrategy describes how Subjects are logged in, mapped and
 * reverse mapped. It is primarily used by doors.
 */
public interface LoginStrategy
{
    /**
     * Logs in a Subject. Returns a Session object.
     *
     * An implementation MAY assume that the calling thread has an
     * associated CDC with a session ID already.
     *
     * @throws PermissionDeniedCacheException when the login is denied
     * @throws CacheException when the login failed
     * @throws IllegalArgumentException when the Subject cannot be processed
     *         because its credentials or principals are not supported by
     *         this LoginStrategy
     */
    LoginReply login(Subject subject) throws CacheException;

    /**
     * Maps the principal to its UidPrincipal or GidPrincipal. Returns
     * null if a mapping cannot be established.
     *
     * It is essential that the uid or gid uniquely identifies the
     * user or group described by {@code principal}. In particular for
     * groups it MUST NOT be the case that the group describes neither
     * a smaller nor larger set than what is implied by the principal.
     */
    Principal map(Principal principal) throws CacheException;

    /**
     * Maps a UidPrincipal or GidPrincipal to the set of semantically
     * equivalent Principals. All these principals would map to the
     * UidPrincipal or GidPrincipal given as an argument if presented
     * to the map method.
     */
    Set<Principal> reverseMap(Principal principal) throws CacheException;
}
