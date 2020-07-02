package org.dcache.auth;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;

import diskCacheV111.util.CacheException;

/**
 * LoginStrategy describes how Subjects are logged in, mapped and
 * reverse mapped. It is primarily used by doors.
 */
public interface LoginStrategy
{
    /**
     * Logs in a Subject.  The input Subject contains material supplied by the
     * client to establish the user identity (public credentials and private
     * credentials) along with information discovered by the door (principals).
     * If the door includes any dCache-defined principals then they must be
     * tagged @AuthenticationInput.  Specific principals defined outside of
     * dCache may also be included.
     * <p>
     * If the login is successful then this method returns a non-null
     * LoginReply value.
     * <p>
     * An implementation MAY assume that the calling thread has an
     * associated CDC with a session ID already.
     *
     * @throws PermissionDeniedCacheException when the login is denied
     * @throws CacheException when the login failed
     * @throws IllegalArgumentException when the Subject cannot be processed
     *         because its credentials or principals are not supported by
     *         this LoginStrategy
     * @see AuthenticationInput
     */
    LoginReply login(Subject subject) throws CacheException;

    /**
     * Maps the principal to its UidPrincipal or GidPrincipal. Returns
     * null if a mapping cannot be established.
     * <p>
     * The input principal must be tagged @AuthenticationOutput.
     * <p>
     * It is essential that the uid or gid uniquely identifies the
     * user or group described by {@code principal}. In particular for
     * groups it MUST NOT be the case that the group describes neither
     * a smaller nor larger set than what is implied by the principal.
     * @see AuthenticationOutput
     */
    Principal map(Principal principal) throws CacheException;

    /**
     * Maps a UidPrincipal or GidPrincipal to the set of semantically
     * equivalent Principals. All these principals would map to the
     * UidPrincipal or GidPrincipal given as an argument if presented
     * to the map method.
     * <p>
     * The returned set contains only principals that have been tagged
     * @AuthenticationOutput.
     * @see AuthenticationOutput
     */
    Set<Principal> reverseMap(Principal principal) throws CacheException;
}
