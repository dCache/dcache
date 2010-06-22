package org.dcache.auth;

import javax.security.auth.Subject;

import diskCacheV111.util.CacheException;

/**
 * LoginStrategy describes how Subjects are logged in, mapped and
 * reverse mapped. It is primarily used by doors.
 *
 * TODO: Add signatures for map and reverse.
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

    // map
    // reverse
}