package org.dcache.auth.attributes;

/**
 * Login attributes describe various properties of a login session, e.g. a home
 * directory.
 */
public interface LoginAttribute
{
    /**
     * Returns a string representation of this login principal.
     */
    String toString();
}
