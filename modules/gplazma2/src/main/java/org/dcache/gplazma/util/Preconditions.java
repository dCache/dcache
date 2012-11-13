package org.dcache.gplazma.util;

import org.dcache.gplazma.AuthenticationException;

/**
 * A collection of utility methods for checking something and throwing the
 * appropriate exception.  These may be useful to gPlazma core or gPlazma
 * plugins
 */
public class Preconditions
{
    /**
     * A utility method that throws an exception if the first argument is false.
     * @param isAuthenticated boolean value.
     * @param message an explanation of why the exception was thrown.
     * @throws AuthenticationException if isAuthenticated is false
     */
    public static void checkAuthentication(boolean isAuthenticated,
            String message) throws AuthenticationException
    {
        if(!isAuthenticated) {
            throw new AuthenticationException(message);
        }
    }

}
