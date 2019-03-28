package org.dcache.gplazma.util;

import org.dcache.gplazma.AuthenticationException;

import static org.dcache.util.Exceptions.genericCheck;

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
     * @param message a message template that explanations why the exception was thrown.
     * @param args the arguments for the message template.
     * @throws AuthenticationException if isAuthenticated is false
     */
    public static void checkAuthentication(boolean isAuthenticated,
            String message, Object... args) throws AuthenticationException
    {
        genericCheck(isAuthenticated, AuthenticationException::new, message, args);
    }
}
