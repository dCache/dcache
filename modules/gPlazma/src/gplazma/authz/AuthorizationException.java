/*
 * AuthorizationException.java
 *
 * Created on January 29, 2005
 */

package gplazma.authz;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public class AuthorizationException extends Exception {

    public AuthorizationException() {
    }

    public AuthorizationException(String message) {
        super(message);
    }

    public AuthorizationException(Throwable cause) {
        super(cause);
    }

    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

} //end of class AuthorizationException
