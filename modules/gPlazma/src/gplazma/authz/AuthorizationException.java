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

public class AuthorizationException extends java.lang.Exception {
	
	public AuthorizationException() {
	}
	public AuthorizationException(String message) {
		super(message);
	}

} //end of class AuthorizationException
