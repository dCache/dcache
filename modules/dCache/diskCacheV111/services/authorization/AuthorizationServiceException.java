/*
 * AuthorizationServiceException.java 
 * 
 * Created on January 29, 2005
 */

package diskCacheV111.services.authorization;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public class AuthorizationServiceException  extends java.lang.Exception {

  static final long serialVersionUID = -621299439311397948L;

  public AuthorizationServiceException() {
	}
	public AuthorizationServiceException(String message) {
		super(message);
	}
  public AuthorizationServiceException(String message, Throwable cause) {
    super(message, cause);
  }

} //end of class AuthorizationServiceException
