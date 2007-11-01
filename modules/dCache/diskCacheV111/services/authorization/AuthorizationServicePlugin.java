/*
 * AuthorizationServicePlugin.java 
 * 
 * Created on January 29, 2005
 */

package diskCacheV111.services.authorization;

//java
import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
//import java.net.URL;

//dcache
import diskCacheV111.util.*;

//jgss
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public abstract class AuthorizationServicePlugin {
	public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket) 
	throws AuthorizationServiceException {
    return null;
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {
    return null;
  }

  public void setLogLevel	(String level) {
  }
} //end of AuthorizationServicePlugin
