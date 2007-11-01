// $Id: KPWDAuthorizationPlugin.java,v 1.3.2.6 2007-02-28 23:10:38 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.3.2.5  2007/01/04 18:13:15  tdh
// Made authRequestID instance, rather than static, variable for threading.
// Previous code might cause incorrect IDs to be printed in log - would not affect authorization.
//
// Revision 1.3.2.4  2006/12/07 12:56:02  tigran
// say, debug, warn, esay does nothing.
// at least plugin works
//
// Revision 1.3.2.3  2006/11/30 21:49:58  tdh
// Backport of rationalization of logging.
//
// Revision 1.7  2006/11/29 19:09:38  tdh
// Added debug log level and changed logging lines.
//
// Revision 1.6  2006/11/28 21:12:35  tdh
// Added setLogLevel function to interface and implemented in plugins. Made all plugins use log4j.
//
// Revision 1.5  2006/08/07 16:38:03  tdh
// Merger of changes from branch, exception handling and ignore blank config file lines.
//
// Revision 1.3.2.2  2006/08/07 15:56:59  tdh
// Catch any Exception (not just IOException) and forward as AuthorizationServiceException.
//
// Revision 1.3.2.1  2006/07/26 18:41:59  tdh
// Backport of recent changes to development branch.
//
// Revision 1.4  2006/07/25 15:13:39  tdh
// Added method to authenticate by DN/Role. Improved logging.
//

/*
 * KPWDAuthorizationPlugin.java
 * 
 * Created on January 29, 2005
 */

package diskCacheV111.services.authorization;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import diskCacheV111.util.*;
import diskCacheV111.util.KAuthFile;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public class KPWDAuthorizationPlugin extends AuthorizationServicePlugin {

	private String kAuthFilePath;
  private long authRequestID=0;
	UserAuthRecord authRecord;
	GSSContext context;
	String desiredUserName;
	
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";

	//constructor
	public KPWDAuthorizationPlugin(String kAuthFilePath, long authRequestID)
	throws AuthorizationServiceException {
		this.kAuthFilePath = kAuthFilePath;
    this.authRequestID=authRequestID;
    debug("KPWDAuthorizationPlugin: authRequestID " + authRequestID + " Plugin now loaded: dcache.kpwd");
	}

  public KPWDAuthorizationPlugin(String kAuthFilePath)
  throws AuthorizationServiceException {
    this.kAuthFilePath = kAuthFilePath;
    debug("KPWDAuthorizationPlugin: now loaded: dcache.kpwd Plugin");
  }

	public KPWDAuthorizationPlugin()
	throws AuthorizationServiceException {
		debug("KPWDAuthorizationPlugin: now loaded: dcache.kpwd Plugin");
	}

  public void setLogLevel	(String level) {
    
  }

  private void debug(String s) {
   
  }

  private void say(String s) {
   
  }

  private void warn(String s) {
   
  }

  private void esay(String s) {
   
  }

	public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

    this.context = context;
    GSSName GSSId;
		String subjectDN;

    try {
			GSSId = context.getSrcName();
			subjectDN = GSSId.toString();
			debug("Subject DN from GSSContext extracted as: " +subjectDN);
		}
		catch(org.ietf.jgss.GSSException gsse ) {
			esay("Error extracting Subject DN from GSSContext: " +gsse);
			throw new AuthorizationServiceException (gsse.toString());
		}

    return authorize(subjectDN, null, desiredUserName, serviceUrl, socket);
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
  throws AuthorizationServiceException {

	  debug("Using dcache.kpwd configuration: " + kAuthFilePath);
	  KAuthFile authF = null;
		this.desiredUserName = desiredUserName;

		String user_name;

		try {
			authF = new KAuthFile(kAuthFilePath);
		}
		catch(Exception e) {
			esay("Exception in KAuthFile instantiation: " +e);
			throw new AuthorizationServiceException (e.toString());
		}
		
		if (desiredUserName != null) {
			debug("Desired Username requested as: " + desiredUserName);
			user_name = desiredUserName;
		}
		else {
			say("Requesting mapping for User with DN: " + subjectDN);
			user_name = authF.getIdMapping(subjectDN);
			say("dcache.kpwd service returned Username: " + user_name);
			if (user_name == null) {
				throw new AuthorizationServiceException("Cannot determine Username from SubjectDN " + subjectDN);
			}	
		}

		/*	
		user_name = authF.getIdMapping(subjectDN);
		System.out.println("Subject DN is mapped to Username:" +user_name);
		if (user_name == null) {
			throw new AuthorizationServiceException("Cannot determine Username from Subject DN=" +subjectDN);
		}
		*/
		
		UserAuthRecord authRecord = authF.getUserRecord(user_name);
		if (authRecord == null) {
			esay("User " +user_name+ " is not found in authorization records");
			esay("dcache.kpwd Authorization Service plugin: Authorization denied for user: " + user_name + " with subject DN: " + subjectDN);
			//throw new AuthorizationServiceException("User " +user_name+ " is not found in authorization records");
		}
		
		if (!authRecord.hasSecureIdentity(subjectDN)) {
			esay("dcache.kpwd Authorization Service plugin: Authorization denied for user: "+user_name + " with subject DN: " +subjectDN);
			authRecord = null;
			//throw new AuthorizationServiceException("dcache.kpwd Authorization Plugin: Authorization denied for user " +user_name+ " with Subject DN " +subjectDN);
		}
		
		return authRecord;
	}	

} //end of class KPWDAuthorizationPlugin
