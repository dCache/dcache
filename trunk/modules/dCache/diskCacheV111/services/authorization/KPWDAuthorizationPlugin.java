// $Id: KPWDAuthorizationPlugin.java,v 1.12 2007-04-17 21:46:54 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11  2007/02/08 20:27:46  tdh
// Added code for dynamic mapping of uid, gid, etc.
//
// Revision 1.10  2007/01/12 19:30:13  tdh
// Added static strings DENIED_MESSAGE and REVOCATION_MESSAGE to AuthorizationServicePlugin.
// GPLAZMALiteVORoleAuthzPlugin will throw REVOCATION_MESSAGE for "-" mapping.
// Plugins will throw exceptions starting with DENIED_MESSAGE for null mappings.
// Changed log level of permission denied to warning in plugins.
//
// Revision 1.9  2007/01/04 17:46:48  tdh
// Made authRequestID instance, rather than static, variable for threading.
//
// Revision 1.8  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationService.
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
import org.apache.log4j.*;


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
  static Logger log = Logger.	getLogger(KPWDAuthorizationPlugin.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);

	//constructor
	public KPWDAuthorizationPlugin(String kAuthFilePath, long authRequestID)
	throws AuthorizationServiceException {
		this.kAuthFilePath = kAuthFilePath;
    this.authRequestID=authRequestID;
    if(((Logger)log).getAppender("KPWDAuthorizationPlugin")==null) {
      Enumeration appenders = log.getParent().getAllAppenders();
      while(appenders.hasMoreElements()) {
        Appender apnd = (Appender) appenders.nextElement();
        if(apnd instanceof ConsoleAppender)
          apnd.setLayout(loglayout);
      }
    }
    log.debug("KPWDAuthorizationPlugin: authRequestID " + authRequestID + " Plugin now loaded: dcache.kpwd");
	}

  public KPWDAuthorizationPlugin(String kAuthFilePath)
  throws AuthorizationServiceException {
    this.kAuthFilePath = kAuthFilePath;
    log.debug("KPWDAuthorizationPlugin: now loaded: dcache.kpwd Plugin");
  }

	public KPWDAuthorizationPlugin()
	throws AuthorizationServiceException {
		log.debug("KPWDAuthorizationPlugin: now loaded: dcache.kpwd Plugin");
	}

  public void setLogLevel	(String level) {
    log.setLevel(Level.toLevel(level));
  }

  private void debug(String s) {
    log.debug("KPWDAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void say(String s) {
    log.info("KPWDAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void warn(String s) {
    log.warn("KPWDAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void esay(String s) {
    log.error("KPWDAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
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
				String denied = DENIED_MESSAGE + ": Cannot determine Username for DN " + subjectDN;
        warn(denied);
        throw new AuthorizationServiceException(denied);
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
			throw new AuthorizationServiceException("User " +user_name+ " is not found in authorization records");
      //return null;
    }
		
		if (!authRecord.hasSecureIdentity(subjectDN)) {
			esay("dcache.kpwd Authorization Service plugin: Authorization denied for user: "+user_name + " with subject DN: " +subjectDN);
			throw new AuthorizationServiceException("dcache.kpwd Authorization Plugin: Authorization denied for user " +user_name+ " with Subject DN " +subjectDN);
      //return null;
    }

    authRecord.DN = subjectDN;

    return authRecord;
	}	

} //end of class KPWDAuthorizationPlugin
