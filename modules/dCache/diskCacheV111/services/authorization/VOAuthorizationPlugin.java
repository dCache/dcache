// $Id: VOAuthorizationPlugin.java,v 1.28 2007-10-23 17:11:24 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.27  2007/10/19 20:50:02  tdh
// Merged caching of saml-vo-mapping.
//
// Revision 1.26  2007/03/27 19:20:28  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.25  2007/02/08 20:27:46  tdh
// Added code for dynamic mapping of uid, gid, etc.
//
// Revision 1.24  2007/01/12 19:30:13  tdh
// Added static strings DENIED_MESSAGE and REVOCATION_MESSAGE to AuthorizationServicePlugin.
// GPLAZMALiteVORoleAuthzPlugin will throw REVOCATION_MESSAGE for "-" mapping.
// Plugins will throw exceptions starting with DENIED_MESSAGE for null mappings.
// Changed log level of permission denied to warning in plugins.
//
// Revision 1.23  2007/01/04 17:46:48  tdh
// Made authRequestID instance, rather than static, variable for threading.
//
// Revision 1.22  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationService.
//
// Revision 1.21  2006/12/15 15:52:57  tdh
// Added check for default priority.
//
// Revision 1.20  2006/12/07 20:28:50  tdh
// Added static method to extract FQAN from context. Authorization using context calls
// this method, then queries gPlazma using group and role; SRM and gridftp no longer
// delegate credentials to gPlamza. Static method replaced code in plugins.
//
// Revision 1.19  2006/11/30 19:19:46  tdh
// Make error message more clear when a user is not found in storage-authzdb.
//
// Revision 1.18  2006/11/29 19:09:38  tdh
// Added debug log level and changed logging lines.
//
// Revision 1.17  2006/11/28 21:12:35  tdh
// Added setLogLevel function to interface and implemented in plugins. Made all plugins use log4j.
//
// Revision 1.16  2006/08/24 21:12:13  tdh
// Added priority entry to storage-authdb line and associated field in UserAuthBase.
//
// Revision 1.15  2006/08/23 16:49:22  tdh
// Improved logging.
//
// Revision 1.14  2006/08/02 19:39:28  tdh
// Added static block to set ACTrustStore designating /etc/grid-security/certificates instead of .../vomsdir.
//
// Revision 1.13  2006/08/01 19:42:26  tdh
// For authorize by context, extract DN and Role, and then call that method.
// CVS ----------------------------------------------------------------------
//
// Revision 1.12  2006/07/25 15:19:35  tdh
// Added method to authenticate by DN/Role, getting target name from host credentials.
//
// Revision 1.11.2.1  2006/07/12 19:50:21  tdh
// Added method for authenticating vs DN and role.
//
// Revision 1.11  2006/07/06 13:53:08  tdh
// Fixed non-compiling development lines.
//
// Revision 1.10  2006/07/05 17:16:00  tdh
// Added DN authorization method and comment.
//
// Revision 1.9  2006/07/05 17:08:44  tdh
// Started modifications to authorize by DN rather than context.
//
// Revision 1.8  2006/07/03 21:14:52  tdh
// Removed commented-out lines of code.
//
// Revision 1.7  2006/07/03 19:56:51  tdh
// Added code to throw and/or catch AuthenticationServiceExceptions from GPLAZMA cell.
//
// Revision 1.6  2006/06/29 20:15:01  tdh
// Made AuthorizationServicePlugin an abstract class and added another authorize method.
//
// Revision 1.5  2006/06/09 16:09:27  tdh
// Check uid, gid, etc for null.
//

/*
 * VOAuthorizationPlugin.java
 * 
 * Created on January 29, 2005
 */

package diskCacheV111.services.authorization;

import java.util.*;
import java.lang.*;
import java.net.Socket;
import java.net.URL;

import diskCacheV111.util.*;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;

import org.gridforum.jgss.ExtendedGSSContext;
import org.opensciencegrid.authz.client.*;
import org.opensciencegrid.authz.common.LocalId;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.apache.log4j.*;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public class VOAuthorizationPlugin extends AuthorizationServicePlugin {

	private String mappingServiceURL;
  private long authRequestID=0;
	private URL mappingServiceURLobject;
  private String targetServiceName;
  private static final String service_key  = "/etc/grid-security/hostkey.pem";
  private static final String service_cert = "/etc/grid-security/hostcert.pem";
	//UserAuthRecord VORecord;
	GSSContext context;
	String desiredUserName;
	String serviceUrl;
	Socket socket;
  static Logger log = Logger.getLogger(VOAuthorizationPlugin.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);
  private static HashMap<String, TimedLocalId> UsernameMap = new HashMap();
  long cache_lifetime=0;

  public VOAuthorizationPlugin(String mappingServiceURL, long authRequestID)
	throws AuthorizationServiceException {
		this.mappingServiceURL = mappingServiceURL;
    this.authRequestID=authRequestID;
    if(((Logger)log).getAppender("VOAuthorizationPlugin")==null) {
      Enumeration appenders = log.getParent().getAllAppenders();
      while(appenders.hasMoreElements()) {
        Appender apnd = (Appender) appenders.nextElement();
        if(apnd instanceof ConsoleAppender)
          apnd.setLayout(loglayout);
      }
    }
    log.debug("VOAuthorizationPlugin: authRequestID " + authRequestID + " Plugin now loaded: saml-vo-mapping");
	}

	public VOAuthorizationPlugin(String mappingServiceURL)
	throws AuthorizationServiceException {
		log.debug("VOAuthorizationPlugin: now loaded: saml-vo-mapping Plugin");
		this.mappingServiceURL = mappingServiceURL;
	}
	
	public VOAuthorizationPlugin()
	throws AuthorizationServiceException {
		log.debug("VOAuthorizationPlugin: now loaded: saml-vo-mapping Plugin");
	}

  public void setLogLevel	(String level) {
    log.setLevel(Level.toLevel(level));
  }

  private void debug(String s) {
    log.debug("VOAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void say(String s) {
    log.info("VOAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void warn(String s) {
    log.warn("VOAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void esay(String s) {
    log.error("VOAuthorizationPlugin: authRequestID " + authRequestID + " " + s);
  }

  public String getTargetServiceName() throws GSSException {

      if(targetServiceName==null) {
        GlobusCredential serviceCredential;
        try {
          serviceCredential =new GlobusCredential(
          service_cert,
          service_key
          );
        } catch(GlobusCredentialException gce) {
          throw new GSSException(GSSException.NO_CRED , 0,
          "could not load host globus credentials " + gce.toString());
        }

        targetServiceName = serviceCredential.getIdentity();
      }

      return targetServiceName;
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

    PRIMAAuthzModule authVO = null;
		this.desiredUserName = desiredUserName;
    LocalId localId;
    String serviceName;

		try {
			 serviceName = getTargetServiceName();
		}
		catch (Exception e) {
			esay("Exception in finding targetServiceName : " + e);
			throw new AuthorizationServiceException (e.toString());
		}

    String key = subjectDN;
    if(cache_lifetime>0) {
      key = (role==null) ? key : key.concat(role);
      TimedLocalId tlocalId = getUsernameMapping(key);
      if( tlocalId!=null && tlocalId.age() < cache_lifetime &&
        tlocalId.sameServiceName(serviceName) &&
        tlocalId.sameDesiredUserName(desiredUserName)) {
          say("Using cached mapping for User with DN: " + subjectDN + " and Role " + role);
		      debug("with Desired user name: " + desiredUserName);

          return getUserAuthRecord(tlocalId.getLocalId(), subjectDN, role);
      }
    }

    say("Requesting mapping for User with DN: " + subjectDN + " and Role " + role);
		debug("with Desired user name: " + desiredUserName);

		debug("Mapping Service URL configuration: " + mappingServiceURL);
		try {
			mappingServiceURLobject = new URL(mappingServiceURL);
			authVO = new PRIMAAuthzModule(mappingServiceURLobject);
		}
		catch (Exception e) {
			esay("Exception in VO mapping client instantiation: " + e);
			throw new AuthorizationServiceException (e.toString());
		}

    try {
			localId = authVO.mapCredentials(subjectDN, role, serviceName, desiredUserName);
		}
		catch (Exception e ) {
			esay(" Exception occurred in mapCredentials: " + e);
			//e.printStackTrace();
			throw new AuthorizationServiceException (e.toString());
		}

    if (localId == null) {
		  String denied = DENIED_MESSAGE + ": No mapping retrieved service for DN " + subjectDN + " and role " + role;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    if(cache_lifetime>0) putUsernameMapping(key, new TimedLocalId(localId, serviceName, desiredUserName));

    return getUserAuthRecord(localId, subjectDN, role);
  }


	public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

		this.context = context;
		this.desiredUserName = desiredUserName;	

		debug("Extracting Subject DN and Role from GSSContext");

    String gssIdentity;
    String fqanValue;
    ExtendedGSSContext extendedcontext;
    if (context instanceof ExtendedGSSContext) {
			extendedcontext = (ExtendedGSSContext) context;
		}
		else {
			esay("Received context not instance of ExtendedGSSContext, Plugin exiting ...");
			return null;
		}

    try {
      gssIdentity = context.getSrcName().toString();
      } catch (GSSException gsse) {
      esay("Caught GSSException in getting DN " + gsse);
			return null;
    }

    try {
      Iterator<String> fqans =AuthorizationService.getFQANsFromContext(extendedcontext).iterator();
      fqanValue = fqans.hasNext() ? fqans.next() : "";
    } catch (Exception gsse) {
      esay("Caught Exception in extracting group and role " + gsse);
			return null;
    }


    return authorize(gssIdentity, fqanValue, desiredUserName, serviceUrl, socket);
	}


  private UserAuthRecord getUserAuthRecord(LocalId localId, String subjectDN, String role) throws AuthorizationServiceException {

    UserAuthRecord VORecord = null;
    String user;

    user=localId.getUserName();
    if(user==null) {
      String denied = DENIED_MESSAGE + ": non-null user record received, but with a null username";
      warn(denied);
      throw new AuthorizationServiceException(denied);
    } else {
      say("VO mapping service returned Username: " + user);
    }

    Integer uid = localId.getUID();
    if(uid==null) {
      String denied = DENIED_MESSAGE + ": uid not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    Integer gid = localId.getGID();
    if(gid==null) {
      String denied = DENIED_MESSAGE + ": gid not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		String home = localId.getRelativeHomePath();
    if(home==null) {
      String denied = DENIED_MESSAGE + ": relative home path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		String root = localId.getRootPath();
    if(root==null) {
      String denied = DENIED_MESSAGE + ": root path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		String fsroot = localId.getFSRootPath();
    if(fsroot==null) {
      String denied = DENIED_MESSAGE + ": fsroot path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    //int priority = VORecord.priority;
    int priority=0;
    String priority_str = localId.getPriority();
    if(priority_str==null) {
      String denied = DENIED_MESSAGE + ": priority not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    } else {
      try {
        priority = Integer.valueOf(priority_str).intValue();
      } catch (Exception e) {
        if(!priority_str.equals("default")) {
        String denied = DENIED_MESSAGE + ": priority for user " + user + " could not be parsed to an integer";
        warn(denied);
        throw new AuthorizationServiceException(denied);
        }
      }
    }

    boolean readonlyflag = localId.getReadOnlyFlag();
		//todo Following to be used later, currently String type "default" is returned from VO mapping
		//int priority = Integer.parseInt(localId.getPriority());

    debug("Plugin now forming user authorization records...");
    HashSet principals = new HashSet();

    VORecord = new UserAuthRecord(user, subjectDN, role, readonlyflag, priority, uid.intValue(), gid.intValue(), home, root, fsroot, principals);
    if (VORecord.isValid()) {
		  debug("User authorization record has been formed and is valid.");
		}

    return VORecord;
  }

  public void setCacheLifetime(String lifetime_str) {
    if(lifetime_str==null || lifetime_str.length()==0) return;
    try {
      setCacheLifetime(Long.decode(lifetime_str).longValue()*1000);
    } catch (NumberFormatException nfe) {
      esay("Could not format saml-vo-mapping-cache-lifetime=" + lifetime_str + " as long integer.");
    }
  }

  public void setCacheLifetime(long lifetime) {
    cache_lifetime = lifetime;
  }

  private synchronized void putUsernameMapping(String key, TimedLocalId tlocalId) {
    UsernameMap.put(key, tlocalId);
  }

  private synchronized TimedLocalId getUsernameMapping(String key) {
    return UsernameMap.get(key);
  }

  private class TimedLocalId extends LocalId {
    LocalId id;
    long timestamp;
    String serviceName=null;
    String desiredUserName=null;

    TimedLocalId(LocalId id) {
      this.id=id;
      this.timestamp=System.currentTimeMillis();
    }

    TimedLocalId(LocalId id, String serviceName, String desiredUserName) {
      this(id);
      this.serviceName=serviceName;
      this.desiredUserName=desiredUserName;
    }

    private LocalId getLocalId() {
      return id;
    }

    private long age() {
      return System.currentTimeMillis() - timestamp;
    }

    private boolean sameServiceName(String requestServiceName) {
      return (serviceName.equals(requestServiceName));
    }

    private boolean sameDesiredUserName(String requestDesiredUserName) {
      if(desiredUserName==null && requestDesiredUserName==null) return true;
      return (desiredUserName.equals(requestDesiredUserName));
    }
  }

} //end of class VOAuthorizationPlugin
