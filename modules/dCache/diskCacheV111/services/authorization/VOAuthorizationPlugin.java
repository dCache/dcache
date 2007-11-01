// $Id: VOAuthorizationPlugin.java,v 1.11.4.9 2007-10-11 02:36:06 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11.4.8  2007/10/10 18:01:55  tdh
// Added code for caching of saml-vo mapping.
//
// Revision 1.11.4.7  2007/02/28 23:10:38  tdh
// Transmit exception messages to calling cell.
//
// Revision 1.11.4.6  2007/01/04 18:13:15  tdh
// Made authRequestID instance, rather than static, variable for threading.
// Previous code might cause incorrect IDs to be printed in log - would not affect authorization.
//
// Revision 1.11.4.5  2007/01/04 11:59:36  omsynge
// Some bug fixes to correct the build for java 1.4
//
// Revision 1.11.4.4  2006/12/11 18:06:16  tdh
// "No attribute found" should have log level debug, not error.
// Found way to turn off "Client ... accepted." message.
//
// Revision 1.11.4.3  2006/11/30 21:49:58  tdh
// Backport of rationalization of logging.
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
import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import diskCacheV111.util.*;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;

import org.gridforum.jgss.ExtendedGSSContext;
import org.opensciencegrid.authz.client.*;
import org.opensciencegrid.authz.common.LocalId;
import org.opensciencegrid.authz.*;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.gssapi.GSSConstants;
import org.glite.security.voms.VOMSValidator;
import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.BasicVOMSTrustStore;
import org.glite.security.voms.ac.AttributeCertificate;
import org.glite.security.voms.ac.ACTrustStore;
import org.apache.log4j.*;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public class VOAuthorizationPlugin extends AuthorizationServicePlugin {

	private String mappingServiceURL;
  private long authRequestID=0;
	private static URL mappingServiceURLobject;
  private static String targetServiceName;
  private static final String service_key  = "/etc/grid-security/hostkey.pem";
  private static final String service_cert = "/etc/grid-security/hostcert.pem";
	UserAuthRecord VORecord;
	GSSContext context;
	String desiredUserName;
	String serviceUrl;
	Socket socket;
  static Logger log = Logger.	getLogger(VOAuthorizationPlugin.class.getName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);
  private static HashMap UsernameMap = new HashMap();
  long cache_lifetime=0;

  static {
    VOMSValidator.setTrustStore(new BasicVOMSTrustStore("/etc/grid-security/certificates", 12*3600*1000));
    Logger.getLogger(org.glite.security.trustmanager.CRLFileTrustManager.class.getName()).setLevel(Level.ERROR);
  }

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

  public static String getTargetServiceName() throws GSSException {

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

		debug("Mapping Service URL configuration: " +mappingServiceURL);

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
          return getUserAuthRecord(tlocalId.getLocalId(), role);
      }
    }

		debug("Mapping Service URL configuration: " + mappingServiceURL);
		try {
			mappingServiceURLobject = new URL(mappingServiceURL);
			authVO = new PRIMAAuthzModule(mappingServiceURLobject);
		}
		catch (Exception e) {
			esay("Exception in VO mapping client instantiation: " +e);
			throw new AuthorizationServiceException (e.toString());
		}

    say("Requesting mapping for User with DN: " + subjectDN + " and Role " + role);
		debug("with Desired user name: " + desiredUserName);

    try {
			localId = authVO.mapCredentials(subjectDN, role, serviceName, desiredUserName);
		}
		catch (Exception e ) {
			esay(" Exception occurred in mapCredentials: " +e);
			//e.printStackTrace();
			throw new AuthorizationServiceException (e.toString());
		}

    if (localId == null) {
      //esay("No mapping retrieved from VO identity mapping service.");
      //return nullVORecord(subjectDN, role);
      throw new AuthorizationServiceException("No mapping retrieved for SubjectDN " + subjectDN + " and role " + role);
    }

    if(cache_lifetime>0) UsernameMap.put(key, new TimedLocalId(localId, serviceName, desiredUserName));

    return getUserAuthRecord(localId, role);

  }


	public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

		//PRIMAAuthzModule authVO = null;
		this.context = context;
		this.desiredUserName = desiredUserName;	
		
		//LocalId localId;
		GSSName GSSId;
		String subjectDN;
    String role;
		//GSSName GSSserviceId;
		//String serviceName;
		//String user;
		ExtendedGSSContext extendedcontext;
		
		if (context instanceof ExtendedGSSContext) {
			extendedcontext = (ExtendedGSSContext)context;
		} 
		else {
			esay("Received context not instance of ExtendedGSSContext, Plugin exiting...");
			return null;
		}
		
		debug("Extracting Subject DN and Role from GSSContext");
		
		/*
    try {
			mappingServiceURLobject = new URL(mappingServiceURL);
			authVO = new PRIMAAuthzModule(mappingServiceURLobject);
		}
		catch (Exception e) {
			esay("Exception in VO mapping client instantiation: " +e);
			throw new AuthorizationServiceException (e.toString());
		}
    */
		
		try {
			GSSId = context.getSrcName();
			subjectDN = GSSId.toString();
			//say("Subject DN from GSSContext extracted as: " +subjectDN);
		}
		catch(org.ietf.jgss.GSSException gsse ) {
			esay(" Error extracting Subject DN from GSSContext: " +gsse);
			throw new AuthorizationServiceException (gsse.toString());
		}
		
		/*
    try {
			GSSserviceId = context.getTargName();
			serviceName = GSSserviceId.toString();
			//say("Service Name from GSSContext extracted as: " +serviceName);
		}
		catch(org.ietf.jgss.GSSException gsse ) {
			esay(" Error extracting Service name from GSSContext: " +gsse);
			throw new AuthorizationServiceException (gsse.toString());
		}
    */

    try {
      role = getFQANFromContext(extendedcontext);
    } catch(org.ietf.jgss.GSSException gsse ) {
			esay(" Error extracting Role from GSSContext: " +gsse);
			throw new AuthorizationServiceException (gsse.toString());
		}

    //say("Requesting mapping for service resource: " +serviceName);
		//say("Requesting mapping for User with Subject DN: " + subjectDN + " and Role " + role);
		//say("with Desired user name: " +desiredUserName);


    return authorize(subjectDN, role, desiredUserName, serviceUrl, socket);
	}


  private UserAuthRecord getUserAuthRecord(LocalId localId, String role) throws AuthorizationServiceException {

    VORecord = null;
    String user;

    user=localId.getUserName();
    if(user==null) {
      throw new AuthorizationServiceException("A non-null user record received, but with a null username!");
    } else {
      say("VO mapping service returned Username: " + user);
    }

    Integer uid = localId.getUID();
    if(uid==null) {
      throw new AuthorizationServiceException("uid not found for " + user);
    }

    Integer gid = localId.getGID();
    if(gid==null) {
      throw new AuthorizationServiceException("gid not found for " + user);
    }

		String home = localId.getRelativeHomePath();
    if(home==null) {
      throw new AuthorizationServiceException("relative home path not found for " + user);
    }

		String root = localId.getRootPath();
    if(root==null) {
      throw new AuthorizationServiceException("root path not found for " + user);
    }

		String fsroot = localId.getFSRootPath();
    if(fsroot==null) {
      throw new AuthorizationServiceException("fsroot path not found for " + user);
    }

    //int priority = VORecord.priority;
    /*int priority=0;
    String priority_str = localId.getPriority();
    if(priority_str==null) {
      throw new AuthorizationServiceException("priority not found for " + user);
    } else {
      try {
        priority = Integer.valueOf(priority_str).intValue();
      } catch (Exception e) {
        throw new AuthorizationServiceException("A non-null record received for " + user + ", but priority could not be parsed to an integer!");
      }
    }
    */

    boolean readonlyflag = localId.getReadOnlyFlag();
		//todo Following to be used later, currently String type "default" is returned from VO mapping
		//int priority = Integer.parseInt(localId.getPriority());

    debug("Plugin now forming user authorization records...");
    HashSet principals = new HashSet();

    VORecord = new UserAuthRecord(user, readonlyflag, uid.intValue(), gid.intValue(), home, root, fsroot, principals);
    if (VORecord.isValid()) {
		  debug("User authorization record has been formed and is valid.");
		}

    return VORecord;
  }

  private UserAuthRecord nullVORecord(String subjectDN, String role) {
    if (VORecord == null) {
			warn("VO Authorization Service plugin: Authorization denied for user");
			warn("with subject DN: " + subjectDN + " and role " + role);
    }

    return null;
  }

  public String getFQANFromContext(ExtendedGSSContext gssContext) throws GSSException {
    String fqanValue = null;
    X509Certificate[] chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
    VOMSValidator validator = new VOMSValidator(chain);
    validator.parse();
    List listOfAttributes = validator.getVOMSAttributes();

    // Getting first FQAN only
    Iterator i = listOfAttributes.iterator();
    if (i.hasNext()) {
      VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
      AttributeCertificate ac = vomsAttribute.getAC();
      String issuer = ac.getIssuer().toString();
      debug("VOAuthorizationPlugin: authRequestID " + authRequestID + " VOMS Server is '" + issuer + "'");
      List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
      Iterator j = listOfFqans.iterator();
      if (j.hasNext()) {
      fqanValue = (String) j.next();
      debug("VOAuthorizationPlugin: authRequestID " + authRequestID + " FQAN is '" + fqanValue + "'");
    } else {
      debug("No FQAN found");
    }
    } else {debug("No attribute found");
    }

    return fqanValue;
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
     return (TimedLocalId)UsernameMap.get(key);
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
