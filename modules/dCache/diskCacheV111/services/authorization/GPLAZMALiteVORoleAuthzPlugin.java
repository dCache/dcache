// $Id: GPLAZMALiteVORoleAuthzPlugin.java,v 1.2.2.8 2007-02-28 23:10:38 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.2.2.7  2007/01/04 18:13:14  tdh
// Made authRequestID instance, rather than static, variable for threading.
// Previous code might cause incorrect IDs to be printed in log - would not affect authorization.
//
// Revision 1.2.2.6  2007/01/04 11:59:36  omsynge
// Some bug fixes to correct the build for java 1.4
//
// Revision 1.2.2.5  2006/11/30 21:49:57  tdh
// Backport of rationalization of logging.
//
// Revision 1.13  2006/11/30 19:19:46  tdh
// Make error message more clear when a user is not found in storage-authzdb.
//
// Revision 1.12  2006/11/29 19:09:36  tdh
// Added debug log level and changed logging lines.
//
// Revision 1.11  2006/11/28 21:12:35  tdh
// Added setLogLevel function to interface and implemented in plugins. Made all plugins use log4j.
//
// Revision 1.10  2006/09/05 14:10:58  tigran
// gPlazma support for dcap
//
// Revision 1.9  2006/08/24 21:12:13  tdh
// Added priority entry to storage-authdb line and associated field in UserAuthBase.
//
// Revision 1.8  2006/08/23 16:45:11  tdh
// Improved logging and exception handling. Added authorization by DN only. Added revocation support.
//
// Revision 1.7  2006/08/07 16:58:56  tdh
// Added role field to new UserAuthRecord construction.
//
// Revision 1.6  2006/08/07 16:38:03  tdh
// Merger of changes from branch, exception handling and ignore blank config file lines.
//
// Revision 1.5  2006/07/26 18:49:57  tdh
// Fixed use Integer rather than int for field from StorageAuthorizationBase.
//
// Revision 1.4  2006/07/25 15:11:50  tdh
// Actual changes of previous commit.
//

/*
 * GPLAZMALiteVORoleAuthzPlugin.java
 * 
 * Created on March 30, 2005
 */

package diskCacheV111.services.authorization;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
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
//import org.opensciencegrid.authz.ac.FQAN;
import org.globus.gsi.gssapi.GSSConstants;
import org.glite.security.voms.VOMSValidator;
import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.BasicVOMSTrustStore;
import org.glite.security.voms.ac.AttributeCertificate;
import org.apache.log4j.*;

import gplazma.gplazmalite.gridVORolemapService.*;
import gplazma.gplazmalite.storageauthzdbService.*;

/**
 *
 * @author Abhishek Singh Rana, Ted Hesselroth
 */

public class GPLAZMALiteVORoleAuthzPlugin extends AuthorizationServicePlugin {

	private String gridVORoleMapPath;
	private String storageAuthzPath;
  private long authRequestID=0;
	UserAuthRecord authRecordtoReturn;
	StorageAuthorizationRecord authRecord;
	GSSContext context;
	String desiredUserName;

  public static final String capnull = "/Capability=NULL";
  public static final int capnulllen = capnull.length();
  public static final String rolenull ="/Role=NULL";
	public static final int rolenulllen = rolenull.length();

  static Logger log = Logger.	getLogger(VOAuthorizationPlugin.class.getName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);
  //private DateFormat _df   = new SimpleDateFormat("MM/dd HH:mm:ss" );

  static {
    VOMSValidator.setTrustStore(new BasicVOMSTrustStore("/etc/grid-security/certificates", 12*3600*1000));
  }

	public GPLAZMALiteVORoleAuthzPlugin(String gridVORoleMapPath, String storageAuthzPath, long authRequestID)
	throws AuthorizationServiceException {
		this.gridVORoleMapPath = gridVORoleMapPath;
		this.storageAuthzPath = storageAuthzPath;
    this.authRequestID=authRequestID;
    if(((Logger)log).getAppender("GPLAZMALiteVORoleAuthzPlugin")==null) {
      Enumeration appenders = log.getParent().getAllAppenders();
      while(appenders.hasMoreElements()) {
        Appender apnd = (Appender) appenders.nextElement();
        if(apnd instanceof ConsoleAppender)
          apnd.setLayout(loglayout);
      }
    }
    log.debug("GPLAZMALiteVORoleAuthzPlugin: authRequestID " + authRequestID + " Plugin now loaded: gplazmalite-vorole-mapping");
	}

	public GPLAZMALiteVORoleAuthzPlugin(String gridVORoleMapPath, String storageAuthzPath)
	throws AuthorizationServiceException {
		this.gridVORoleMapPath = gridVORoleMapPath;
		this.storageAuthzPath = storageAuthzPath;
    log.debug("GPLAZMALiteVORoleAuthzPlugin: now loaded: gplazmalite-vorole-mapping Plugin");
  }

  public void setLogLevel	(String level) {
    log.setLevel(Level.toLevel(level));
  }

  private void debug(String s) {
    log.debug("GPLAZMALiteVORoleAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void say(String s) {
    log.info("GPLAZMALiteVORoleAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void warn(String s) {
    log.warn("GPLAZMALiteVORoleAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void esay(String s) {
    log.error("GPLAZMALiteVORoleAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

    String gssIdentity=null;
    String fqanValue=null;
    ExtendedGSSContext extendedcontext;
    if (context instanceof ExtendedGSSContext) {
			extendedcontext = (ExtendedGSSContext) context;
		}
		else {
			esay("Received context not instance of ExtendedGSSContext, Plugin exiting...");
			return null;
		}

    X509Certificate[] chain;
    try {
      chain = (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
    } catch (GSSException gsse) {
      esay("Caught GSSException in getting cerficate chain " + gsse);
			return null;
    }

    try {
      gssIdentity = context.getSrcName().toString();
      } catch (GSSException gsse) {
      esay("Caught GSSException in getting DN " + gsse);
			return null;
    }

    VOMSValidator validator = new VOMSValidator(chain);
    //Vector vectorOfAttributes = validator.parse(chain); //for newer glite
    validator.parse();
    List listOfAttributes = validator.getVOMSAttributes();

            // Getting first FQAN only
            Iterator i = listOfAttributes.iterator();
            if (i.hasNext()) {
                VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
                AttributeCertificate ac = vomsAttribute.getAC();
                String issuer = ac.getIssuer().toString();
                debug("VOMS Server is '" + issuer + "'");
                List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
                Iterator j = listOfFqans.iterator();
                if (j.hasNext()) {
                    fqanValue = (String) j.next();
                    debug("FQAN is '" + fqanValue + "'");
                } else {
                    debug("No FQAN found");
                }
            } else {
                debug("No attribute found");
            }

		//requestedServiceName = X509NameHelper.toString(new X509Name(gssContext.getTargName().toString()) );


    return authorize(gssIdentity, fqanValue, desiredUserName, serviceUrl, socket);
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
  throws AuthorizationServiceException {

    String user_name=null;
    if(role == null) {
    	role = "";
    }
    String gridFineGrainIdentity = subjectDN.concat(role);
    debug("Using grid-vorolemap configuration: " + gridVORoleMapPath);
		debug("Using storage-authzdb configuration: " + storageAuthzPath);

    gPLAZMAliteGridVORoleAuthz gridVORolemapServ = null;
		DCacheSRMauthzRecordsService storageRecordsServ = null;

		try {
			gridVORolemapServ = new gPLAZMAliteGridVORoleAuthz(gridVORoleMapPath);
			storageRecordsServ = new DCacheSRMauthzRecordsService(storageAuthzPath);
		} catch(Exception ase) {
			esay("Exception in reading gplazmalite-vorole-mapping configuration files: ");
      esay(gridVORoleMapPath + " or " + storageAuthzPath + " " + ase);
			throw new AuthorizationServiceException (ase.toString());
		}

		if (desiredUserName != null) {
			debug("Desired Username requested as: " +desiredUserName);
			user_name = desiredUserName;
		}
		else {
			say("Desired Username not requested. Will attempt a mapping.");
			try {
        say("Requesting mapping for User with DN and role: " + gridFineGrainIdentity);
        user_name = mapUsername(gridVORolemapServ, gridFineGrainIdentity);
        if(user_name==null) {
          gridFineGrainIdentity = "*".concat(role);
          user_name = mapUsername(gridVORolemapServ, gridFineGrainIdentity);
        }
			}
			catch(Exception e) {
				throw new AuthorizationServiceException(e.toString());
			}			
			say("Subject DN + Grid Vo Role are mapped to Username: " + user_name);
			if (user_name == null) {
				esay("Cannot determine Username from Subject DN + Grid Vo Role");
				esay("as: " + gridFineGrainIdentity);
				throw new AuthorizationServiceException("Permission Denied.");
			}
      if (user_name.equals("-")) {
				esay("Subject DN + Grid Vo Role found in revocation list");
				esay("as: " + gridFineGrainIdentity);
				throw new AuthorizationServiceException("Permission Denied - revocation.");
			}
		}

    authRecord = storageRecordsServ.getStorageUserRecord(user_name);

		if (authRecord == null) {
      esay("A null record was received from the storage authorization service.");
      return nullGridVORoleRecord(subjectDN, role);
    }

    String  user=authRecord.Username;
    if(user==null) {
      throw new AuthorizationServiceException("A non-null user record received, but with a null username!");
    }

    //int priority = authRecord.priority;

    int uid = authRecord.UID;
    if(uid==-1) {
      throw new AuthorizationServiceException("uid not found for " + user);
    }

    int gid = authRecord.GID;
    if(gid==-1) {
      throw new AuthorizationServiceException("gid not found for " + user);
    }

		String home = authRecord.Home;
    if(home==null) {
      throw new AuthorizationServiceException("relative home path not found for " + user);
    }

		String root = authRecord.Root;
    if(root==null) {
      throw new AuthorizationServiceException("root path not found for " + user);
    }

		String fsroot = authRecord.FsRoot;
    if(fsroot==null) {
      throw new AuthorizationServiceException("fsroot path not found for " + user);
    }

    boolean readonlyflag = authRecord.ReadOnly;
		//todo Following to be used later, currently String type "default" is returned from VO mapping
		//int priority = Integer.parseInt(localId.getPriority());

    debug("Plugin now forming user authorization records...");
    HashSet principals = new HashSet();

    authRecordtoReturn = new UserAuthRecord(user, readonlyflag, uid, gid, home, root, fsroot, principals);
    if (authRecordtoReturn.isValid()) {
		  debug("User authorization record has been formed and is valid.");
		}

		return authRecordtoReturn;
	}

  private UserAuthRecord nullGridVORoleRecord(String subjectDN, String role) {
    if (authRecord == null) {
			warn("Grid VO Role Authorization Service plugin: Authorization denied for user");
			warn("with subject DN: " + subjectDN + " and role " + role);
    }

    return null;
  }


  private String mapUsername(gPLAZMAliteGridVORoleAuthz gridVORolemapServ, String gridFineGrainIdentity) throws Exception {
		String user_name;
    try {
        user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity);
        if(user_name==null) {
          // Remove "/Capability=NULL" and "/Role=NULL"
          if(gridFineGrainIdentity.endsWith(capnull))
            gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - capnulllen);
          if(gridFineGrainIdentity.endsWith(rolenull))
            gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - rolenulllen);
          user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity);
        }
    } catch (Exception e) {
      throw e;
    }

    return user_name;
  }

} //end of class GPLAZMALiteVORoleAuthzPlugin
