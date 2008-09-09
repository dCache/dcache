// $Id: GPLAZMALiteVORoleAuthzPlugin.java,v 1.20 2007-04-17 21:46:15 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.19  2007/03/27 19:20:28  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.18  2007/02/08 20:27:45  tdh
// Added code for dynamic mapping of uid, gid, etc.
//
// Revision 1.17  2007/01/12 19:30:13  tdh
// Added static strings DENIED_MESSAGE and REVOCATION_MESSAGE to AuthorizationServicePlugin.
// GPLAZMALiteVORoleAuthzPlugin will throw REVOCATION_MESSAGE for "-" mapping.
// Plugins will throw exceptions starting with DENIED_MESSAGE for null mappings.
// Changed log level of permission denied to warning in plugins.
//
// Revision 1.16  2007/01/04 17:46:48  tdh
// Made authRequestID instance, rather than static, variable for threading.
//
// Revision 1.15  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationService.
//
// Revision 1.14  2006/12/07 20:28:49  tdh
// Added static method to extract FQAN from context. Authorization using context calls
// this method, then queries gPlazma using group and role; SRM and gridftp no longer
// delegate credentials to gPlamza. Static method replaced code in plugins.
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
import java.util.regex.Pattern;
import java.lang.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import diskCacheV111.util.*;
import org.dcache.auth.UserAuthRecord;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.gridforum.jgss.ExtendedGSSContext;
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

  static Logger log = Logger.getLogger(GPLAZMALiteVORoleAuthzPlugin.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);
  //private DateFormat _df   = new SimpleDateFormat("MM/dd HH:mm:ss" );

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
    } catch (Exception e) {
      esay("Caught Exception in extracting group and role " + e);
			return null;
    }

    return authorize(gssIdentity, fqanValue, desiredUserName, serviceUrl, socket);
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
  throws AuthorizationServiceException {

    String user_name=null;
    if(role == null) {
    	role = "";
    }
    String gridFineGrainIdentity = subjectDN.concat(role);
    debug("Using grid-vorolemap configuration: " +gridVORoleMapPath);
		debug("Using storage-authzdb configuration: " +storageAuthzPath);

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

	  if (desiredUserName == null) say("Desired Username not requested. Will attempt a mapping.");
    // Do even if username is requested, in order to check blacklist
    try {
      say("Requesting mapping for User with DN and role: " + gridFineGrainIdentity);
      user_name = mapUsername(gridVORolemapServ, gridFineGrainIdentity);
      if(user_name==null) {
        gridFineGrainIdentity = "*".concat(role);
        user_name = mapUsername(gridVORolemapServ, gridFineGrainIdentity);
      }
		} catch(Exception e) {
			throw new AuthorizationServiceException(e.toString());
		}
		if (desiredUserName != null) say("Subject DN + Grid Vo Role are mapped to Username: " + user_name);

    if (user_name == null) {
			String denied = DENIED_MESSAGE + ": Cannot determine Username from grid-vorolemap for DN " + subjectDN + " and role " + role;
      warn(denied);
      throw new AuthorizationServiceException(denied);
		}
    if (user_name.equals("-")) {
		  String why = ": for DN " + subjectDN + " and role " + role;
		  throw new AuthorizationServiceException(REVOCATION_MESSAGE + why);
	  }

    if (desiredUserName != null) {
			debug("Desired Username requested as: " +desiredUserName);
      try {
        user_name = mapUsername(gridVORolemapServ, gridFineGrainIdentity, desiredUserName);
        if(user_name==null) {
          gridFineGrainIdentity = "*".concat(role);
          user_name = mapUsername(gridVORolemapServ, gridFineGrainIdentity, desiredUserName);
        }
      } catch(Exception e) {
				throw new AuthorizationServiceException(e.toString());
			}
      if (user_name == null) {
        String denied = DENIED_MESSAGE + ": Requested username " + desiredUserName + " not found for " + subjectDN + " and role " + role;
        warn(denied);
        throw new AuthorizationServiceException(denied);
		  }
      if (user_name.equals("-")) {
		    String why = ": for DN " + subjectDN + " and role " + role;
		    throw new AuthorizationServiceException(REVOCATION_MESSAGE + why);
	    }
    }

    authRecord = storageRecordsServ.getStorageUserRecord(user_name);

		if (authRecord == null) {
      esay("A null record was received from the storage authorization service.");
      return nullGridVORoleRecord(subjectDN, role);
    }

    if(authRecord instanceof DynamicAuthorizationRecord) {
      DynamicAuthorizationRecord dynrecord = (DynamicAuthorizationRecord) authRecord;
      dynrecord.subjectDN = subjectDN;
      dynrecord.role = role;
      authRecord = getDynamicRecord(user_name, dynrecord);
    }

    String  user=authRecord.Username;
    if(user==null) throw new AuthorizationServiceException("received null username");

    int priority = authRecord.priority;

    int uid = authRecord.UID;
    if(uid==-1) throw new AuthorizationServiceException("uid not found for " + user);

    int gid = authRecord.GID;
    if(gid==-1) throw new AuthorizationServiceException("gid not found for " + user);

		String home = authRecord.Home;
    if(home==null) {
      throw new AuthorizationServiceException("relative home path not found for " + user);
    }

		String root = authRecord.Root;
    if(root==null) {
      throw new AuthorizationServiceException("root path not found for " + user);
    }

		String fsroot = authRecord.FsRoot;
    if(fsroot==null) throw new AuthorizationServiceException("fsroot path not found for " + user);

    boolean readonlyflag = authRecord.ReadOnly;
		//todo Following to be used later, currently String type "default" is returned from VO mapping
		//int priority = Integer.parseInt(localId.getPriority());

    debug("Plugin now forming user authorization records...");
    HashSet principals = new HashSet();

    authRecordtoReturn = new UserAuthRecord(user, subjectDN, role, readonlyflag, priority, uid, authRecord.GIDs, home, root, fsroot, principals);
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

  private String mapUsername(gPLAZMAliteGridVORoleAuthz gridVORolemapServ, String gridFineGrainIdentity, String username) throws Exception {
    String user_name;
    try {
        user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity, username);
        if(user_name==null) {
          // Remove "/Capability=NULL" and "/Role=NULL"
          if(gridFineGrainIdentity.endsWith(capnull))
            gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - capnulllen);
          if(gridFineGrainIdentity.endsWith(rolenull))
            gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - rolenulllen);
          user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity, username);
        }
    } catch (Exception e) {
      throw e;
    }

    return user_name;
  }

} //end of class GPLAZMALiteVORoleAuthzPlugin
