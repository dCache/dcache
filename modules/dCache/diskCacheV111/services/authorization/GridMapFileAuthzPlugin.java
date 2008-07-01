// $Id: GridMapFileAuthzPlugin.java,v 1.15 2007-04-17 21:46:15 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.14  2007/02/08 20:27:45  tdh
// Added code for dynamic mapping of uid, gid, etc.
//
// Revision 1.13  2007/01/12 19:30:13  tdh
// Added static strings DENIED_MESSAGE and REVOCATION_MESSAGE to AuthorizationServicePlugin.
// GPLAZMALiteVORoleAuthzPlugin will throw REVOCATION_MESSAGE for "-" mapping.
// Plugins will throw exceptions starting with DENIED_MESSAGE for null mappings.
// Changed log level of permission denied to warning in plugins.
//
// Revision 1.12  2007/01/04 17:46:48  tdh
// Made authRequestID instance, rather than static, variable for threading.
//
// Revision 1.11  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationService.
//
// Revision 1.10  2006/11/30 19:19:46  tdh
// Make error message more clear when a user is not found in storage-authzdb.
//
// Revision 1.9  2006/11/29 19:09:38  tdh
// Added debug log level and changed logging lines.
//
// Revision 1.8  2006/11/28 21:12:35  tdh
// Added setLogLevel function to interface and implemented in plugins. Made all plugins use log4j.
//
// Revision 1.7  2006/08/24 21:12:13  tdh
// Added priority entry to storage-authdb line and associated field in UserAuthBase.
//
// Revision 1.6  2006/08/23 16:46:29  tdh
// Improved logging.
//
// Revision 1.5  2006/08/07 16:38:03  tdh
// Merger of changes from branch, exception handling and ignore blank config file lines.
//
// Revision 1.2.2.2  2006/08/07 15:56:59  tdh
// Catch any Exception (not just IOException) and forward as AuthorizationServiceException.
//
// Revision 1.2.2.1  2006/07/26 18:37:40  tdh
// Fixed use Integer rather than int for field from StorageAuthorizationBase.
//
// Revision 1.3  2006/07/25 15:12:23  tdh
// Added method to authenticate by DN/Role.
//

/*
 * GridMapFileAuthzPlugin.java
 *
 * Created on March 30, 2005
 */

package diskCacheV111.services.authorization;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import diskCacheV111.util.*;
import org.dcache.auth.UserAuthRecord;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.apache.log4j.*;

import gplazma.gplazmalite.gridmapfileService.*;
import gplazma.gplazmalite.storageauthzdbService.*;

/**
 *
 * @author Abhishek Singh Rana, Ted Hesselroth
 */

public class GridMapFileAuthzPlugin extends AuthorizationServicePlugin {

	private String gridMapFilePath;
	private String storageAuthzPath;
  private long authRequestID=0;
	UserAuthRecord authRecordtoReturn;
	StorageAuthorizationRecord authRecord;
	GSSContext context;
	String desiredUserName;
  static Logger log = Logger.	getLogger(GridMapFileAuthzPlugin.class.getSimpleName());
  private static String logpattern = "%d{MM/dd HH:mm:ss,SSS} %m%n";
  private static PatternLayout loglayout = new PatternLayout(logpattern);

	public GridMapFileAuthzPlugin(String gridMapFilePath, String storageAuthzPath, long authRequestID)
	throws AuthorizationServiceException {
		this.gridMapFilePath = gridMapFilePath;
		this.storageAuthzPath = storageAuthzPath;
    this.authRequestID=authRequestID;
    if(((Logger)log).getAppender("GridMapFileAuthzPlugin")==null) {
          Enumeration appenders = log.getParent().getAllAppenders();
          while(appenders.hasMoreElements()) {
            Appender apnd = (Appender) appenders.nextElement();
            if(apnd instanceof ConsoleAppender)
              apnd.setLayout(loglayout);
          }
        }
    log.debug("GridMapFileAuthzPlugin: authRequestID " + authRequestID + " Plugin now loaded: grid-mapfile");
  }

	public GridMapFileAuthzPlugin(String gridMapFilePath, String storageAuthzPath)
	throws AuthorizationServiceException {
		this.gridMapFilePath = gridMapFilePath;
		this.storageAuthzPath = storageAuthzPath;
		log.debug("GridMapFileAuthzPlugin: now loaded: grid-mapfile Plugin");
  }

  public void setLogLevel	(String level) {
    log.setLevel(Level.toLevel(level));
  }

  private void debug(String s) {
    log.debug("GridMapFileAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void say(String s) {
    log.info("GridMapFileAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void warn(String s) {
    log.warn("GridMapFileAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

  private void esay(String s) {
    log.error("GridMapFileAuthzPlugin: authRequestID " + authRequestID + " " + s);
  }

	public UserAuthRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
	throws AuthorizationServiceException {

    this.context = context;
    GSSName GSSId;
		String subjectDN;

    try {
			GSSId = context.getSrcName();
			subjectDN = GSSId.toString();
			say("Subject DN from GSSContext extracted as: " +subjectDN);
		}
		catch(org.ietf.jgss.GSSException gsse ) {
			esay(" Error extracting Subject DN from GSSContext: " +gsse);
			throw new AuthorizationServiceException (gsse.toString());
		}

    return authorize(subjectDN, null, desiredUserName, serviceUrl, socket);
  }

  public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
  throws AuthorizationServiceException {

    //say("Using grid-mapfile configuration: " +gridMapFilePath);
    //say("Using storage-authzdb configuration: " +storageAuthzPath);
    GridMapFileAuthzService gridmapServ = null;
    DCacheSRMauthzRecordsService storageRecordsServ = null;
    this.desiredUserName = desiredUserName;
    String user_name;

    try {
      gridmapServ = new GridMapFileAuthzService(gridMapFilePath);
      storageRecordsServ = new DCacheSRMauthzRecordsService(storageAuthzPath);
    }
    catch(Exception ase) {
			esay("Exception in reading grid-mapfile configuration files: ");
      esay(gridMapFilePath + " or " + storageAuthzPath + " " + ase);
			throw new AuthorizationServiceException (ase.toString());
		}

		if (desiredUserName != null) {
			debug("Desired Username requested as: " +desiredUserName);
		}

		try {
			user_name = gridmapServ.getMappedUsername(subjectDN);
		}
		catch(Exception e) {
			throw new AuthorizationServiceException(e.toString());
		}
		say("Subject DN is mapped to Username: " + user_name);

    if (user_name == null) {
		  String denied = DENIED_MESSAGE + ": Cannot determine Username from grid-mapfile for DN " + subjectDN;
      warn(denied);
      throw new AuthorizationServiceException(denied);
		}
	  if (desiredUserName != null && !user_name.equals(desiredUserName)) {
		  String denied = DENIED_MESSAGE + ": Requested username " + desiredUserName + " does not match returned username " + user_name + " for " + subjectDN;
      warn(denied);
      throw new AuthorizationServiceException(denied);
	  }

    authRecord = storageRecordsServ.getStorageUserRecord(user_name);

		if (authRecord == null) {
      esay("No mapping retrieved from grid-mapfile service.");
      return nullGridMapRecord(subjectDN, role);
    }

    String  user=authRecord.Username;
    if(user==null) {
      throw new AuthorizationServiceException("A non-null record received, but with a null username!");
    }

    int priority = authRecord.priority;

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
    HashSet<String> principals = new HashSet<String>();

    authRecordtoReturn = new UserAuthRecord(user, subjectDN, null, readonlyflag, priority, uid, gid, home, root, fsroot, principals);
    if (authRecordtoReturn.isValid()) {
		  debug("User authorization record has been formed and is valid.");
		}

		return authRecordtoReturn;
	}


  private UserAuthRecord nullGridMapRecord(String subjectDN, String role) {
    if (authRecord == null) {
			warn("grid-mapfile plugin: Authorization denied for user");
			warn("with subject DN: " + subjectDN + " and role " + role);
    }

    return null;
  }

} //end of class GridMapFileAuthzPlugin
