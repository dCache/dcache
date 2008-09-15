// $Id: AuthorizationServicePlugin.java,v 1.28 2007-10-23 17:11:24 tdh Exp $
// $Log: not supported by cvs2svn $

package diskCacheV111.services.authorization;

//java
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.io.*;
import java.lang.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.AnnotatedElement;
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
import org.apache.log4j.Logger;
import gplazma.gplazmalite.storageauthzdbService.DynamicAuthorizationRecord;
import gplazma.gplazmalite.storageauthzdbService.StorageAuthorizationRecord;
import gplazma.gplazmalite.storageauthzdbService.DCacheSRMauthzRecordsService;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public abstract class AuthorizationServicePlugin {

  public static final String DENIED_MESSAGE="Permission Denied";
  public static final String REVOCATION_MESSAGE=DENIED_MESSAGE+" - revocation.";
  public static final Class STR_CLASS = "z".getClass();
  public static final Class INT_CLASS = Integer.TYPE;
  public String storageAuthzPath;

  private void debug(String s){}
  private void say(String s){}
  private void warn(String s){}
  private void esay(String s){}

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

  public UserAuthRecord getAuthRecord(String username, String subjectDN, String role) throws AuthorizationServiceException {

    DCacheSRMauthzRecordsService storageRecordsServ;

    try {
      storageRecordsServ = new DCacheSRMauthzRecordsService(storageAuthzPath);
    } catch(Exception ase) {
      esay("Exception in reading storage-authzdb configuration file: ");
      esay(storageAuthzPath + " " + ase);
      throw new AuthorizationServiceException(ase.toString());
    }

    StorageAuthorizationRecord authRecord = storageRecordsServ.getStorageUserRecord(username);

    if (authRecord == null) {
      esay("A null record was received from the storage authorization service.");
      return null;
    }

    if(authRecord instanceof DynamicAuthorizationRecord) {
      DynamicAuthorizationRecord dynrecord = (DynamicAuthorizationRecord) authRecord;
      dynrecord.subjectDN = subjectDN;
      dynrecord.role = role;
      authRecord = getDynamicRecord(username, dynrecord);
    }

    String  user=authRecord.Username; if(user==null) {
      String denied = DENIED_MESSAGE + ": received null username " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    //Integer uid = localId.getUID(); if(uid==null) {
    int uid = authRecord.UID; if(uid==-1) {
      String denied = DENIED_MESSAGE + ": uid not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    //Integer gid = localId.getGID(); if(gid==null) {
    int[] gids = authRecord.GIDs; if(gids[0]==-1) {
      String denied = DENIED_MESSAGE + ": gids not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		//String home = localId.getRelativeHomePath(); if(home==null) {
    String home = authRecord.Home; if(home==null) {
      String denied = DENIED_MESSAGE + ": relative home path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

		//String root = localId.getRootPath(); if(root==null) {
    String root = authRecord.Root; if(root==null) {
      String denied = DENIED_MESSAGE + ": root path not found for " + user;
      warn(denied);
      throw new AuthorizationServiceException(denied);
    }

    String fsroot = authRecord.FsRoot; //if(root==null) {
    int priority = authRecord.priority;

    boolean readonlyflag = authRecord.ReadOnly;

    debug("Plugin now forming user authorization records...");
    HashSet principals = new HashSet();

    UserAuthRecord authRecordtoReturn = new UserAuthRecord(user, subjectDN, role, readonlyflag, priority, uid, gids, home, root, fsroot, principals);
    if (authRecordtoReturn.isValid()) {
      debug("User authorization record has been formed and is valid.");
    }

    return authRecordtoReturn;
  }



  public String getDynamicString(String dynamic_mapper, String id_method, String subjectDN, String role) throws AuthorizationServiceException {
    try {
      return  getDynamicValue(dynamic_mapper, id_method, subjectDN, role).toString();
    } catch (Exception e) {
      throw new AuthorizationServiceException("Method " + id_method + " failed for DynamicMapper " + dynamic_mapper + " for DN " + subjectDN + " and role " + role);
    }
  }

  protected Integer getDynamicInteger(String dynamic_mapper, String id_method, String subjectDN, String role) throws AuthorizationServiceException {
    String value=null;
    try {
      value = getDynamicString(dynamic_mapper, id_method, subjectDN, role);
      return Integer.decode(value);
    } catch (NumberFormatException nfe) {
      throw new AuthorizationServiceException("Method " + id_method + " return value " + value + " not parsable to Integer for DynamicMapper " + dynamic_mapper + " for DN " + subjectDN + " and role " + role);
    }
  }

  private Object getDynamicValue(String dynamic_mapper, String id_method, String subjectDN, String role) throws AuthorizationServiceException {

    Object ret_val;

        try {
          Class DynamicMapper = Class.forName(dynamic_mapper);
          Method meth = DynamicMapper.getMethod(id_method, STR_CLASS, STR_CLASS);
          ret_val = meth.invoke(this, subjectDN, role);
        } catch (ClassNotFoundException cnfe) {
          throw new AuthorizationServiceException("ClassNotFoundException for DynamicMapper " + dynamic_mapper + " for DN " + subjectDN + " and role " + role);
        } catch (NoSuchMethodException nsm) {
          throw new AuthorizationServiceException("NoSuchMethodException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        } catch (InvocationTargetException ite) {
          throw new AuthorizationServiceException("InvocationTargetException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        } catch (IllegalAccessException iac) {
          throw new AuthorizationServiceException("IllegalAccessException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        } catch (NumberFormatException nfe) {
          throw new AuthorizationServiceException("NumberFormatException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        }

    return ret_val;
  }

  public <T> T getDynamicValue(String dynamic_mapper, String uid_method, String subjectDN, String role, Class<T> ct) throws AuthorizationServiceException {
    //return (T) getDynamicValue(dynamic_mapper, uid_method, subjectDN, role);
    return null;
  }

  public StorageAuthorizationRecord getDynamicRecord(String dynamic_mapper, DynamicAuthorizationRecord dynrecord)
    throws AuthorizationServiceException {

    String subjectDN = dynrecord.subjectDN;
    String role = dynrecord.subjectDN;
    Map<String, String> strvals;
    String method=null;

    try {
      strvals = dynrecord.getStringValues();

      for( Map.Entry<String, String> strval : strvals.entrySet()) {
        String key = strval.getKey();
        method = strval.getValue();
        String result, input = getRegExInput(method, dynrecord);
        if(input==null)
          result = getDynamicString(dynamic_mapper, method, subjectDN, role);
        else
          result = getDynamicString(dynamic_mapper, "regular_expression", method, input);
        strvals.put(key, result);
      }

      return new DynamicAuthorizationRecord(dynrecord);

    } catch (AuthorizationServiceException ase) {
      throw ase;
    } catch (NumberFormatException nfe) {
      throw new AuthorizationServiceException("NumberFormatException from uid mapping method " + method + " for DN " + subjectDN + " and role " + role);
    }

  }


  public static String getRegExInput(String method, DynamicAuthorizationRecord dynrecord) {

    if(method.startsWith("$")) {
      StringTokenizer t = new StringTokenizer(method, "/");
      int ntokens = t.countTokens();
      if ( ntokens < 3) return null;

      String varname = t.nextToken().toLowerCase();
      return dynrecord.getStringValues().get(varname);
    }

    return null;
  }

} //end of AuthorizationServicePlugin