/*
 * AuthorizationServicePlugin.java
 *
 * Created on January 29, 2005
 */

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
import gplazma.gplazmalite.storageauthzdbService.DynamicAuthorizationRecord;
import gplazma.gplazmalite.storageauthzdbService.StorageAuthorizationRecord;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public abstract class AuthorizationServicePlugin {

  public static final String DENIED_MESSAGE="Permission Denied";
  public static final String REVOCATION_MESSAGE=DENIED_MESSAGE+" - revocation.";
  public static final Class STR_CLASS = "z".getClass();
  public static final Class INT_CLASS = Integer.TYPE;

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


  public String getRegExInput(String method, DynamicAuthorizationRecord dynrecord) {

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
