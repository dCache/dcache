// $Id: AuthorizationPlugin.java,v 1.28 2007-10-23 17:11:24 tdh Exp $
// $Log: not supported by cvs2svn $

package gplazma.authz.plugins;

import java.util.*;
import java.lang.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.security.cert.X509Certificate;

import gplazma.authz.records.DynamicAuthorizationRecord;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.records.DCacheSRMauthzRecordsService;
import gplazma.authz.AuthorizationException;


/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public abstract class AuthorizationPlugin {

    long authRequestID;
    public static final String DENIED_MESSAGE="Permission Denied";
    public static final String REVOCATION_MESSAGE=DENIED_MESSAGE+" - revocation.";
    //public static final Class STR_CLASS = "z".getClass();
    //public static final Class INT_CLASS = Integer.TYPE;

    public AuthorizationPlugin(long authRequestID) {
        this.authRequestID = authRequestID;
    }

    public long getAuthRequestID() {
        return authRequestID;
    }

    /*public gPlazmaAuthorizationRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {
        return null;
    }*/

    public abstract gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException;


    //public void setLogLevel	(String level) {
    //}

    /*public gPlazmaAuthorizationRecord getAuthRecord(String username, String subjectDN, String role) throws AuthorizationException {

       DCacheSRMauthzRecordsService storageRecordsServ;

       try {
         storageRecordsServ = new DCacheSRMauthzRecordsService(storageAuthzPath);
       } catch(Exception ase) {
         esay("Exception in reading storage-authzdb configuration file: ");
         esay(storageAuthzPath + " " + ase);
         throw new AuthorizationException(ase.toString());
       }

       gPlazmaAuthorizationRecord authRecord = storageRecordsServ.getStorageUserRecord(username);

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
         throw new AuthorizationException(denied);
       }

       //Integer uid = localId.getUID(); if(uid==null) {
       int uid = authRecord.UID; if(uid==-1) {
         String denied = DENIED_MESSAGE + ": uid not found for " + user;
         warn(denied);
         throw new AuthorizationException(denied);
       }

       //Integer gid = localId.getGID(); if(gid==null) {
       int[] gids = authRecord.GIDs; if(gids[0]==-1) {
         String denied = DENIED_MESSAGE + ": gids not found for " + user;
         warn(denied);
         throw new AuthorizationException(denied);
       }

           //String home = localId.getRelativeHomePath(); if(home==null) {
       String home = authRecord.Home; if(home==null) {
         String denied = DENIED_MESSAGE + ": relative home path not found for " + user;
         warn(denied);
         throw new AuthorizationException(denied);
       }

           //String root = localId.getRootPath(); if(root==null) {
       String root = authRecord.Root; if(root==null) {
         String denied = DENIED_MESSAGE + ": root path not found for " + user;
         warn(denied);
         throw new AuthorizationException(denied);
       }

       String fsroot = authRecord.FsRoot; //if(root==null) {
       int priority = authRecord.priority;

       boolean readonlyflag = authRecord.ReadOnly;

       debug("Plugin now forming user authorization records...");
       HashSet principals = new HashSet();

       //UserAuthRecord authRecordtoReturn = new UserAuthRecord(user, subjectDN, role, readonlyflag, priority, uid, gids, home, root, fsroot, principals);
       //if (authRecordtoReturn.isValid()) {
       //  debug("User authorization record has been formed and is valid.");
       //}

      // return authRecordtoReturn;
         return authRecord;
     }
    */


    public String getDynamicString(String dynamic_mapper, String id_method, String subjectDN, String role) throws AuthorizationException {
        try {
            Object retobj = getDynamicValue(dynamic_mapper, id_method, subjectDN, role);
            return  (retobj==null) ? null : retobj.toString();
        } catch (Exception e) {
            throw new AuthorizationException("Method " + id_method + " failed for DynamicMapper " + dynamic_mapper + " for DN " + subjectDN + " and role " + role);
        }
    }

    protected Integer getDynamicInteger(String dynamic_mapper, String id_method, String subjectDN, String role) throws AuthorizationException {
        String value=null;
        try {
            value = getDynamicString(dynamic_mapper, id_method, subjectDN, role);
            return Integer.decode(value);
        } catch (NumberFormatException nfe) {
            throw new AuthorizationException("Method " + id_method + " return value " + value + " not parsable to Integer for DynamicMapper " + dynamic_mapper + " for DN " + subjectDN + " and role " + role);
        }
    }

    private Object getDynamicValue(String dynamic_mapper, String id_method, String subjectDN, String role) throws AuthorizationException {

        Object ret_val;

        try {
            //Class DynamicMapper = Class.forName(dynamic_mapper);
            //Method meth = DynamicMapper.getMethod(id_method, STR_CLASS, STR_CLASS);
            Method meth = (Method) DCacheSRMauthzRecordsService.getDynamicMethods().get(id_method);
            if(meth==null) {
                //throw new AuthorizationException("No method " + id_method + " found in " + DCacheSRMauthzRecordsService.getDynamicMapper());
                return null;
            } else {
                ret_val = meth.invoke(this, subjectDN, role);
            }
            //} catch (ClassNotFoundException cnfe) {
            //throw new AuthorizationException("ClassNotFoundException for DynamicMapper " + dynamic_mapper + " for DN " + subjectDN + " and role " + role);
            //} catch (NoSuchMethodException nsm) {
            //throw new AuthorizationException("NoSuchMethodException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        } catch (InvocationTargetException ite) {
            throw new AuthorizationException("InvocationTargetException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        } catch (IllegalAccessException iac) {
            throw new AuthorizationException("IllegalAccessException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        } catch (NumberFormatException nfe) {
            throw new AuthorizationException("NumberFormatException from uid mapping method " + id_method + " for DN " + subjectDN + " and role " + role);
        }

        return ret_val;
    }

    public <T> T getDynamicValue(String dynamic_mapper, String uid_method, String subjectDN, String role, Class<T> ct) throws AuthorizationException {
        //return (T) getDynamicValue(dynamic_mapper, uid_method, subjectDN, role);
        return null;
    }

    public gPlazmaAuthorizationRecord getDynamicRecord(String dynamic_mapper, DynamicAuthorizationRecord dynrecord)
            throws AuthorizationException {

        String subjectDN = dynrecord.subjectDN;
        String role = dynrecord.role;
        Map<String, String> strvals;
        String method=null;

        try {
            DynamicAuthorizationRecord retrecord = new DynamicAuthorizationRecord(dynrecord);
            strvals = retrecord.getStringValues();

            for( Map.Entry<String, String> strval : strvals.entrySet()) {
                String key = strval.getKey();
                method = strval.getValue();
                String result, input = getRegExInput(method, retrecord);
                if(input==null)
                    result = getDynamicString(dynamic_mapper, method, subjectDN, role);
                else
                    result = getDynamicString(dynamic_mapper, "regular_expression", method, input);
                if (result!=null) strvals.put(key, result);
            }

            return new DynamicAuthorizationRecord(retrecord);

        } catch (AuthorizationException ase) {
            throw ase;
        } catch (NumberFormatException nfe) {
            throw new AuthorizationException("NumberFormatException from uid mapping method " + method + " for DN " + subjectDN + " and role " + role);
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

} //end of AuthorizationPlugin