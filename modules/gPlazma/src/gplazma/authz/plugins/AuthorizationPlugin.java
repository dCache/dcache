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

    public AuthorizationPlugin(long authRequestID) {
        this.authRequestID = authRequestID;
    }

    public long getAuthRequestID() {
        return authRequestID;
    }


    public abstract gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException;


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
            Method meth = (Method) DCacheSRMauthzRecordsService.getDynamicMethods().get(id_method);
            if(meth==null) {
                return null;
            } else {
                ret_val = meth.invoke(this, subjectDN, role);
            }
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