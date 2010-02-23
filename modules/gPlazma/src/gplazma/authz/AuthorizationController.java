// $Id: AuthorizationController.java,v 1.38 2007-10-19 20:50:02 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.37  2007/08/03 20:20:01  timur
// implementing some of the findbug bugs and recommendations, avoid selfassignment, possible nullpointer exceptions, syncronization issues, etc
//
// Revision 1.36  2007/08/03 15:46:02  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes etc, per findbug recommendations
//
// Revision 1.35  2007/08/02 16:00:28  timur
// reuse instance of Random, per findbugs recommendation
//
// Revision 1.34  2007/06/08 18:23:06  tdh
// Provisional fix for DN extraction from certificate chain.
//
// Revision 1.33  2007/05/31 19:28:59  tdh
// Use getIssuer instead of getSubject for DN.
//
// Revision 1.32  2007/05/31 18:13:09  tdh
// Remove excision of CN=\d\d\d\d\d\d from SubjectDN.
//
// Revision 1.31  2007/05/01 19:19:50  tdh
// Clip "CN=limited proxy" string from end of DN.
//
// Revision 1.30  2007/04/17 21:48:31  tdh
// Added print of line number when catching generic throwable from plugin.
//
// Revision 1.29  2007/03/27 19:20:28  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.28  2007/03/16 22:36:16  tdh
// Propagate requested username.
//
// Revision 1.27  2007/03/16 21:59:49  tdh
// Added ability to request username.
// Config files are read only if they have changed.
//
// Revision 1.26  2007/02/08 20:27:45  tdh
// Added code for dynamic mapping of uid, gid, etc.
//
// Revision 1.25  2007/01/31 17:56:45  tdh
// Added field and methods for vomsValidation to AuthorizationConfig.
// Added option to validate attributes in AuthorizationController.
// Made static BasicTrustStore instantiation in AuthorizationController use /etc/grid-security/vomsdir, or, if empty, certificates directory.
//
// Revision 1.24  2007/01/12 19:28:45  tdh
// Will throw exception REVOCATION_MESSAGE is caught.
// Fixed DN field ordering.
// Fixed authorization being done twice since SAZ was added.
//
// Revision 1.23  2007/01/04 17:45:03  tdh
// Forwarding log level to SAZ plugin.
// Using full chain to authorize.
// Fixed order of DN fields.
//
// Revision 1.22  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationController.
//
// Revision 1.21  2006/12/15 15:56:30  tdh
// Added support for SAZ authorization.
//
// Revision 1.20  2006/12/07 20:28:49  tdh
// Added static method to extract FQAN from context. Authorization using context calls
// this method, then queries gPlazma using group and role; SRM and gridftp no longer
// delegate credentials to gPlamza. Static method replaced code in plugins.
//
// Revision 1.19  2006/11/29 19:11:35  tdh
// Added ability to set log level and propagate to plugins.
//
// Revision 1.18  2006/11/28 21:14:55  tdh
// Check that caller is not null.
//
// Revision 1.17  2006/11/28 21:13:20  tdh
// Set log4j log level in plugins according to printout level of calling cell.
//
// Revision 1.16  2006/11/13 16:33:41  tigran
// fixed CLOSE_WAIT:
// delegated socked was no closed
//
// Revision 1.15  2006/09/07 20:12:52  tdh
// Propagate fix of Revision 1.11.4.4 to development branch.
//
// Revision 1.14  2006/08/23 16:40:05  tdh
// Propagate authorization request ID to plugins so log entries can be tagged with it.
//
// Revision 1.13  2006/08/07 16:38:03  tdh
// Merger of changes from branch, exception handling and ignore blank config file lines.
//
// Revision 1.11.4.3  2006/08/07 16:23:41  tdh
// Added exception handling lines to other authorization methods.
//
// Revision 1.11.4.2  2006/08/07 15:55:23  tdh
// Checks that authorization is null before throwing AuthorizationException.
//
// Revision 1.11.4.1  2006/07/26 18:41:59  tdh
// Backport of recent changes to development branch.
//
// Revision 1.12  2006/07/25 14:58:45  tdh
// Merged DN/Role authentication. Added logging and authRequestID code.
//
// Revision 1.11.2.2  2006/07/12 19:45:57  tdh
// Uncommented VORoleMapAuthzPlugin and added CVS line.
//

package gplazma.authz;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.security.cert.X509Certificate;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;

//import org.apache.log4j.Logger;
//import org.apache.log4j.Level;

import org.ietf.jgss.GSSContext;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.GSSConstants;
import org.gridforum.jgss.ExtendedGSSContext;
import org.apache.log4j.*;
import gplazma.authz.plugins.AuthorizationPlugin;
import gplazma.authz.plugins.saz.SAZAuthorizationPlugin;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.util.HostUtil;
import gplazma.authz.util.NameRolePair;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public class AuthorizationController {

    private static final Logger log = Logger.getLogger(AuthorizationController.class);
    private AuthorizationConfig authConfig=null;
    private AuthorizationPluginLoader plugin_loader;

    public static final String GPLAZMA_SRMDCACHE_RELEASE_VERSION="0.1-1";
    private static String service_cert          = "/etc/grid-security/hostcert.pem";
    private static String service_key           = "/etc/grid-security/hostkey.pem";
    private static String trusted_cacerts = "/etc/grid-security/certificates";

    private long authRequestID;
    private String authRequestID_str;
    private static final Random random = new Random();
    //private String loglevel=null;
    //public static final Pattern pattern1 = Pattern.compile("/CN=proxy");
    //public static final Pattern pattern2 = Pattern.compile("/CN=\\d{6,}");
    //public static final Pattern pattern3 = Pattern.compile("/CN=limited proxy");
    /** Whether to use SAZ callout if gsscontext is available. **/
    private boolean use_saz=false;
    private boolean omitEmail=false;
   // private DateFormat _df   = new SimpleDateFormat("MM/dd HH:mm:ss" );
    public static final String capnull = "/Capability=NULL";
    public static final int capnulllen = capnull.length();
    public static final String rolenull ="/Role=NULL";
    public static final int rolenulllen = rolenull.length();

    static {
        Logger.getLogger(org.glite.security.trustmanager.CRLFileTrustManager.class.getName()).setLevel(Level.ERROR);
        Logger.getLogger("org.glite.security.trustmanager.ContextWrapper").setLevel(Level.OFF);
        Logger.getLogger("org.glite.security.trustmanager.axis.AXISSocketFactory").setLevel(Level.OFF);
        Logger.getLogger("org.glite.security.util.DirectoryList").setLevel(Level.OFF);
    }


    public AuthorizationController()
    throws AuthorizationException {
        this(null, random.nextInt(Integer.MAX_VALUE));
    }

    public AuthorizationController(String authservConfigFilePath)
    throws AuthorizationException {
        this(authservConfigFilePath, random.nextInt(Integer.MAX_VALUE));
    }

    public AuthorizationController(long authRequestID)
    throws AuthorizationException {
        this(null, authRequestID);
    }

    //public AuthorizationController(CellAdapter caller)
    //throws AuthorizationException {
    //    this(null, random.nextInt(Integer.MAX_VALUE), caller);
    //}

    public AuthorizationController(String authConfigFilePath, long authRequestID)
    throws AuthorizationException {
        this.authRequestID=authRequestID;
        authRequestID_str = getFormattedAuthRequestID(authRequestID);
        try {
            authConfig = new AuthorizationConfig(authConfigFilePath, authRequestID);
        } catch(java.io.IOException ioe) {
            log.error("Exception in AuthorizationConfig instantiation :" + ioe);
            throw new AuthorizationException(ioe.toString());
        }
        plugin_loader = new AuthorizationPluginLoader(authConfig, authRequestID);
        //log.trace("AuthorizationController instantiated.");
    }

    public long getAuthRequestID() {
        return authRequestID;
    }

    public AuthorizationPluginLoader getPluginLoader() {
        return plugin_loader;
    }

    public Map <NameRolePair, gPlazmaAuthorizationRecord> authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationException {
        AuthorizationException authexceptions=null;

        X509Certificate[] chain;
        ExtendedGSSContext extendedcontext;
        try {
            if (context instanceof ExtendedGSSContext) {
                extendedcontext = (ExtendedGSSContext) context;
            } else {
                log.error("Received context not instance of ExtendedGSSContext, AuthorizationController exiting ...");
                authexceptions = new AuthorizationException("\nException thrown by " + this.getClass().getName() +
                        ": Received context not instance of ExtendedGSSContext");
                throw authexceptions;
            }

            chain=(X509Certificate[])extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch(Exception e) {
            log.error("AuthorizationController - Exception: " + e);
            authexceptions = new AuthorizationException("\nException thrown by " + this.getClass().getName() + ": " + e.getMessage());
            throw authexceptions;
        }

        return authorize(chain, desiredUserName, serviceUrl, socket);
    }

    public Map <NameRolePair, gPlazmaAuthorizationRecord> authorize(X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationException {
        AuthorizationException authexceptions=null;

        try {
            setUseSAZ(authConfig.getSazClientOn());
        } catch(Exception e) {
            authexceptions = new AuthorizationException("\nException thrown by " + authConfig.getClass().getName() + ": " + e.getMessage());
            throw authexceptions;
        }

        if (getUseSAZ()) {
            SAZAuthorizationPlugin sazclient;
            GSSContext context=null;
            Socket sazsocket=null;
            try {
                sazclient = new SAZAuthorizationPlugin(authRequestID);
                //plugin_loader.forwardLogLevel(sazclient);
                context= HostUtil.getServiceContext(service_cert, service_key, trusted_cacerts);
                //context=getUserContext("/tmp/x509up_u500");
                sazsocket = X509CertUtil.getGsiClientSocket(authConfig.getSazServerHost(),
                        Integer.parseInt(authConfig.getSazServerPort()), (ExtendedGSSContext) context);
                ((GssSocket)sazsocket).setUseClientMode(true);
                //UserAuthRecord sazauth = sazclient.authorize(new X509Certificate[]{chain[i]}, desiredUserName, serviceUrl, sazsocket);
                gPlazmaAuthorizationRecord sazauth = sazclient.authorize(chain, desiredUserName, serviceUrl, sazsocket);
                if(sazauth==null) {
                    authexceptions = new AuthorizationException("Authorization denied by SAZ");
                    throw authexceptions;
                }
            } catch(Exception e) {
                if(e instanceof AuthorizationException)
                    authexceptions = new AuthorizationException(" from SAZAuthorizationPlugin : " + e.getMessage());
                else
                    authexceptions = new AuthorizationException(" from SAZAuthorizationPlugin : " + e);
                try {
                    if(context!=null) context.dispose();
                    if(sazsocket!=null) sazsocket.close();
                } catch (Exception de) {
                    String error = authexceptions==null?"":authexceptions.getMessage() +
                            ": Exception thrown by SAZAuthorizationPlugin: " + de.getMessage();
                    authexceptions = new AuthorizationException(error);
                }
                throw authexceptions;
            }
            try {
                if(context!=null) context.dispose();
                if(sazsocket!=null) sazsocket.close();
            } catch (Exception de) {
                String error = authexceptions==null?"":authexceptions.getMessage() +
                        ": Exception thrown by SAZAuthorizationPlugin: " + de.getMessage();
                authexceptions = new AuthorizationException(error);
            }
        }

        String subjectDN;
        Collection <String> roles;

        try {
            subjectDN = X509CertUtil.getSubjectFromX509Chain(chain, omitEmail);
            if(omitEmail) log.warn("Removed email field from DN: " + subjectDN);
        } catch(Exception e) {
            String msg = e.getMessage();
            if(msg==null)
                throw new AuthorizationException("Could not extract subject DN from certificate chain");
            else throw new AuthorizationException("Could not extract subject DN from certificate chain: "  + msg);
        }

        try {
            roles = X509CertUtil.getFQANsFromX509Chain(chain, authConfig.getVOMSValidation());
        } catch(Exception e) {            String msg = e.getMessage();
            if(msg==null)
                throw new AuthorizationException("for subject DN " + subjectDN);
            else throw new AuthorizationException("for subject DN " + subjectDN + " " + msg);
        }

        return authorize(subjectDN, roles, chain, desiredUserName, serviceUrl, socket);

    }

    public Map <NameRolePair, gPlazmaAuthorizationRecord>
        authorize(String subjectDN, Collection <String> roles, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {
        Map <NameRolePair, gPlazmaAuthorizationRecord> records = new LinkedHashMap <NameRolePair, gPlazmaAuthorizationRecord>();

        AuthorizationException authexceptions=null;

        Iterator <String> roleIter = roles.iterator();
        if(!roleIter.hasNext()) roleIter = new NullIterator<String>();
        while (roleIter.hasNext()) {
            String role = roleIter.next();
            gPlazmaAuthorizationRecord r=null;
            try {
                r = authorize(subjectDN, role, chain, desiredUserName, serviceUrl, socket);
            } catch (AuthorizationException ase) {
                if(authexceptions==null)
                    authexceptions = ase;
                else
                    authexceptions = new AuthorizationException(authexceptions.getMessage() + "\n" + ase.getMessage());
            }
            records.put(new NameRolePair(subjectDN, role), r);
            if(desiredUserName!=null) break;
        }

        // Check whether authorization was denied.
         for( NameRolePair nameAndRole : records.keySet()) {
          gPlazmaAuthorizationRecord r = records.get(nameAndRole);
          if(r!=null) {
              return records;
          }
         }

        // All authrecords are null. Perhaps no plugins were tried.
        if(!plugin_loader.getPlugins().hasNext()) {
            return records;
        }

        // Indicate denial
        if(authexceptions==null) {
            String denied = AuthorizationPlugin.DENIED_MESSAGE + " for " + subjectDN + " and roles " + roles.toString();
            authexceptions = new AuthorizationException(denied);
        }
        throw authexceptions;
    }


    public gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationException {
        gPlazmaAuthorizationRecord r = null;
        AuthorizationException authexceptions=null;

        AuthorizationPlugin p;
        Iterator plugins = plugin_loader.getPlugins();
        while (r==null && plugins.hasNext()) {
            p = (AuthorizationPlugin) plugins.next();
            try {
                r = p.authorize(subjectDN, role, chain, desiredUserName, serviceUrl, socket);
            } catch(AuthorizationException ae) {
                if(authexceptions==null)
                    authexceptions = new AuthorizationException("\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
                else
                    authexceptions = new AuthorizationException(authexceptions.getMessage() + "\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
                //if(ae.getMessage().equals(AuthorizationPlugin.REVOCATION_MESSAGE)) throw authexceptions;
            }
        }

        if(authexceptions!=null && r==null) throw authexceptions;

        return r;
    }


    public static class NullIterator<String> implements Iterator {
        private boolean hasnext = true;
        public boolean hasNext() { return hasnext; }
        public String next()
        throws java.util.NoSuchElementException {
            if(hasnext) {
                hasnext = false;
                return null;
            }
            throw new java.util.NoSuchElementException("no more nulls");
        }

        public void remove() {}
    }

    public static String getFormattedAuthRequestID(long id) {
        String idstr;
        idstr = String.valueOf(id);
        while (idstr.length()<10) idstr = " " + idstr;
        return " " + idstr;
    }

    public void setLogLevel	(Level level) {
        log.setLevel(level);
        authConfig.setLogLevel(log.getLevel());
        plugin_loader.setLogLevel(log.getLevel());
    }

    public void setUseSAZ(boolean boolarg) {
        use_saz = boolarg;
    }

    public boolean getUseSAZ() {
        return use_saz;
    }

} //end of class AuthorizationController
