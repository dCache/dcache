package gplazma.authz;

import java.util.*;
import java.net.Socket;
import java.security.cert.X509Certificate;

import org.ietf.jgss.GSSContext;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.GSSConstants;
import org.gridforum.jgss.ExtendedGSSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(AuthorizationController.class);
    private AuthorizationConfig authConfig=null;
    private AuthorizationPluginLoader plugin_loader;

    public static final String GPLAZMA_SRMDCACHE_RELEASE_VERSION="0.1-1";
    private static String service_cert          = "/etc/grid-security/hostcert.pem";
    private static String service_key           = "/etc/grid-security/hostkey.pem";
    private static String trusted_cacerts = "/etc/grid-security/certificates";

    private long authRequestID;
    private static final Random random = new Random();
    /** Whether to use SAZ callout if gsscontext is available. **/
    private boolean use_saz=false;
    private boolean omitEmail=false;
    public static final String capnull = "/Capability=NULL";
    public static final int capnulllen = capnull.length();
    public static final String rolenull ="/Role=NULL";
    public static final int rolenulllen = rolenull.length();

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

    public AuthorizationController(String authConfigFilePath, long authRequestID)
    throws AuthorizationException {
        this.authRequestID=authRequestID;
        try {
            authConfig = new AuthorizationConfig(authConfigFilePath, authRequestID);
        } catch(java.io.IOException ioe) {
            log.error("Exception in AuthorizationConfig instantiation :" + ioe);
            throw new AuthorizationException(ioe.toString());
        }
        plugin_loader = new AuthorizationPluginLoader(authConfig, authRequestID);
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
                context= HostUtil.getServiceContext(service_cert, service_key, trusted_cacerts);
                sazsocket = X509CertUtil.getGsiClientSocket(authConfig.getSazServerHost(),
                        Integer.parseInt(authConfig.getSazServerPort()), (ExtendedGSSContext) context);
                ((GssSocket)sazsocket).setUseClientMode(true);
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

    public Map<NameRolePair, gPlazmaAuthorizationRecord> authorize(String subjectDN, Collection<String> roles, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        Map<NameRolePair, gPlazmaAuthorizationRecord> records = new LinkedHashMap<NameRolePair, gPlazmaAuthorizationRecord>();
        Iterator<AuthorizationPlugin> plugins = plugin_loader.getPlugins();
        AuthorizationException authexceptions = null;

        while (records.isEmpty() && plugins.hasNext()) {

            AuthorizationPlugin plugin = plugins.next();

            for (String role : roles) {

                try {
                    gPlazmaAuthorizationRecord authorizationRecord = plugin.authorize(subjectDN, role, chain, desiredUserName, serviceUrl, socket);
                    records.put(new NameRolePair(subjectDN, role), authorizationRecord);

                } catch (AuthorizationException ex) {
                    if (authexceptions == null) {
                        authexceptions = ex;
                    } else {
                        authexceptions = new AuthorizationException(authexceptions.getMessage() + "\n" + ex.getMessage());
                    }
                }

                if (desiredUserName != null) {
                    break;
                }
            }
        }
        // Check whether authorization was denied.
        if (records.isEmpty()) {

            // Indicate denial
            if (authexceptions == null) {
                String denied = AuthorizationPlugin.DENIED_MESSAGE + " for " + subjectDN + " and roles " + roles.toString();
                authexceptions = new AuthorizationException(denied);
            }
            throw authexceptions;
        }
        return records;
    }


    public static String getFormattedAuthRequestID(long id) {
        String idstr;
        idstr = String.valueOf(id);
        while (idstr.length()<10) idstr = " " + idstr;
        return " " + idstr;
    }

    public void setUseSAZ(boolean boolarg) {
        use_saz = boolarg;
    }

    public boolean getUseSAZ() {
        return use_saz;
    }

}
