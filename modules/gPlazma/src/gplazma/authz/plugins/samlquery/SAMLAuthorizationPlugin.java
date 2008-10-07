// $Id: SAML1AuthorizationPlugin.java,v 1.28 2007-10-23 17:11:24 tdh Exp $
// $Log: not supported by cvs2svn $

/*
 * SAML1AuthorizationPlugin.java
 *
 * Created on January 29, 2005
 */

package gplazma.authz.plugins.samlquery;

import java.util.*;
import java.lang.*;
import java.net.Socket;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.gridforum.jgss.ExtendedGSSContext;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import gplazma.authz.AuthorizationException;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.plugins.RecordMappingPlugin;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public abstract class SAMLAuthorizationPlugin extends RecordMappingPlugin {

    private String mappingServiceURL;
    private String targetServiceName;
    private static final String service_key  = "/etc/grid-security/hostkey.pem";
    private static final String service_cert = "/etc/grid-security/hostcert.pem";
    GSSContext context;
    String desiredUserName;
    String serviceUrl;
    Socket socket;

  public SAMLAuthorizationPlugin(String mappingServiceURL, String storageAuthzPath, long authRequestID) {
        super(storageAuthzPath, authRequestID);
        this.mappingServiceURL = mappingServiceURL;
    }

  public String getMappingServiceURL() {
      return mappingServiceURL;
  }

    public String getTargetServiceName() throws GSSException {

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

    public gPlazmaAuthorizationRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        this.context = context;
        this.desiredUserName = desiredUserName;

        getLogger().debug("Extracting Subject DN and Role from GSSContext");

        String gssIdentity;
        String fqanValue;
        ExtendedGSSContext extendedcontext;
        if (context instanceof ExtendedGSSContext) {
            extendedcontext = (ExtendedGSSContext) context;
        }
        else {
            getLogger().error("Received context not instance of ExtendedGSSContext, Plugin exiting ...");
            return null;
        }

        try {
            gssIdentity = context.getSrcName().toString();
        } catch (GSSException gsse) {
            getLogger().error("Caught GSSException in getting DN " + gsse);
            return null;
        }

        try {
            Iterator<String> fqans = X509CertUtil.getFQANsFromContext(extendedcontext).iterator();
            fqanValue = fqans.hasNext() ? fqans.next() : "";
        } catch (Exception gsse) {
            getLogger().error("Caught Exception in extracting group and role " + gsse);
            return null;
        }


        return authorize(gssIdentity, fqanValue, desiredUserName, serviceUrl, socket);
    }

    public abstract gPlazmaAuthorizationRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket) throws AuthorizationException;

    /*
    private gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecord(LocalId localId, String subjectDN, String role) throws AuthorizationException {

    String username = localId.getUserName();

    if(username==null) {
      String denied = DENIED_MESSAGE + ": non-null user record received, but with a null username";
      warn(denied);
      throw new AuthorizationException(denied);
    } else {
      say("VO mapping service returned Username: " + username);
    }

    return getAuthRecord(username, subjectDN, role);
  }
    */
/*
    private gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecordx(LocalId localId, String subjectDN, String role) throws AuthorizationException {

        gPlazmaAuthorizationRecord authRecord = null;
        String user;

        user=localId.getUserName();
        if(user==null) {
            String denied = DENIED_MESSAGE + ": non-null user record received, but with a null username";
            warn(denied);
            throw new AuthorizationException(denied);
        } else {
            say("VO mapping service returned Username: " + user);
        }

        authRecord = storageRecordsServ.getStorageUserRecord(user_name);

        Integer uid = localId.getUID();
        if(uid==null) {
            String denied = DENIED_MESSAGE + ": uid not found for " + user;
            warn(denied);
            throw new AuthorizationException(denied);
        }

        Integer gid = localId.getGID();
        if(gid==null) {
            String denied = DENIED_MESSAGE + ": gid not found for " + user;
            warn(denied);
            throw new AuthorizationException(denied);
        }

        String home = localId.getRelativeHomePath();
        if(home==null) {
            String denied = DENIED_MESSAGE + ": relative home path not found for " + user;
            warn(denied);
            throw new AuthorizationException(denied);
        }

        String root = localId.getRootPath();
        if(root==null) {
            String denied = DENIED_MESSAGE + ": root path not found for " + user;
            warn(denied);
            throw new AuthorizationException(denied);
        }

        String fsroot = localId.getFSRootPath();
        if(fsroot==null) {
            String denied = DENIED_MESSAGE + ": fsroot path not found for " + user;
            warn(denied);
            throw new AuthorizationException(denied);
        }

        //int priority = VORecord.priority;
        int priority=0;
        String priority_str = localId.getPriority();
        if(priority_str==null) {
            String denied = DENIED_MESSAGE + ": priority not found for " + user;
            warn(denied);
            throw new AuthorizationException(denied);
        } else {
            try {
                priority = Integer.valueOf(priority_str).intValue();
            } catch (Exception e) {
                if(!priority_str.equals("default")) {
                    String denied = DENIED_MESSAGE + ": priority for user " + user + " could not be parsed to an integer";
                    warn(denied);
                    throw new AuthorizationException(denied);
                }
            }
        }

        boolean readonlyflag = localId.getReadOnlyFlag();
        //todo Following to be used later, currently String type "default" is returned from VO mapping
        //int priority = Integer.parseInt(localId.getPriority());

        debug("Plugin now forming user authorization records...");
        HashSet<String> principals = new HashSet<String>();

        gauthrec = new gPlazmaAuthorizationRecord(user,
                                      readonlyflag,
                                      priority,
                                      uid.intValue(), new int[]{gid.intValue()}, home, root, fsroot,
                                      subjectDN,
                                      role,
                                      getAuthRequestID());
        return gauthrec;
    }

*/
} //end of class SAML1AuthorizationPlugin
