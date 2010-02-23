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
import java.security.cert.X509Certificate;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.gridforum.jgss.ExtendedGSSContext;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import gplazma.authz.AuthorizationException;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.plugins.RecordMappingPlugin;
import org.apache.log4j.Logger;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public abstract class SAMLAuthorizationPlugin extends RecordMappingPlugin {

    private static final Logger logger = Logger.getLogger(SAMLAuthorizationPlugin.class);
    private String mappingServiceURL;
    private GlobusCredential serviceCredential;
    private String targetServiceName;
    private String targetServiceIssuer;
    private static final String service_key  = "/etc/grid-security/hostkey.pem";
    private static final String service_cert = "/etc/grid-security/hostcert.pem";
    private static final String service_CAFiles="/etc/grid-security/certificates/*.0";
    private static final String service_keyPasswd=null;
    GSSContext context;
    String desiredUserName;
    String serviceUrl;
    Socket socket;

  public SAMLAuthorizationPlugin(String mappingServiceURL, String storageAuthzPath, long authRequestID) {
        super(storageAuthzPath, authRequestID);
        this.mappingServiceURL = mappingServiceURL;
        setSslProperties();
    }

  public String getMappingServiceURL() {
      return mappingServiceURL;
  }

    public String getTargetServiceName() throws GSSException {

        if(targetServiceName==null) {
            if(serviceCredential==null) {
                try {
                    serviceCredential =new GlobusCredential(
                            service_cert,
                            service_key
                    );
                } catch(GlobusCredentialException gce) {
                    throw new GSSException(GSSException.NO_CRED , 0,
                            "could not load host globus credentials " + gce.toString());
                }
            }

            targetServiceName = serviceCredential.getIdentity();
        }

        return targetServiceName;
    }

    public String getTargetServiceIssuer() throws GSSException {

        if(targetServiceIssuer==null) {
            if(serviceCredential==null) {
                try {
                    serviceCredential =new GlobusCredential(
                            service_cert,
                            service_key
                    );
                } catch(GlobusCredentialException gce) {
                    throw new GSSException(GSSException.NO_CRED , 0,
                            "could not load host globus credentials " + gce.toString());
                }
            }

            targetServiceIssuer = X509CertUtil.toGlobusDN(serviceCredential.getIssuer());
        }

        return targetServiceIssuer;
    }

    public gPlazmaAuthorizationRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        this.context = context;
        this.desiredUserName = desiredUserName;

        logger.debug("Extracting Subject DN and Role from GSSContext");

        String gssIdentity;
        String fqanValue;
        ExtendedGSSContext extendedcontext;
        if (context instanceof ExtendedGSSContext) {
            extendedcontext = (ExtendedGSSContext) context;
        }
        else {
            logger.error("Received context not instance of ExtendedGSSContext, Plugin exiting ...");
            return null;
        }

        try {
            gssIdentity = context.getSrcName().toString();
        } catch (GSSException gsse) {
            logger.error("Caught GSSException in getting DN " + gsse);
            return null;
        }

        try {
            Iterator<String> fqans = X509CertUtil.getFQANsFromContext(extendedcontext).iterator();
            fqanValue = fqans.hasNext() ? fqans.next() : "";
        } catch (Exception gsse) {
            logger.error("Caught Exception in extracting group and role " + gsse);
            return null;
        }


        return authorize(gssIdentity, fqanValue, null, desiredUserName, serviceUrl, socket);
    }

    public abstract gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket) throws AuthorizationException;

     private void setSslProperties() {

        if (service_CAFiles != null) {
            System.setProperty("sslCAFiles", service_CAFiles);
        }
        if (service_cert!= null) {
            System.setProperty("sslCertfile", service_cert);
        }
        if (service_key != null) {
            System.setProperty("sslKey", service_key);
        }
        if (service_keyPasswd != null) {
             System.setProperty("sslKeyPasswd", service_keyPasswd);
        }
    }

} //end of class SAMLAuthorizationPlugin
