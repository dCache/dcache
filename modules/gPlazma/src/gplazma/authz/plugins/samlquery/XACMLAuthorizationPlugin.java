// $Id: XACMLAuthorizationPlugin.java,v 1.28 2007-10-23 17:11:24 tdh Exp $
// $Log: not supported by cvs2svn $

package gplazma.authz.plugins.samlquery;

import java.util.*;
import java.lang.*;
import java.net.Socket;

import gplazma.authz.AuthorizationException;
import gplazma.authz.util.HostUtil;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import org.opensciencegrid.authz.xacml.common.LocalId;
import org.opensciencegrid.authz.xacml.common.XACMLConstants;
import org.opensciencegrid.authz.xacml.client.MapCredentialsClient;
import org.apache.log4j.*;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public class XACMLAuthorizationPlugin extends SAMLAuthorizationPlugin {

    private static HashMap<String, TimedLocalId> UsernameMap = new HashMap();


    public XACMLAuthorizationPlugin(String mappingServiceURL, String storageAuthzPath, long authRequestID) {
        super(mappingServiceURL, storageAuthzPath, authRequestID);
        if(getLogger().getAppender("XACMLAuthorizationPlugin")==null) {
            Enumeration appenders = getLogger().getParent().getAllAppenders();
            while(appenders.hasMoreElements()) {
                Appender apnd = (Appender) appenders.nextElement();
                if(apnd instanceof ConsoleAppender)
                    apnd.setLayout(getLogLayout());
            }
        }
        getLogger().debug("XACMLAuthorizationPlugin: authRequestID " + authRequestID + " Plugin now loaded: saml-vo-mapping");
    }

    public gPlazmaAuthorizationRecord authorize(String X509Subject, String role, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        MapCredentialsClient xacmlClient;
        LocalId localId;
        String resourceDNSName;
        String resourceX509Name;
        String resourceX509Issuer;
        String key = X509Subject;

        try {
            resourceX509Name = getTargetServiceName();
        }
        catch (Exception e) {
            getLogger().error("Exception in finding targetServiceName : " + e);
            throw new AuthorizationException(e.toString());
        }

        if(getCacheLifetime()>0) {
            key = (role==null) ? key : key.concat(role);
            TimedLocalId tlocalId = getUsernameMapping(key);
            if( tlocalId!=null && tlocalId.age() < getCacheLifetime() &&
                    tlocalId.sameServiceName(resourceX509Name) &&
                    tlocalId.sameDesiredUserName(desiredUserName)) {
                getLogger().info("Using cached mapping for User with DN: " + X509Subject + " and Role " + role);
                getLogger().debug("with Desired user name: " + desiredUserName);

                return getgPlazmaAuthorizationRecord(tlocalId.getLocalId(), X509Subject, role);
            }
        }

        try {
            String[] hosts = HostUtil.getHosts();
            resourceDNSName = hosts.length>0 ? hosts[0] : null;
        } catch (Exception e) {
            getLogger().error("Exception in finding targetServiceName : " + e);
            throw new AuthorizationException(e.toString());
        }

        try {
            resourceX509Issuer = getTargetServiceIssuer();
        }
        catch (Exception e) {
            getLogger().error("Exception in finding targetServiceIssuer : " + e);
            throw new AuthorizationException(e.toString());
        }

        getLogger().info("Requesting mapping for User with DN: " + X509Subject + " and Role " + role);
        getLogger().debug("with Desired user name: " + desiredUserName);

        getLogger().debug("Mapping Service URL configuration: " + getMappingServiceURL());
        try {
            xacmlClient = new MapCredentialsClient();
            xacmlClient.setX509Subject(X509Subject);
            //xacmlClient.setCondorCanonicalNameID();
            //xacmlClient.setX509SubjectIssuer();
            //xacmlClient.setVO();
            //xacmlClient.setVOMSSigningSubject();
            //xacmlClient.setVOMSSigningIssuer();
            xacmlClient.setFqan(role);
            //xacmlClient.setCertificateSerialNumber(); //todo make Integer
            //xacmlClient.setCASerialNumber(); //todo make Integer
            //xacmlClient.setVOMS_DNS_Port();
            //xacmlClient.setCertificatePoliciesOIDs();
            //xacmlClient.setCertificateChain(); //todo make byte[]
            xacmlClient.setResourceType(XACMLConstants.RESOURCE_SE);
            xacmlClient.setResourceDNSHostName(resourceDNSName);
            xacmlClient.setResourceX509ID(resourceX509Name);
            xacmlClient.setResourceX509Issuer(resourceX509Issuer);
            xacmlClient.setRequestedaction(XACMLConstants.ACTION_ACCESS);
            //xacmlClient.setRSL_string();
        } catch (Exception e) {
            getLogger().error("Exception in XACML mapping client instantiation: " + e);
            throw new AuthorizationException(e.toString());
        }

        try {
            localId = xacmlClient.mapCredentials(getMappingServiceURL());
        }
        catch (Exception e ) {
            getLogger().error(" Exception occurred in mapCredentials: " + e);
            throw new AuthorizationException(e.toString());
        }

        if (localId == null) {
            String denied = DENIED_MESSAGE + ": No XACML mapping retrieved service for DN " + X509Subject + " and role " + role;
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }

        gPlazmaAuthorizationRecord authRecord = getgPlazmaAuthorizationRecord(localId, X509Subject, role);

        if(authRecord==null) {
            String denied = DENIED_MESSAGE + ": No authorization record found for username " + localId.getUserName() + " mapped from " + X509Subject + " and role " + role;
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }

        if(getCacheLifetime()>0) putUsernameMapping(key, new TimedLocalId(localId, resourceX509Name, desiredUserName));

        return authRecord;
    }

    private gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecord(LocalId localId, String subjectDN, String role) throws AuthorizationException {

        String username = localId.getUserName();

        if(username!=null) {
            getLogger().error("XACML mapping service returned Username: " + username);
            return getgPlazmaAuthorizationRecord(username, subjectDN, role);
        }

        if(localId.getUID() == null || localId.getGID() == null) {
            String denied = DENIED_MESSAGE + ": XACML mapping returned a null username and no uid or gid for DN " + subjectDN + " amd role " + role;
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }

        username = localId.getUID() + ":" + localId.getGID();
        gPlazmaAuthorizationRecord authRecord = getgPlazmaAuthorizationRecord(username, subjectDN, role);

        if(authRecord==null) {
            String denied = DENIED_MESSAGE + ": No authorization record found for username " + username + " mapped from DN " + subjectDN + " and role " + role;
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }

        int uid, gid;
        try {
            uid = Integer.decode(localId.getUID());
            gid = Integer.decode(localId.getGID());
        } catch (NumberFormatException nfe) {
            String denied = DENIED_MESSAGE + ":  could not be decoded to an integer";
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }

        if (authRecord.getUID() != uid || authRecord.getGID() != gid) {
            String denied = DENIED_MESSAGE + ": uid or gid from mapping service did not match uid or gid of authorization record";
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }

        return authRecord;
    }

    private synchronized void putUsernameMapping(String key, TimedLocalId tlocalId) {
        UsernameMap.put(key, tlocalId);
    }

    private synchronized TimedLocalId getUsernameMapping(String key) {
        return UsernameMap.get(key);
    }

    private class TimedLocalId extends LocalId {
        LocalId id;
        long timestamp;
        String serviceName=null;
        String desiredUserName=null;

        TimedLocalId(LocalId id) {
            this.id=id;
            this.timestamp=System.currentTimeMillis();
        }

        TimedLocalId(LocalId id, String serviceName, String desiredUserName) {
            this(id);
            this.serviceName=serviceName;
            this.desiredUserName=desiredUserName;
        }

        private LocalId getLocalId() {
            return id;
        }

        private long age() {
            return System.currentTimeMillis() - timestamp;
        }

        private boolean sameServiceName(String requestServiceName) {
            return (serviceName.equals(requestServiceName));
        }

        private boolean sameDesiredUserName(String requestDesiredUserName) {
            if(desiredUserName==null && requestDesiredUserName==null) return true;
            if(desiredUserName==null) return false;
            return (desiredUserName.equals(requestDesiredUserName));
        }
    }

} //end of class XACMLAuthorizationPlugin