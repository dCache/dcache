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

    public void debug(String s) {
        getLogger().debug("XACMLAuthorizationPlugin: authRequestID " + getAuthRequestID() + " " + s);
    }

    public void say(String s) {
        getLogger().info("XACMLAuthorizationPlugin: authRequestID " + getAuthRequestID() + " " + s);
    }

    public void warn(String s) {
        getLogger().warn("XACMLAuthorizationPlugin: authRequestID " + getAuthRequestID() + " " + s);
    }

    public void esay(String s) {
        getLogger().error("XACMLAuthorizationPlugin: authRequestID " + getAuthRequestID() + " " + s);
    }

    public gPlazmaAuthorizationRecord authorize(String X509Subject, String role, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        MapCredentialsClient xacmlClient;
        LocalId localId;
        String resourceDNSName;
        String resourceX509Name;

        try {
            String[] hosts = HostUtil.getHosts();
            resourceDNSName = hosts.length>0 ? hosts[0] : null;
        } catch (Exception e) {
            esay("Exception in finding targetServiceName : " + e);
            throw new AuthorizationException(e.toString());
        }

        try {
            resourceX509Name = getTargetServiceName();
        }
        catch (Exception e) {
            esay("Exception in finding targetServiceName : " + e);
            throw new AuthorizationException(e.toString());
        }

        String key = X509Subject;
        if(getCacheLifetime()>0) {
            key = (role==null) ? key : key.concat(role);
            TimedLocalId tlocalId = getUsernameMapping(key);
            if( tlocalId!=null && tlocalId.age() < getCacheLifetime() &&
                    tlocalId.sameServiceName(resourceX509Name) &&
                    tlocalId.sameDesiredUserName(desiredUserName)) {
                say("Using cached mapping for User with DN: " + X509Subject + " and Role " + role);
                debug("with Desired user name: " + desiredUserName);

                return getgPlazmaAuthorizationRecord(tlocalId.getLocalId(), X509Subject, role);
            }
        }

        say("Requesting mapping for User with DN: " + X509Subject + " and Role " + role);
        debug("with Desired user name: " + desiredUserName);

        debug("Mapping Service URL configuration: " + getMappingServiceURL());
        try {
            xacmlClient = new MapCredentialsClient();
        }
        catch (Exception e) {
            esay("Exception in XACML mapping client instantiation: " + e);
            throw new AuthorizationException(e.toString());
        }

        try {
            localId = xacmlClient.mapCredentials(X509Subject, role, resourceDNSName, resourceX509Name, getMappingServiceURL());
        }
        catch (Exception e ) {
            esay(" Exception occurred in mapCredentials: " + e);
            throw new AuthorizationException(e.toString());
        }

        if (localId == null) {
            String denied = DENIED_MESSAGE + ": No mapping retrieved service for DN " + X509Subject + " and role " + role;
            warn(denied);
            throw new AuthorizationException(denied);
        }

        if(getCacheLifetime()>0) putUsernameMapping(key, new TimedLocalId(localId, resourceX509Name, desiredUserName));

        return getgPlazmaAuthorizationRecord(localId, X509Subject, role);
    }

    private gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecord(LocalId localId, String subjectDN, String role) throws AuthorizationException {

        String username = localId.getUserName();

        if(username==null) {
            String denied = DENIED_MESSAGE + ": non-null user record received, but with a null username";
            warn(denied);
            throw new AuthorizationException(denied);
        } else {
            say("VO mapping service returned Username: " + username);
        }

        return getgPlazmaAuthorizationRecord(username, subjectDN, role);
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