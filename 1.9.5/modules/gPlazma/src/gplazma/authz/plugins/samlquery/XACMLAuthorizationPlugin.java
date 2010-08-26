// $Id: XACMLAuthorizationPlugin.java,v 1.28 2007-10-23 17:11:24 tdh Exp $
// $Log: not supported by cvs2svn $

package gplazma.authz.plugins.samlquery;

import java.util.*;
import java.lang.*;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CRLException;
import java.io.IOException;

import gplazma.authz.AuthorizationException;
import gplazma.authz.util.HostUtil;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import org.opensciencegrid.authz.xacml.common.LocalId;
import org.opensciencegrid.authz.xacml.common.XACMLConstants;
import org.opensciencegrid.authz.xacml.client.MapCredentialsClient;
import org.apache.log4j.*;
import org.ietf.jgss.GSSException;
import org.glite.voms.VOMSAttribute;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public class XACMLAuthorizationPlugin extends SAMLAuthorizationPlugin {

    private static final HashMap<String, TimedLocalId> UsernameMap = new HashMap();


    public XACMLAuthorizationPlugin(String mappingServiceURL, String storageAuthzPath, long authRequestID) {
        super(mappingServiceURL, storageAuthzPath, authRequestID);
        getLogger().info("xacml-vo-mapping plugin now loaded for URL " + mappingServiceURL);
    }

    public gPlazmaAuthorizationRecord authorize(String X509Subject, String fqan, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        String CondorCanonicalNameID=null;
        String X509SubjectIssuer=null;
        String VO=null;
        String VOMSSigningSubject=null;
        String VOMSSigningIssuer=null;

        String CertificateSerialNumber=null; //todo make Integer
        String CASerialNumber=null; //todo make Integer
        String VOMS_DNS_Port=null;
        String CertificatePoliciesOIDs=null;
        String CertificateChain=null; //todo make byte[]
        String resourceType=XACMLConstants.RESOURCE_SE;
        String resourceDNSHostName;
        String resourceX509ID;
        String resourceX509Issuer;
        String requestedaction=XACMLConstants.ACTION_ACCESS;
        String RSL_string=null;
        MapCredentialsClient xacmlClient;
        LocalId localId;
        String key = X509Subject;

        try {
            X509SubjectIssuer = X509CertUtil.getSubjectX509Issuer(chain);
        } catch (Exception e) {
            getLogger().warn("Could not determine subject-x509-issuer : " + e.getMessage());
        }

        VOMSAttribute vomsAttr=null;
            if (chain !=null && fqan !=null) {
                vomsAttr = X509CertUtil.getVOMSAttribute(chain, fqan);
            }
            if (vomsAttr!=null) {
                VO = vomsAttr.getVO();
                String X500IssuerName = vomsAttr.getAC().getIssuer().toString();
                VOMSSigningSubject = X509CertUtil.toGlobusDN(X500IssuerName);
            }

        try {
            resourceX509ID = getTargetServiceName();
        }
        catch (Exception e) {
            getLogger().error("Exception in finding targetServiceName : " + e);
            throw new AuthorizationException(e.toString());
        }

        synchronized (UsernameMap) {
            if(getCacheLifetime()>0) {
                key = (fqan==null) ? key : key.concat(fqan);
                TimedLocalId tlocalId = getUsernameMapping(key);
                if( tlocalId!=null && tlocalId.age() < getCacheLifetime() &&
                        tlocalId.sameServiceName(resourceX509ID) &&
                        tlocalId.sameDesiredUserName(desiredUserName)) {
                    getLogger().info("Using cached mapping for User with DN: " + X509Subject + " and Role " + fqan + " with Desired user name: " + desiredUserName);

                    return getgPlazmaAuthorizationRecord(tlocalId.getLocalId(), X509Subject, fqan);
                }
            }

            try {
                String[] hosts = HostUtil.getHosts();
                resourceDNSHostName = hosts.length>0 ? hosts[0] : null;
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

            getLogger().info("Requesting mapping for User with DN: " + X509Subject + " and Role " + fqan + " with Desired user name: " + desiredUserName);

            getLogger().debug("Mapping Service URL configuration: " + getMappingServiceURL());
            try {
                xacmlClient = new MapCredentialsClient();
                xacmlClient.setX509Subject(X509Subject);
                xacmlClient.setCondorCanonicalNameID(CondorCanonicalNameID);
                xacmlClient.setX509SubjectIssuer(X509SubjectIssuer);
                xacmlClient.setVO(VO);
                xacmlClient.setVOMSSigningSubject(VOMSSigningSubject);
                xacmlClient.setVOMSSigningIssuer(VOMSSigningIssuer);
                xacmlClient.setFqan(fqan);
                xacmlClient.setCertificateSerialNumber(CertificateSerialNumber); //todo make Integer
                xacmlClient.setCASerialNumber(CASerialNumber); //todo make Integer
                xacmlClient.setVOMS_DNS_Port(VOMS_DNS_Port);
                xacmlClient.setCertificatePoliciesOIDs(CertificatePoliciesOIDs);
                xacmlClient.setCertificateChain(CertificateChain); //todo make byte[]
                xacmlClient.setResourceType(resourceType);
                xacmlClient.setResourceDNSHostName(resourceDNSHostName);
                xacmlClient.setResourceX509ID(resourceX509ID);
                xacmlClient.setResourceX509Issuer(resourceX509Issuer);
                xacmlClient.setRequestedaction(requestedaction);
                xacmlClient.setRSL_string(RSL_string);
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
                String denied = DENIED_MESSAGE + ": No XACML mapping retrieved service for DN " + X509Subject + " and role " + fqan;
                getLogger().warn(denied);
                throw new AuthorizationException(denied);
            }

            gPlazmaAuthorizationRecord gauthrec = getgPlazmaAuthorizationRecord(localId, X509Subject, fqan);

            if(gauthrec==null) {
                String denied = DENIED_MESSAGE + ": No authorization record found for username " + localId.getUserName() + " mapped from " + X509Subject + " and role " + fqan;
                getLogger().warn(denied);
                throw new AuthorizationException(denied);
            }

            if(getCacheLifetime()>0) putUsernameMapping(key, new TimedLocalId(localId, resourceX509ID, desiredUserName));

            return gauthrec;
        }
    }

    private gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecord(LocalId localId, String subjectDN, String role) throws AuthorizationException {

        String username = localId.getUserName();

        if(username!=null) {
            getLogger().info("xacml-vo-mapping service returned Username: " + username);
            gPlazmaAuthorizationRecord gauthrec = getgPlazmaAuthorizationRecord(username, subjectDN, role);
            return gauthrec;
        } else {
            if(localId.getUID() == null || localId.getGID() == null) {
                String denied = DENIED_MESSAGE + ": XACML mapping returned a null username and no uid or gid for DN " + subjectDN + " amd role " + role;
                getLogger().warn(denied);
                throw new AuthorizationException(denied);
            } else {
                getLogger().info("xacml-vo-mapping service returned uid:primarygid " + localId.getUID() + ":" + localId.getGID());
            }
        }

        username = localId.getUID() + ":" + localId.getGID();
        gPlazmaAuthorizationRecord gauthrec = getgPlazmaAuthorizationRecord(username, subjectDN, role);

        if(gauthrec==null) {
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

        if (gauthrec.getUID() != uid || gauthrec.getGID() != gid) {
            String denied = DENIED_MESSAGE + ": uid or gid from mapping service did not match uid or gid of authorization record";
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }


        // If the authorization service returned secondary GIDs, add them to the GIDs obtained from the local policy.
        String[] secndgids = localId.getSecondaryGIDs();
        if (secndgids !=null && secndgids.length > 0) {
            int[] authzgids = gauthrec.getGIDs();
            LinkedHashSet<String> allGIDs = new LinkedHashSet<String>(authzgids.length + secndgids.length);
            for (int gidl : authzgids) {
                allGIDs.add(Integer.toString(gidl));
            }
            allGIDs.addAll(Arrays.asList(secndgids));
            int numGIDs = allGIDs.size();
            if(numGIDs != authzgids.length) {
                int[] newGIDs = new int[numGIDs];
                Iterator<String> GIDsIter = allGIDs.iterator();
                int i=0;
                while (GIDsIter.hasNext()) {
                    newGIDs[i++] = Integer.decode(GIDsIter.next());
                }
                gauthrec =  new gPlazmaAuthorizationRecord(gauthrec.getUsername(),
                        gauthrec.isReadOnly(),
                        gauthrec.getPriority(),
                        gauthrec.getUID(),
                        newGIDs,
                        gauthrec.getHome(),
                        gauthrec.getRoot(),
                        gauthrec.getFsRoot());
            }
        }

        return gauthrec;
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
