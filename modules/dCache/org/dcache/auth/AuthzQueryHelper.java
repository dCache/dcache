package org.dcache.auth;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSException;
import org.gridforum.jgss.ExtendedGSSContext;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.dcache.vehicles.gPlazmaDelegationInfo;
import org.dcache.vehicles.AuthorizationMessage;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.cells.CellStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gplazma.authz.AuthorizationException;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.util.NameRolePair;

import java.security.cert.X509Certificate;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.Map;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.AuthenticationMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DNInfo;
import diskCacheV111.vehicles.X509Info;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CDC;

public class AuthzQueryHelper
{
    private static final Logger log =
        LoggerFactory.getLogger(AuthzQueryHelper.class);

    private long authRequestID;
    private static final Random random = new Random();
    private CellEndpoint caller;
    private boolean delegate_to_gplazma=false;
    private int delegation_timeout=60000;
    private CellStub gPlazmaStub;

    public AuthzQueryHelper(CellEndpoint endpoint)
        throws AuthorizationException
    {
        this(endpoint, random.nextInt(Integer.MAX_VALUE));
    }

    public AuthzQueryHelper(CellEndpoint endpoint, long authRequestID)
        throws AuthorizationException
    {
        this(new CellStub(endpoint, new CellPath("gPlazma"), 180000L),
             authRequestID);
    }

    public AuthzQueryHelper(CellStub stub)
        throws AuthorizationException
    {
        this(stub, random.nextInt(Integer.MAX_VALUE));
    }

    public AuthzQueryHelper(CellStub stub, long authRequestID)
        throws AuthorizationException
    {
        this.authRequestID = authRequestID;
        this.gPlazmaStub = stub;
    }

    public void setDelegateToGplazma(boolean delegate)
    {
        delegate_to_gplazma = delegate;
    }

    public boolean getDelegateToGplazma()
    {
        return delegate_to_gplazma;
    }

    public void setDelegationTimeout(int timeout)
    {
        delegation_timeout = timeout;
    }

    public int getDelegationTimeout()
    {
        return delegation_timeout;
    }

    // Called on requesting cell
    //
    public AuthenticationMessage
        authorize(GSSContext serviceContext, String user)
        throws AuthorizationException
    {
        gPlazmaDelegationInfo deleginfo =
            new gPlazmaDelegationInfo(authRequestID, user, Long.valueOf(0));
        return authorize(serviceContext, deleginfo);
    }

    public AuthenticationMessage authorize(GSSContext serviceContext)
        throws AuthorizationException
    {
        gPlazmaDelegationInfo deleginfo =
            new gPlazmaDelegationInfo(authRequestID, null, Long.valueOf(0));
        return authorize(serviceContext, deleginfo);
    }

    public AuthorizationMessage getAuthorization(GSSContext serviceContext, String user)
        throws AuthorizationException
    {
        gPlazmaDelegationInfo deleginfo =
            new gPlazmaDelegationInfo(authRequestID, user, Long.valueOf(0));
        AuthenticationMessage authmessage =
            authorize(serviceContext, deleginfo);
        return new AuthorizationMessage(authmessage);
    }

    public AuthorizationMessage getAuthorization(GSSContext serviceContext)
        throws AuthorizationException
    {
        return getAuthorization(serviceContext, null);
    }

    public AuthorizationMessage getAuthorization(String subjectDN, List<String> roles,String user)
        throws AuthorizationException
    {
        AuthenticationMessage authmessage =
                authorize(subjectDN, roles, user);
        return new AuthorizationMessage(authmessage);
    }


    public AuthenticationMessage
        authorize(GSSContext serviceContext, gPlazmaDelegationInfo deleginfo)
        throws AuthorizationException
    {
        if (!delegate_to_gplazma) {
            if (!(serviceContext instanceof ExtendedGSSContext)) {
                log.error("Received context not instance of ExtendedGSSContext, AuthorizationController exiting ...");
                return null;
            }

            try {
                ExtendedGSSContext extendedcontext =
                    (ExtendedGSSContext) serviceContext;
                X509Certificate[] chain =
                    (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
                return authorize(chain, deleginfo.getUser()); //todo add getSrcName to args
            } catch (GSSException e) {
                log.error("Error extracting X509 chain from GSSContext: " + e);
                throw new AuthorizationException(e);
            }
        }

        try {
            GSSName GSSIdentity = serviceContext.getSrcName();
            RemoteGsiftpDelegateUserCredentialsMessage reply =
                sendAndWaitGPlazmaRequest(deleginfo, RemoteGsiftpDelegateUserCredentialsMessage.class);

            if (reply.getId() != authRequestID) {
                throw new AuthorizationException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + reply.getId());
            }

            GSIGssSocket authsock;
            try {
                authsock =
                    SslGsiSocketFactory.delegateCredential(InetAddress.getByName(reply.getHost()),
                                                           reply.getPort(),
                                                           serviceContext.getDelegCred(),
                                                           false);
            } catch (UnknownHostException e) {
                throw new AuthorizationException("authRequestID " + authRequestID + " unknown host exception in delegation", e);
            } catch (Exception e) {
                throw new AuthorizationException("authRequestID " + authRequestID + " delegation failed for authentification of " + GSSIdentity, e);
            }

            if (authsock == null) {
                throw new AuthorizationException("authRequestID " + authRequestID + " socket to receive authorization object was null for " + GSSIdentity);
            }

            try {
                authsock.setSoTimeout(delegation_timeout);
                InputStream authin = authsock.getInputStream();
                ObjectInputStream authstrm = new ObjectInputStream(authin);
                Object authobj = authstrm.readObject();
                if (authobj == null) {
                    throw new AuthorizationException("authRequestID " + authRequestID + " authorization object was null for " + GSSIdentity);
                }

                if (authobj instanceof Exception) {
                    Exception e = (Exception) authobj;
                    throw new AuthorizationException("authRequestID " + authRequestID + " received exception \"" + e.getMessage() + "\"", e);
                }

                AuthenticationMessage authmessage =
                    (AuthenticationMessage) authobj;
                Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authmessage.getgPlazmaAuthzMap();
                logAuthzMessage(user_auths, log);
                return authmessage;
            } catch (IOException e) {
                throw new AuthorizationException("authRequestID " + authRequestID + " could not receive authorization object for " + GSSIdentity, e);
            } catch (ClassCastException e) {
                throw new AuthorizationException("authRequestID " + authRequestID + " incorrect class for authorization object for " + GSSIdentity, e);
            } finally {
                try {
                    authsock.close();
                } catch (IOException e) {
                    log.warn("Unexpected failure to close socket", e);
                }
            }
        } catch (CacheException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " " + e.getMessage(), e);
        } catch (GSSException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " error getting source from context", e);
        } catch (InterruptedException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " message thread was interrupted", e);
        } catch (ClassNotFoundException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " class of object returned from " + gPlazmaStub.getDestinationPath() + " not found", e);
        }
    }

    public AuthenticationMessage authorize(String subjectDN, List<String> roles)
        throws AuthorizationException
    {
        return authorize(new DNInfo(subjectDN, roles, authRequestID));
    }

    public AuthenticationMessage authorize(String subjectDN, String role)
        throws AuthorizationException
    {
        return authorize(new DNInfo(subjectDN, role, authRequestID));
    }

    public AuthenticationMessage
        authorize(String subjectDN, List<String> roles, String user)
        throws AuthorizationException
    {
        return authorize(new DNInfo(subjectDN, roles, user, authRequestID));
    }

    public AuthenticationMessage
        authorize(String subjectDN, String role, String user)
        throws AuthorizationException
    {
        return authorize(new DNInfo(subjectDN, role, user, authRequestID));
    }

    public AuthenticationMessage authorize(DNInfo dnInfo)
        throws AuthorizationException
    {
        try {
            AuthenticationMessage reply =
                sendAndWaitGPlazmaRequest(dnInfo, AuthenticationMessage.class);
            if (reply.getAuthRequestID() != authRequestID) {
                throw new AuthorizationException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + reply.getId());
            }
            Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths =
                reply.getgPlazmaAuthzMap();
            logAuthzMessage(user_auths, log);
            return reply;
        } catch (CacheException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " " + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " message thread was interrupted", e);
        }
    }

    public AuthenticationMessage authorize(X509Certificate[] chain, String user)
        throws AuthorizationException
    {
        X509Info x509info = new X509Info(chain, user, authRequestID);

        try {
            AuthenticationMessage reply =
                sendAndWaitGPlazmaRequest(x509info, AuthenticationMessage.class);
            if (reply.getAuthRequestID() != authRequestID) {
                throw new AuthorizationException("authRequestID " + authRequestID + " authentication failed: mismatch with returned authRequestID " + reply.getId());
            }
            Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths =
                reply.getgPlazmaAuthzMap();
            logAuthzMessage(user_auths, log);
            return reply;
        } catch (CacheException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " " + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new AuthorizationException("authRequestID " + authRequestID + " message thread was interrupted", e);
        }
    }

    public static void
        logAuthzMessage(Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths,
                        Logger log)
    {
        if (log.isDebugEnabled()) {
            for (NameRolePair nameAndRole: user_auths.keySet()) {
                StringBuilder sb = new StringBuilder("authorized ");
                sb.append(nameAndRole.toString()).append(" as: ");
                gPlazmaAuthorizationRecord record = user_auths.get(nameAndRole);
                if (record != null) {
                    sb.append(record.toShortString());
                } else {
                    sb.append("NOT AUTHORIZED");
                }
                log.debug(sb.toString());
            }
        }
    }

    private <T extends Serializable>
                       T sendAndWaitGPlazmaRequest(Serializable load,
                                                   Class<T> type)
        throws AuthorizationException, CacheException, InterruptedException
    {
        Object cdcSession = CDC.getSession();
        try {
            StringBuilder sb = new StringBuilder();
            if (cdcSession != null) {
                sb.append(cdcSession);
            }

            // Add current authRequestId. FIXME: This is a miss use of
            // session IDs.
            sb.append(":authId:").append(authRequestID);
            CDC.setSession(sb.toString());

            // FIXME: Since the gPlazma cell does not follow the
            // dCache message conventions, we cannot rely on CellStub
            // doing all the error handling. We have to do part of it
            // here.
            Serializable reply =
                gPlazmaStub.sendAndWait(load, Serializable.class);
            if (type.isInstance(reply)) {
                if (reply instanceof Message) {
                    Message msg = (Message) reply;
                    int rc = msg.getReturnCode();
                    if (rc != 0) {
                        throw new CacheException(rc, String.valueOf(msg.getErrorObject()));
                    }
                }
                return (T) reply;
            } else if (reply instanceof AuthorizationException) {
                throw (AuthorizationException) reply;
            } else {
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         reply.toString());
            }
        } finally {
            //restore original session
            CDC.setSession(cdcSession);
        }
    }
}
