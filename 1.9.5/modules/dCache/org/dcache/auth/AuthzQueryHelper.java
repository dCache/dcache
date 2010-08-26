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
import org.apache.log4j.Logger;
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
import java.util.*;

import diskCacheV111.vehicles.AuthenticationMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DNInfo;
import diskCacheV111.vehicles.X509Info;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import dmg.cells.nucleus.*;

public class AuthzQueryHelper {
    static Logger log = Logger.getLogger(AuthzQueryHelper.class.getSimpleName());

    long authRequestID;
    private static final Random random = new Random();
    private CellEndpoint caller;
    private boolean delegate_to_gplazma=false;
    private long cell_message_timeout;
    private int delegation_timeout=60000;

    public AuthzQueryHelper(CellEndpoint caller)
    throws AuthorizationException {
        this(caller, 180000L);
    }

    public AuthzQueryHelper(CellEndpoint caller, long msg_timeout)
    throws AuthorizationException {
        this(random.nextInt(Integer.MAX_VALUE), caller, msg_timeout);
    }

    public AuthzQueryHelper(long authRequestID, CellEndpoint caller, long msg_timeout)
    throws AuthorizationException {
        this.authRequestID=authRequestID;
        this.caller = caller;
        this.cell_message_timeout = msg_timeout;
    }

    public void setDelegateToGplazma(boolean boolarg) {
        delegate_to_gplazma = boolarg;
    }

    public void setDelegationTimeout(int deleg_t) {
        delegation_timeout = deleg_t;
    }

    public int getDelegationTimeout() {
        return delegation_timeout;
    }

    // Called on requesting cell
    //
    public AuthenticationMessage authorize(GSSContext serviceContext, String user, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {

        gPlazmaDelegationInfo deleginfo = new gPlazmaDelegationInfo(authRequestID, user, new Long(0));

        return  authorize(serviceContext, deleginfo, cellpath, caller);
    }

    public AuthenticationMessage authorize(GSSContext serviceContext, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {

        gPlazmaDelegationInfo deleginfo = new gPlazmaDelegationInfo(authRequestID, null, new Long(0));

        return  authorize(serviceContext, deleginfo, cellpath, caller);
    }

    public AuthorizationMessage getAuthorization(GSSContext serviceContext, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {
        return getAuthorization(serviceContext, null, cellpath, caller);
    }

    public AuthorizationMessage getAuthorization(GSSContext serviceContext,  String user, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {

        gPlazmaDelegationInfo deleginfo = new gPlazmaDelegationInfo(authRequestID, user, new Long(0));

        AuthenticationMessage authmessage = authorize(serviceContext, deleginfo, cellpath, caller);
        log.debug("AuthenticationMessage: "+authmessage);
        return new AuthorizationMessage(authmessage);
    }

    public AuthorizationMessage getAuthorization(String subjectDN, List<String> roles,String user,CellPath cellpath, CellEndpoint caller)
        throws AuthorizationException
    {
        AuthenticationMessage authmessage =
                authorize(subjectDN, roles, user,cellpath,caller);
        return new AuthorizationMessage(authmessage);
    }

    public AuthenticationMessage authorize(GSSContext serviceContext, gPlazmaDelegationInfo deleginfo, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {

        if(!delegate_to_gplazma) {
            //if (do_saz) {
            X509Certificate[] chain;

            ExtendedGSSContext extendedcontext;

            if (serviceContext instanceof ExtendedGSSContext) {
                extendedcontext = (ExtendedGSSContext) serviceContext;
            } else {
                String error = "Received context not instance of " +
                        "ExtendedGSSContext, AuthzQueryHelper " +
                        "exiting ...";
                log.error(error);
                throw new AuthorizationException(error);
            }

            try {
                chain = (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
            } catch(org.ietf.jgss.GSSException gsse ) {
                log.error(" Error extracting X509 chain from GSSContext: " +gsse);
                throw new AuthorizationException(gsse.toString());
            }

            return authorize(chain, deleginfo.getUser(), cellpath, caller); //todo add getSrcName to args
            //}

            //return
        }

        AuthenticationMessage authmessage = null;

        String authcellname = cellpath.getCellName();
        try {
            GSSName GSSIdentity = serviceContext.getSrcName();
            CellMessage m = new CellMessage(cellpath, deleginfo);
            m = caller.sendAndWait(m, cell_message_timeout) ;
            if(m==null) {
                throw new AuthorizationException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + GSSIdentity);
            }


            Object obj = m.getMessageObject();
            if(obj instanceof RemoteGsiftpDelegateUserCredentialsMessage) {
                if(((Message) obj).getId()!=authRequestID) {
                    throw new AuthorizationException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + ((Message) obj).getId());
                }
                GSIGssSocket authsock=null;

                try {

                    authsock = SslGsiSocketFactory.delegateCredential(
                            InetAddress.getByName(((RemoteGsiftpDelegateUserCredentialsMessage) obj).getHost()),
                            ((RemoteGsiftpDelegateUserCredentialsMessage) obj).getPort(),
                            //ExtendedGSSManager.getInstance().createCredential(GSSCredential.INITIATE_ONLY),
                            serviceContext.getDelegCred(),
                            false);
                    //say(this.toString() + " delegation appears to have succeeded");
                } catch ( UnknownHostException uhe ) {
                    throw new AuthorizationException("authRequestID " + authRequestID + " unknown host exception in delegation " + uhe);
                } catch(Exception e) {
                    throw new AuthorizationException("authRequestID " + authRequestID + " delegation failed for authentification of " + GSSIdentity + " " + e);
                }

                Object authobj=null;
                if(authsock!=null) {
                    try {
                        authsock.setSoTimeout(delegation_timeout);
                        InputStream authin = authsock.getInputStream();
                        ObjectInputStream authstrm = new ObjectInputStream(authin);
                        authobj = authstrm.readObject();
                        if(authobj==null) {
                            throw new AuthorizationException("authRequestID " + authRequestID + " authorization object was null for " + GSSIdentity);
                        } else {
                            if( authobj instanceof Exception ) throw (Exception) authobj;
                            authmessage = (AuthenticationMessage) authobj;
                            Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authmessage.getgPlazmaAuthzMap();
                            if(log.isDebugEnabled()) {
                                logAuthzMessage(user_auths, log);
                            }
                        }
                    } catch (IOException ioe) {
                        throw new AuthorizationException("authRequestID " + authRequestID + " could not receive authorization object for " + GSSIdentity + " " + ioe);
                    } catch (ClassCastException cce) {
                        throw new AuthorizationException("authRequestID " + authRequestID + " incorrect class for authorization object for " + GSSIdentity + " " + cce);
                    } finally {
                        authsock.close();
                    }

                } else {
                    throw new AuthorizationException("authRequestID " + authRequestID + " socket to receive authorization object was null for " + GSSIdentity);
                }

            } else {
                if( obj instanceof NoRouteToCellException)
                    throw (NoRouteToCellException) obj;
                if( obj instanceof Throwable )
                    throw (Throwable) obj;
            }
        } catch ( AuthorizationException ase ) {
            throw ase;
        } catch ( GSSException gsse ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NoRouteToCellException nre ) {
            throw new AuthorizationException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " received exception \"" + t.getMessage() + "\" at " + t.getStackTrace()[0]);
        }

        return authmessage;
    }

    public AuthenticationMessage authorize(String subjectDN, List<String> roles, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {
        DNInfo dnInfo = new DNInfo(subjectDN, roles, authRequestID);
        return authorize(dnInfo, cellpath, caller);
    }

    public AuthenticationMessage authorize(String subjectDN, String role, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {
        DNInfo dnInfo = new DNInfo(subjectDN, role, authRequestID);
        return authorize(dnInfo, cellpath, caller);
    }

    public AuthenticationMessage authorize(String subjectDN, List<String> roles, String user, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {
        DNInfo dnInfo = new DNInfo(subjectDN, roles, user, authRequestID);
        return authorize(dnInfo, cellpath, caller);
    }

    public AuthenticationMessage authorize(String subjectDN, String role, String user, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {
        DNInfo dnInfo = new DNInfo(subjectDN, role, user, authRequestID);
        return authorize(dnInfo, cellpath, caller);
    }

    public AuthenticationMessage authorize(DNInfo dnInfo, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {

        AuthenticationMessage authmessage = null;

        String authcellname = cellpath.getCellName();
        try {
            CellMessage m = new CellMessage(cellpath, dnInfo);
            m = caller.sendAndWait(m, cell_message_timeout) ;
            if(m==null) {
                throw new AuthorizationException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + dnInfo.getDN() + " and roles " + dnInfo.getFQANs());
            }

            Object authobj = m.getMessageObject();
            if (authobj==null) {
                throw new AuthorizationException(
                        "authRequestID " + authRequestID +
                        " authorization object was null for " +
                        dnInfo.getDN() + " and roles " + dnInfo.getFQANs());
            }
            else if(authobj instanceof AuthenticationMessage) {
                if(((AuthenticationMessage) authobj).getAuthRequestID()!=authRequestID) {
                    throw new AuthorizationException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + ((Message) authobj).getId());
                }
                authmessage = (AuthenticationMessage) authobj;
                Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authmessage.getgPlazmaAuthzMap();
                if(log.isDebugEnabled()) {
                    logAuthzMessage(user_auths, log);
                }
            } else {
                if( authobj instanceof NoRouteToCellException ) {
                    throw (NoRouteToCellException) authobj;
                }
                if( authobj instanceof Throwable ) {
                    throw (Throwable) authobj;
                }
                throw new AuthorizationException("unknown type of authobj");
            }
        } catch ( AuthorizationException ase ) {
            throw ase;
        } catch ( GSSException gsse ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NoRouteToCellException nre ) {
            throw new AuthorizationException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " received exception \"" + t.getMessage() + "\" at " + t.getStackTrace()[0]);
        }

        return authmessage;
    }

    public AuthenticationMessage authorize(X509Certificate[] chain, String user, CellPath cellpath, CellEndpoint caller) throws AuthorizationException {

        AuthenticationMessage authmessage = null;
        X509Info x509info = new X509Info(chain, user, authRequestID);

        String authcellname = cellpath.getCellName();
        try {
            CellMessage m = new CellMessage(cellpath, x509info);
            m = caller.sendAndWait(m, cell_message_timeout);
            if(m==null) {
                String subjectDN = X509CertUtil.getSubjectFromX509Chain(chain, false);
                throw new AuthorizationException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authorization of " + subjectDN);
            }

            Object authobj = m.getMessageObject();
            if(authobj==null) {
                String subjectDN = X509CertUtil.getSubjectFromX509Chain(chain, false);
                throw new AuthorizationException("authRequestID " + authRequestID + " authorization object was null for " + subjectDN);
            }
            if(authobj instanceof AuthenticationMessage) {
                authmessage = (AuthenticationMessage) authobj;
                if(authmessage.getAuthRequestID()!=authRequestID) {
                    throw new AuthorizationException("authRequestID " + authRequestID + " authentication failed: mismatch with returned authRequestID " + ((Message) authobj).getId());
                }
                Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths = authmessage.getgPlazmaAuthzMap();
                if(log.isDebugEnabled()) {
                    logAuthzMessage(user_auths, log);
                }
            } else {
                if( authobj instanceof Throwable )
                    throw (Throwable) authobj;
                else
                    throw new AuthorizationException("authRequestID " + authRequestID + " incorrect class for authorization object");
            }
        } catch ( AuthorizationException ase ) {
            throw ase;
        } catch ( GSSException gsse ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NoRouteToCellException nre ) {
            throw new AuthorizationException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " received exception \"" + t.getMessage() + "\" at " + t.getStackTrace()[0]);
        }

        return authmessage;
    }

    public static void logAuthzMessage(Map <NameRolePair, gPlazmaAuthorizationRecord> user_auths, Logger log) {
        for( NameRolePair nameAndRole : user_auths.keySet()) {
            StringBuilder sb = new StringBuilder("authorized ");
            sb.append(nameAndRole.toString()).append(" as: ");
            gPlazmaAuthorizationRecord record = user_auths.get(nameAndRole);
            if(record!=null) {
                sb.append(record.toShortString());
            } else {
                sb.append("null");
            }
            log.debug(sb);
        }
    }
}
