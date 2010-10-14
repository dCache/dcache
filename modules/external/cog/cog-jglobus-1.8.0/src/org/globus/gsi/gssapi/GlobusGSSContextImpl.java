/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi.gssapi;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.ChannelBinding;

import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSCredential;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Vector;
import java.util.LinkedList;
import java.util.Date;
import java.util.Calendar;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.KeyPair;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.security.interfaces.RSAPrivateKey;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.CertificateRevocationLists;
import org.globus.gsi.CertUtil;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.ptls.PureTLSUtil;
import org.globus.gsi.ptls.PureTLSContext;
import org.globus.gsi.ptls.PureTLSTrustedCertificates;
import org.globus.gsi.proxy.ProxyPathValidator;
import org.globus.gsi.proxy.ProxyPathValidatorException;
import org.globus.gsi.proxy.ProxyPolicyHandler;
import org.globus.util.I18n;
import org.globus.common.CoGProperties;

import COM.claymoresystems.ptls.SSLConn;
import COM.claymoresystems.ptls.SSLRecord;
import COM.claymoresystems.ptls.SSLDebug;
import COM.claymoresystems.ptls.SSLCipherSuite;
import COM.claymoresystems.ptls.SSLCipherState;
import COM.claymoresystems.ptls.SSLHandshake;
import COM.claymoresystems.sslg.SSLPolicyInt;
import COM.claymoresystems.sslg.CertVerifyPolicyInt;
import COM.claymoresystems.cert.X509Cert;
import COM.claymoresystems.util.Util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of SSL/GSI mechanism for Java GSS-API. The implementation
 * is based on the <a href="http://www.rtfm.com/puretls/">PureTLS library</a>
 * (for SSL API) and the 
 * <a href="http://www.bouncycastle.org/">BouncyCastle library</a> 
 * (for certificate processing API).
 * <BR>
 * The implementation is not designed to be thread-safe.
 */
public class GlobusGSSContextImpl implements ExtendedGSSContext {
    
    private static Log logger = 
        LogFactory.getLog(GlobusGSSContextImpl.class.getName());

    private static I18n i18n =
            I18n.getI18n("org.globus.gsi.gssapi.errors",
                         GlobusGSSContextImpl.class.getClassLoader());


    private static Log sslLogger = 
        LogFactory.getLog(SSLDebug.class.getName());


    /**
     * KeyPair generation with cache of keypairs if configured

     */

    private KeyPairCache keyPairCache = KeyPairCache.getKeyPairCache();

    
    /** Used to distinguish between a token created by 
     * <code>wrap</code> with {@link GSSConstants#GSI_BIG
     * GSSConstants.GSI_BIG}
     * QoP and a regular token created by <code>wrap</code>. */
    public static final int GSI_WRAP = 26; /** SSL3_RT_GSSAPI_OPENSSL */

    private static final int GSI_SEQUENCE_SIZE = 8;
    
    private static final int GSI_MESSAGE_DIGEST_PADDING = 12;
    
    private static final short [] NO_ENCRYPTION = {SSLPolicyInt.TLS_RSA_WITH_NULL_MD5};
    
    private static final byte[] DELEGATION_TOKEN = new byte[] {GSIConstants.DELEGATION_CHAR};
    
    private static final int 
        UNDEFINED = 0,
        INITIATE = 1,
        ACCEPT = 2;

    /** Handshake state */
    protected int state = HANDSHAKE; 

    /* handshake states */
    private static final int
        HANDSHAKE = 0,
        CLIENT_START_DEL = 2,
        CLIENT_END_DEL = 3,
        SERVER_START_DEL = 4,
        SERVER_END_DEL = 5;

    /** Delegation state */
    protected int delegationState = DELEGATION_START;

    /* delegation states */
    private static final int
        DELEGATION_START = 0,
        DELEGATION_SIGN_CERT = 1,
        DELEGATION_COMPLETE_CRED = 2;

    /** Credential delegated using delegation API */
    protected ExtendedGSSCredential delegatedCred;

    /** Delegation finished indicator */
    protected boolean delegationFinished = false;

    // gss context state variables
    protected boolean credentialDelegation = false;
    protected boolean anonymity = false;
    protected boolean encryption = true;
    protected boolean established = false;

    /** The name of the context initiator */
    protected GSSName sourceName = null;

    /** The name of the context acceptor */
    protected GSSName targetName = null;

    /** Context role */
    protected int role = UNDEFINED;

    /** Credential delegated during context establishment */
    protected ExtendedGSSCredential delegCred;

    // these can be set via setOption
    protected Integer delegationType = GSIConstants.DELEGATION_TYPE_LIMITED;
    protected Integer gssMode = GSIConstants.MODE_GSI;
    protected Boolean checkContextExpiration = Boolean.FALSE;
    protected Boolean rejectLimitedProxy = Boolean.FALSE;
    protected Boolean requireClientAuth = Boolean.TRUE;
    protected Boolean acceptNoClientCerts = Boolean.FALSE;
    protected Boolean requireAuthzWithDelegation = Boolean.TRUE;

    // *** implementation-specific variables ***
    
    /** Credential of this context. Might be anonymous */
    protected GlobusGSSCredentialImpl ctxCred;
    
    /** Expected target name. Used for authorization in initiator */
    protected GSSName expectedTargetName = null;

    /** Context expiration date. */
    protected Date goodUntil = null;
    
    protected SSLConn conn;
    protected PureTLSContext context;
    protected SSLPolicyInt policy;
    protected TokenInputStream in;
    protected ByteArrayOutputStream out;
    protected BouncyCastleCertProcessingFactory certFactory;

    /** Used during delegation */
    protected KeyPair keyPair;

    /* Needed to verifing certs */
    protected TrustedCertificates tc;
    
    protected Map proxyPolicyHandlers;

    /** Limited peer credentials */
    protected Boolean peerLimited = null;

    /**
     * @param target expected target name. Can be null.
     * @param cred credential. Cannot be null. Might be anonymous.
     */
    public GlobusGSSContextImpl(GSSName target,
                                GlobusGSSCredentialImpl cred)
        throws GSSException {

        if (cred == null) {
            throw new GSSException(GSSException.NO_CRED);
        }

        this.expectedTargetName = target;
        this.ctxCred = cred;
        
        this.context = new PureTLSContext();

        CertVerifyPolicyInt certPolicy = PureTLSUtil.getDefaultCertVerifyPolicy();
        
        this.policy = new SSLPolicyInt();
        this.policy.negotiateTLS(false);
        this.policy.waitOnClose(false);
        this.policy.setCertVerifyPolicy(certPolicy);
        this.context.setPolicy(policy);

        setSSLDebugging();
    }

    private void setSSLDebugging() {
        if (sslLogger.isTraceEnabled()) {
            SSLDebug.setDebug( 0xffff );
        } else if (sslLogger.isDebugEnabled()) {
            SSLDebug.setDebug( SSLDebug.DEBUG_CERT );
        }
    }

    /**
     * This function drives the accepting side of the context establishment
     * process. It is expected to be called in tandem with the
     * {@link #initSecContext(byte[], int, int) initSecContext} function.
     * <BR>
     * The behavior of context establishment process can be modified by 
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE}
     * and {@link GSSConstants#REJECT_LIMITED_PROXY 
     * GSSConstants.REJECT_LIMITED_PROXY} context options. If the
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE} 
     * option is set to 
     * {@link GSIConstants#MODE_SSL GSIConstants.MODE_SSL}
     * the context establishment process will be compatible with regular SSL
     * (no credential delegation support). If the option is set to
     * {@link GSIConstants#MODE_GSI GSIConstants.MODE_GSI}
     * credential delegation during context establishment process will be accepted.
     * If the {@link GSSConstants#REJECT_LIMITED_PROXY
     * GSSConstants.REJECT_LIMITED_PROXY} option is enabled, a peer
     * presenting limited proxy credential will be automatically 
     * rejected and the context establishment process will be aborted.
     * 
     * @return a byte[] containing the token to be sent to the peer.
     *         null indicates that no token is generated (needs more data)
     */
    public byte[] acceptSecContext(byte[] inBuff, int off, int len) 
        throws GSSException {
        logger.debug("enter acceptSecContext");

        if (this.conn == null) {
            this.role = ACCEPT;
            
            if (this.ctxCred.getName().isAnonymous()) {
                throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL,
                                             GlobusGSSException.UNKNOWN,
                                             "acceptCtx00");
            }

            if (this.ctxCred.getUsage() != GSSCredential.ACCEPT_ONLY &&
                this.ctxCred.getUsage() != GSSCredential.INITIATE_AND_ACCEPT) {
                throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL,
                                             GlobusGSSException.UNKNOWN,
                                             "badCredUsage");
            }

            setCredential();
            init(SSLConn.SSL_SERVER);
        }

        this.out.reset();
        this.in.putToken(inBuff, off, len);

        switch (state) {
            
        case HANDSHAKE:
            
            try {
                this.conn.getHandshake().processHandshake();
                if (this.conn.getHandshake().finishedP()) {
                    logger.debug("acceptSecContext handshake finished");
                    handshakeFinished();
                    
                    // acceptor
                    X509Certificate cert = this.ctxCred.getCertificateChain()[0];
                    setGoodUntil(cert.getNotAfter());
                    this.targetName = this.ctxCred.getName();

                    // initiator - peer
                    Vector chain = this.conn.getCertificateChain();
                    if (chain == null || chain.size() == 0) {
                        this.sourceName = new GlobusGSSName();
                        this.anonymity = true;
                    } else {
                        X509Cert crt = (X509Cert)chain.elementAt(chain.size()-1);
                        setGoodUntil(crt.getValidityNotAfter());
                        
                        String identity = verifyChain(chain);

                        this.sourceName = new GlobusGSSName(identity);
                        this.anonymity = false;
                    }

                    if (this.gssMode == GSIConstants.MODE_GSI) {
                        this.state = SERVER_START_DEL;
                    } else {
                        setDone();
                    }
                }
            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }

            break;

        case SERVER_START_DEL:
            
            try {
                if (this.in.available() <= 0) {
                    return null;
                }

                int delChar = this.conn.getInStream().read();
                if (delChar != GSIConstants.DELEGATION_CHAR) {
                    setDone();
                    break;
                }
                
                Vector chain = this.conn.getCertificateChain();
                if (chain == null || chain.size() == 0) {
                    throw new GlobusGSSException(GSSException.FAILURE, 
                                                 GlobusGSSException.DELEGATION_ERROR,
                                                 "noClientCert");
                }

                X509Certificate tmpCert = 
                    PureTLSUtil.convertCert((X509Cert)chain.lastElement());
                byte [] req = generateCertRequest(tmpCert);
                this.conn.getOutStream().write(req, 0, req.length);
            } catch (GeneralSecurityException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }
            
            this.state = SERVER_END_DEL;
            break;

        case SERVER_END_DEL:

            try {
                if (this.in.available() <= 0) {
                    return null;
                }

                X509Certificate certificate = CertUtil.loadCertificate(this.conn.getInStream());

                if (logger.isTraceEnabled()) {
                    logger.trace("Received delegated cert: " + 
                               certificate.toString());
                }

                verifyDelegatedCert(certificate);
                
                Vector chain = this.conn.getCertificateChain();
                int chainLen = chain.size();
                X509Certificate [] newChain = new X509Certificate[chainLen + 1];
                newChain[0] = certificate;
                for (int i=0;i<chainLen;i++) {
                    newChain[i+1] = PureTLSUtil.convertCert((X509Cert)chain.elementAt(chainLen - 1 - i));
                }

                GlobusCredential proxy = 
                    new GlobusCredential(this.keyPair.getPrivate(), newChain);

                this.delegCred = 
                    new GlobusGSSCredentialImpl(proxy,
                                                GSSCredential.INITIATE_AND_ACCEPT);
                
            } catch (GeneralSecurityException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }
            setDone();
            break;

        default:
            throw new GSSException(GSSException.FAILURE);
        }

        // TODO: we could also add a check if this.in is empty to make sure
        // we processed all data.

        logger.debug("exit acceptSeContext");
        return (this.out.size() > 0) ? this.out.toByteArray() : null;
    }

    /**
     * This function drives the initiating side of the context establishment
     * process. It is expected to be called in tandem with the
     * {@link #acceptSecContext(byte[], int, int) acceptSecContext} function.
     * <BR>
     * The behavior of context establishment process can be modified by 
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE},
     * {@link GSSConstants#DELEGATION_TYPE GSSConstants.DELEGATION_TYPE}, and
     * {@link GSSConstants#REJECT_LIMITED_PROXY GSSConstants.REJECT_LIMITED_PROXY}
     * context options. If the {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE} 
     * option is set to {@link GSIConstants#MODE_SSL GSIConstants.MODE_SSL}
     * the context establishment process will be compatible with regular SSL
     * (no credential delegation support). If the option is set to
     * {@link GSIConstants#MODE_GSI GSIConstants.GSS_MODE_GSI}
     * credential delegation during context establishment process will performed.
     * The delegation type to be performed can be set using the 
     * {@link GSSConstants#DELEGATION_TYPE GSSConstants.DELEGATION_TYPE}
     * context option. If the {@link GSSConstants#REJECT_LIMITED_PROXY 
     * GSSConstants.REJECT_LIMITED_PROXY} option is enabled, 
     * a peer presenting limited proxy credential will be automatically 
     * rejected and the context establishment process will be aborted.
     *
     * @return a byte[] containing the token to be sent to the peer.
     *         null indicates that no token is generated (needs more data). 
     */
    public byte[] initSecContext(byte[] inBuff, int off, int len) 
        throws GSSException {
        logger.debug("enter iniSecContext");

        if (this.conn == null) {
            this.role = INITIATE;

            if (this.credentialDelegation) {
                if (this.gssMode == GSIConstants.MODE_SSL) {
                    throw new GlobusGSSException(GSSException.FAILURE,
                                                 GlobusGSSException.BAD_ARGUMENT,
                                                 "initCtx00");
                }
                if (this.anonymity) {
                    throw new GlobusGSSException(GSSException.FAILURE,
                                                 GlobusGSSException.BAD_ARGUMENT,
                                                 "initCtx01");
                }
            }

            if (this.anonymity || this.ctxCred.getName().isAnonymous()) {
                this.anonymity = true;
            } else {
                this.anonymity = false;
                setCredential();
                
                if (this.ctxCred.getUsage() != GSSCredential.INITIATE_ONLY &&
                    this.ctxCred.getUsage() != GSSCredential.INITIATE_AND_ACCEPT) {
                    throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL,
                                                 GlobusGSSException.UNKNOWN,
                                                 "badCredUsage");
                }
            }
            
            init(SSLConn.SSL_CLIENT);
        }

        // Unless explicitly disabled, check if delegation is
        // requested and expected target is null
        logger.debug("Require authz with delegation" 
                     + this.requireAuthzWithDelegation);
        if (!Boolean.FALSE.equals(this.requireAuthzWithDelegation)) {

            if (this.expectedTargetName == null && 
                this.credentialDelegation) {
                throw new GlobusGSSException(GSSException.FAILURE,
                                             GlobusGSSException.BAD_ARGUMENT,
                                         "initCtx02");
            }
        }

        this.out.reset();
        this.in.putToken(inBuff, off, len);

        switch (state) {
            
        case HANDSHAKE:
            
            try {
                this.conn.getHandshake().processHandshake();
                if (this.conn.getHandshake().finishedP()) {
                    logger.debug("iniSecContext handshake finished");
                    handshakeFinished();

                    Vector chain = this.conn.getCertificateChain();
                    X509Cert crt = (X509Cert)chain.elementAt(chain.size()-1);
                    setGoodUntil(crt.getValidityNotAfter());
                    // acceptor - peer

                    String identity = verifyChain(chain);

                    this.targetName = new GlobusGSSName(identity);

                    // initiator 
                    if (this.anonymity) {
                        this.sourceName = new GlobusGSSName();
                    } else {
                        X509Certificate cert = this.ctxCred.getCertificateChain()[0];
                        setGoodUntil(cert.getNotAfter());
                        this.sourceName = this.ctxCred.getName();
                    }
                    
                    // mutual authentication test
                    if (this.expectedTargetName != null &&
                        !this.expectedTargetName.equals(this.targetName)) {
                        throw new GlobusGSSException(GSSException.UNAUTHORIZED,
                                                     GlobusGSSException.BAD_NAME,
                                                     "authFailed00",
                                                     new Object[] {this.expectedTargetName,
                                                                   this.targetName});
                    }

                    if (this.gssMode == GSIConstants.MODE_GSI) {
                        this.state = CLIENT_START_DEL;
                        // if there is data to return then
                        // break. otherwise we fall through!!!
                        if (this.out.size() > 0) {
                            break;
                        } 
                    } else {
                        setDone();
                        break;
                    }

                } else {
                    break;
                }

            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }

        case CLIENT_START_DEL:
            
            // sanity check - might be invalid state
            if (this.state != CLIENT_START_DEL || this.out.size() > 0) {
                throw new GSSException(GSSException.FAILURE);
            }

            try {
                if (getCredDelegState()) {
                    this.conn.getOutStream().write(GSIConstants.DELEGATION_CHAR);
                    this.state = CLIENT_END_DEL;
                } else {
                    this.conn.getOutStream().write('0');
                    setDone();
                }
            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }

            break;

        case CLIENT_END_DEL:

            try {
                if (this.in.available() <= 0) {
                    return null;
                }
                
                X509Certificate [] chain = this.ctxCred.getCertificateChain();

                X509Certificate cert = 
                    this.certFactory.createCertificate(this.conn.getInStream(),
                                                       chain[0],
                                                       this.ctxCred.getPrivateKey(),
                                                       -1,
                                                       getDelegationType(chain[0]));

                byte [] enc = cert.getEncoded();
                this.conn.getOutStream().write(enc, 0, enc.length);
                setDone();
            } catch (GeneralSecurityException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }

            break;

        default:
            throw new GSSException(GSSException.FAILURE);
        }

        // TODO: we could also add a check if this.in is empty to make sure
        // we processed all data.

        logger.debug("exit initSecContext");
        return (this.out.size() > 0 || this.state == CLIENT_START_DEL) ? 
            this.out.toByteArray() : null;
    }

    private void setDone() {
        this.established = true;
    }

    private void setGoodUntil(Date date) {
        if (this.goodUntil == null) {
            this.goodUntil = date;
        } else if (date.before(this.goodUntil)) {
            this.goodUntil = date;
        }
    }

    private void init(int how) 
        throws GSSException {

        short [] cs;
        if (this.encryption) {
            // always make sure to add NULL cipher at the end
            short [] ciphers = this.policy.getCipherSuites();
            short [] newCiphers = new short[ciphers.length + 1];
            System.arraycopy(ciphers, 0, newCiphers, 0, ciphers.length);
            newCiphers[ciphers.length] = SSLPolicyInt.TLS_RSA_WITH_NULL_MD5;
            cs = newCiphers;
        } else {
            // encryption not requested - accept only one cipher
            // XXX: in the future might want to iterate through 
            // all cipher and enable only the null encryption ones
            cs = NO_ENCRYPTION;
        }
        this.policy.setCipherSuites(cs);
        this.policy.requireClientAuth(this.requireClientAuth.booleanValue());
        this.policy.setAcceptNoClientCert(this.acceptNoClientCerts.booleanValue());

        setTrustedCertificates();
        
        this.in = new TokenInputStream();
        this.out = new ByteArrayOutputStream();

        try {
            this.conn = new SSLConn(null, 
                                    this.in,
                                    this.out, 
                                    this.context, 
                                    how); 
        } catch (IOException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }       

        this.conn.init();
        this.certFactory = BouncyCastleCertProcessingFactory.getDefault();
        this.state = HANDSHAKE;
    }

    /* this is called when handshake is done */
    private void handshakeFinished()
        throws IOException {
        // this call just forces some internal library
        // variables to be initailized
        this.conn.finishHandshake();
        SSLCipherSuite cs = 
            SSLCipherSuite.findCipherSuite(this.conn.getCipherSuite());
        this.encryption = !cs.getCipherAlg().equals("NULL");
        logger.debug("encryption alg: " + cs.getCipherAlg()); 
    }
    
    // allows bypass of PureTLS checks - since they were
    // already performed during SSL hashshake
    static class GSSProxyPathValidator extends ProxyPathValidator {
        public void validate(X509Certificate [] certPath,
                             TrustedCertificates trustedCerts,
                             CertificateRevocationLists crlsList)
            throws ProxyPathValidatorException {
            super.validate(certPath, trustedCerts, crlsList);
        }
    }

    private String verifyChain(Vector peerCerts)
        throws GSSException {
        
        X509Certificate[] peerChain = null;
        try {
            peerChain = PureTLSUtil.certificateChainToArray(peerCerts);
        } catch (GeneralSecurityException e) {
            throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL,
                                         e);
        }

        GSSProxyPathValidator validator = new GSSProxyPathValidator();

        if (this.proxyPolicyHandlers != null) {
            Iterator iter = this.proxyPolicyHandlers.keySet().iterator();
            String oid;
            ProxyPolicyHandler handler;
            while(iter.hasNext()) {
                oid = (String)iter.next();
                handler = 
                    (ProxyPolicyHandler)this.proxyPolicyHandlers.get(oid);
                validator.setProxyPolicyHandler(oid, handler);
            }
        }

        CertificateRevocationLists certRevList =
            CertificateRevocationLists.getDefaultCertificateRevocationLists();

        validator.setRejectLimitedProxyCheck(
                  this.rejectLimitedProxy.booleanValue());

        try {
            validator.validate(peerChain, this.tc, certRevList);
        } catch (ProxyPathValidatorException e) {
            if (e.getErrorCode() == 
                ProxyPathValidatorException.LIMITED_PROXY_ERROR) {
                throw new GlobusGSSException(GSSException.UNAUTHORIZED, 
                                             e);
            } else {
                throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL,
                                             e);
            }
        }
        
        // C code also sets a flag RECEIVED_LIMITED_PROXY
        // when recevied certs is a limited proxy
        this.peerLimited = (validator.isLimited()) ? 
            Boolean.TRUE : Boolean.FALSE;
        
        return validator.getIdentity();
    }
    
    private void setCredential() 
        throws GSSException {
        try {
            this.context.setCredential(this.ctxCred.getGlobusCredential());
        } catch (GeneralSecurityException e) {
            throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL, e);
        }
    }

    private void setTrustedCertificates()
        throws GSSException {
        if (this.tc == null) {
            this.tc = PureTLSTrustedCertificates.getDefaultPureTLSTrustedCertificates();
        }
        if (this.tc == null) {
            throw new GlobusGSSException(GSSException.DEFECTIVE_CREDENTIAL,
                                         GlobusGSSException.UNKNOWN,
                                         "noCaCerts");
        }
        try {
            this.context.setRootList(this.tc.getX509CertList());
        } catch (GeneralSecurityException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }
    }

    /**
     * Wraps a message for integrity and protection.
     * Returns a GSI-wrapped token when privacy is not requested and
     * QOP requested is set to 
     * {@link GSSConstants#GSI_BIG GSSConstants.GSI_BIG}. Otherwise 
     * a regular SSL-wrapped token is returned.
     */
    public byte[] wrap(byte []inBuf, int off, int len, MessageProp prop) 
        throws GSSException {

        checkContext();

        logger.debug("enter wrap");

        byte [] token = null;
        boolean doGSIWrap = false;

        if (prop != null) {
            if (prop.getQOP() != 0 && prop.getQOP() != GSSConstants.GSI_BIG) {
                throw new GSSException(GSSException.BAD_QOP);
            }
            doGSIWrap = (!prop.getPrivacy() && prop.getQOP() == GSSConstants.GSI_BIG);
        }
        
        if (doGSIWrap) {
            
            byte [] mic = getMIC(inBuf, off, len, null);

            byte [] wtoken = new byte[5 + len + mic.length];
            wtoken[0] = GSI_WRAP;
            wtoken[1] = 3;
            wtoken[2] = 0;
            wtoken[3] = (byte)(mic.length >>> 8);
            wtoken[4] = (byte)(mic.length >>> 0);
            System.arraycopy(mic, 0, wtoken, 5, mic.length);
            System.arraycopy(inBuf, off, wtoken, 5+mic.length, len);

            token = wtoken;
        } else {
            token = wrap(inBuf, off, len);

            if (prop != null) {
                prop.setPrivacy(this.encryption);
                prop.setQOP(0);
            }
        }
        
        logger.debug("exit wrap");
        return token;
    }
    
    private byte[] wrap(byte[] inBuf, int off, int len) 
        throws GSSException {
        this.out.reset();
        try {
            this.conn.getOutStream().write(inBuf, off, len);
        } catch (IOException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }
        return this.out.toByteArray();
    }
    
    /**
     * Unwraps a token generated by <code>wrap</code> method on the other side of the context.
     * The input token can either be a regular SSL-wrapped token or GSI-wrapped token.
     * Upon return from the method the <code>MessageProp</code> object will contain
     * the applied QOP and privacy state of the message. In case of GSI-wrapped token 
     * the applied QOP will be set to 
     * {@link GSSConstants#GSI_BIG GSSConstants.GSI_BIG}
     */
    public byte[] unwrap(byte []inBuf, int off, int len, MessageProp prop) 
        throws GSSException {

        checkContext();

        logger.debug("enter unwrap");

        byte [] token = null;

        /*
         * see if the token is a straight SSL packet or
         * one of ours made by wrap using get_mic
         */
        if (inBuf[off] == GSI_WRAP &&
            inBuf[off+1] == 3 && 
            inBuf[off+2] == 0) {
            
            int micLen = SSLUtil.toShort(inBuf[off+3], inBuf[off+4]);
            int msgLen = len - 5 - micLen;

            if (micLen > len-5 || msgLen < 0) {
                throw new GSSException(GSSException.DEFECTIVE_TOKEN);
            } 
            
            verifyMIC(inBuf, off+5, micLen,
                      inBuf, off+5+micLen, msgLen, 
                      null);

            if (prop != null) {
                prop.setPrivacy(false);
                prop.setQOP(GSSConstants.GSI_BIG);
            }

            // extract the data
            token = new byte[msgLen];
            System.arraycopy(inBuf, off+5+micLen, token, 0, msgLen);
            
        } else {
            token = unwrap(inBuf, off, len);
            
            if (prop != null) {
                prop.setPrivacy(this.encryption);
                prop.setQOP(0);
            }
        }
        
        logger.debug("exit unwrap");
        return token;
    }
    
    private byte[] unwrap(byte[] inBuf, int off, int len) 
        throws GSSException {

        ByteArrayInputStream in =
            new ByteArrayInputStream(inBuf, off, len);
        ByteArrayOutputStream out =
            new ByteArrayOutputStream();

        // TODO: this might need to be rewritten
        // to catch lower level exceptions
        // e.g. mac too long, etc.
        try {
            while(in.available() > 0) {
                SSLRecord r = new SSLRecord(null);
                r.decode(this.conn, in);
                switch (r.getType().getValue()) {
                case SSLRecord.SSL_CT_APPLICATION_DATA:
                    out.write(r.getData().getValue());
                    break;
                case SSLRecord.SSL_CT_ALERT:
                    this.conn.getRecordReader().processAlert(r.getData().getValue());
                    break;
                default:
                    throw new Exception(i18n.getMessage("tokenFail03"));
                }
            }
        } catch (IOException e) {
            throw new GlobusGSSException(GSSException.BAD_MIC, e);
        } catch (Exception e) {
            throw new GlobusGSSException(GSSException.DEFECTIVE_TOKEN, e);
        }
        
        return out.toByteArray();
    }

    public void dispose() 
        throws GSSException {
        // doesn't do anything right now
        logger.debug("dipose");
    }

    public boolean isEstablished() {
        return this.established;
    }

    public void requestCredDeleg(boolean state) throws GSSException {
        this.credentialDelegation = state;
    }

    public boolean getCredDelegState() {
        return this.credentialDelegation;
    }
    
    public boolean isInitiator() 
        throws GSSException {
        if (this.role == UNDEFINED) {
            throw new GSSException(GSSException.FAILURE);
        }
        return (this.role == INITIATE);
    }

    public boolean isProtReady() {
        return isEstablished();
    }

    public void requestLifetime(int lifetime) 
        throws GSSException {
        if (lifetime == GSSContext.INDEFINITE_LIFETIME) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.UNKNOWN,
                                         "badLifetime00");
        }

        if (lifetime != GSSContext.DEFAULT_LIFETIME) {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, lifetime);
            setGoodUntil(calendar.getTime());
        }
    }

    public int getLifetime() {
        if (this.goodUntil != null) {
            return (int)((this.goodUntil.getTime() - System.currentTimeMillis())/1000);
        } else {
            return -1;
        }
    }

    public Oid getMech() throws GSSException {
        return GSSConstants.MECH_OID;
    }

    public GSSCredential getDelegCred() throws GSSException {
        return this.delegCred;
    }

    public void requestConf(boolean state) 
        throws GSSException {
        // enabled encryption
        this.encryption = state;
    }

    public boolean getConfState() {
        return this.encryption;
    }

    /**
     * Returns a cryptographic MIC (message integrity check)
     * of a specified message.
     */
    public byte[] getMIC(byte [] inBuf, 
                         int off,
                         int len,
                         MessageProp prop) 
        throws GSSException {

        checkContext();

        logger.debug("enter getMic");

        if (prop != null && (prop.getQOP() != 0 || prop.getPrivacy())) {
            throw new GSSException(GSSException.BAD_QOP);
        }

        SSLCipherState st = this.conn.getWriteCipherState();
        SSLCipherSuite cs = st.getCipherSuite();
        long sequence = this.conn.getWriteSequence();

        byte [] mic = new byte[GSI_MESSAGE_DIGEST_PADDING + cs.getDigestOutputLength()];
        
        System.arraycopy(Util.toBytes(sequence), 0, mic, 0, GSI_SEQUENCE_SIZE);
        System.arraycopy(Util.toBytes(len, 4), 0, mic, GSI_SEQUENCE_SIZE, 4);

        this.conn.incrementWriteSequence();

        int pad_ct = (cs.getDigestOutputLength()==16) ? 48 : 40;
        
        try {
            MessageDigest md = 
                MessageDigest.getInstance(cs.getDigestAlg());
        
            md.update(st.getMacKey());
            for(int i=0;i<pad_ct;i++) {
                md.update(SSLHandshake.pad_1);
            }
            md.update(mic, 0, GSI_MESSAGE_DIGEST_PADDING);
            md.update(inBuf, off, len);

            byte[] digest = md.digest();

            System.arraycopy(digest, 0, mic, GSI_MESSAGE_DIGEST_PADDING, digest.length);
        } catch (NoSuchAlgorithmException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }
        
        if (prop != null) {
            prop.setPrivacy(false);
            prop.setQOP(0);
        }
        
        logger.debug("exit getMic");
        return mic;
    }
    
    /**
     * Verifies a cryptographic MIC (message integrity check)
     * of a specified message.
     */
    public void verifyMIC(byte[] inTok, int tokOff, int tokLen, // mic
                          byte[] inMsg, int msgOff, int msgLen, // real msg
                          MessageProp prop) 
        throws GSSException {

        checkContext();

        logger.debug("enter verifyMic");

        SSLCipherState st = this.conn.getReadCipherState();
        SSLCipherSuite cs = st.getCipherSuite();

        logger.debug("digest algorithm: " + cs.getDigestAlg());

        if (tokLen != (GSI_MESSAGE_DIGEST_PADDING + cs.getDigestOutputLength())) {
            throw new GlobusGSSException(GSSException.DEFECTIVE_TOKEN,
                                         GlobusGSSException.TOKEN_FAIL,
                                         "tokenFail00",
                                         new Object[] {new Integer(tokLen), 
                                                       new Integer(GSI_MESSAGE_DIGEST_PADDING + 
                                                                   cs.getDigestOutputLength())});
        }
        
        int bufLen = SSLUtil.toInt(inTok, tokOff+GSI_SEQUENCE_SIZE);
        if (bufLen != msgLen) {
            throw new GlobusGSSException(GSSException.DEFECTIVE_TOKEN, 
                                         GlobusGSSException.TOKEN_FAIL,
                                         "tokenFail01",
                                         new Object[] {new Integer(msgLen), new Integer(bufLen)});
        }
        
        int pad_ct = (cs.getDigestOutputLength()==16) ? 48 : 40;

        byte [] digest = null;
        
        try {
            MessageDigest md = 
                MessageDigest.getInstance(cs.getDigestAlg());
            
            md.update(st.getMacKey());
            for(int i=0;i<pad_ct;i++) {
                md.update(SSLHandshake.pad_1);
            }
            md.update(inTok, tokOff, GSI_MESSAGE_DIGEST_PADDING);
            md.update(inMsg, msgOff, msgLen);

            digest = md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }
        
        byte [] token = new byte[tokLen-GSI_MESSAGE_DIGEST_PADDING];
        System.arraycopy(inTok, tokOff+GSI_MESSAGE_DIGEST_PADDING, token, 0, token.length);

        if (!Arrays.equals(digest, token)) {
            throw new GlobusGSSException(GSSException.BAD_MIC, 
                                         GlobusGSSException.BAD_MIC,
                                         "tokenFail02");
        }
        
        long tokSeq = SSLUtil.toLong(inTok, tokOff);
        long readSeq = this.conn.getReadSequence();
        long seqTest = tokSeq - readSeq;

        logger.debug("Token seq#   : " + tokSeq);
        logger.debug("Current seq# : " + readSeq);
        
        if (seqTest > 0) {
            // gap token
            throw new GSSException(GSSException.GAP_TOKEN);
        } else if (seqTest < 0) {
            // old token
            throw new GSSException(GSSException.OLD_TOKEN);
        } else {
            this.conn.incrementReadSequence();
        }

        if (prop != null) {
            prop.setPrivacy(false);
            prop.setQOP(0);
        }
        
        logger.debug("exit verifyMic");
    }


    /**
     * It works just like {@link #initSecContext(byte[], int, int) initSecContext} method.
     * It reads one SSL token from input stream, calls 
     * {@link #initSecContext(byte[], int, int) acceptSecContext} method and
     * writes the output token to the output stream (if any)
     * SSL token is not read on the initial call.
     */
    public int initSecContext(InputStream in, OutputStream out)
        throws GSSException {
        byte [] inToken = null;
        try {
            if (this.conn == null) {
                inToken = new byte[0];
            } else {
                inToken = SSLUtil.readSslMessage(in);
            }
            byte [] outToken = initSecContext(inToken, 0, inToken.length);
            if (outToken != null) {
                out.write(outToken);
                return outToken.length;
            } else {
                return 0;
            }
        } catch (IOException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }
    }

    /**
     * It works just like {@link #acceptSecContext(byte[], int, int) acceptSecContext}
     * method. It reads one SSL token from input stream, calls 
     * {@link #acceptSecContext(byte[], int, int) acceptSecContext}
     * method and writes the output token to the output stream (if any)
     */
    public void acceptSecContext(InputStream in, OutputStream out)
        throws GSSException {
        try {
            byte [] inToken = SSLUtil.readSslMessage(in);
            byte [] outToken = acceptSecContext(inToken, 0, inToken.length);
            if (outToken != null) {
                out.write(outToken);
            }
        } catch (IOException e) {
            throw new GlobusGSSException(GSSException.FAILURE, e);
        }
    }
    
    public GSSName getSrcName() throws GSSException {
        return this.sourceName;
    }
    
    public GSSName getTargName() throws GSSException {
        return this.targetName;
    }

    public void requestInteg(boolean state) 
        throws GSSException {
        if (!state) {
            throw new GlobusGSSException(GSSException.FAILURE, 
                                         GlobusGSSException.BAD_OPTION, 
                                         "integOn");
        }
    }
    
    public boolean getIntegState() {
        return true; // it is always on with ssl
    }

    public void requestSequenceDet(boolean state) 
        throws GSSException {
        if (!state) {
            throw new GlobusGSSException(GSSException.FAILURE, 
                                         GlobusGSSException.BAD_OPTION,
                                         "seqDet");
        }
    }
    
    public boolean getSequenceDetState() {
        return true; // it is always on with ssl
    }

    public void requestReplayDet(boolean state) 
        throws GSSException {
        if (!state) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION, 
                                         "replayDet");
        }
    }

    public boolean getReplayDetState() {
        return true; // is is always on with ssl
    }

    public void requestAnonymity(boolean state) 
        throws GSSException {
        this.anonymity = state;
    }

    public boolean getAnonymityState() {
        return this.anonymity;
    }

    public void requestMutualAuth(boolean state) 
        throws GSSException {
        if (!state) {
            throw new GlobusGSSException(GSSException.FAILURE, 
                                         GlobusGSSException.BAD_OPTION, 
                                         "mutualAuthOn");
        }
    }
    
    public boolean getMutualAuthState() {
        return true; // always on with gsi i guess
    }

    protected byte[] generateCertRequest(X509Certificate cert) 
        throws GeneralSecurityException {

        int bits = 
            ((RSAPublicKey)cert.getPublicKey()).getModulus().bitLength();

        this.keyPair = keyPairCache.getKeyPair(bits);
        
        return this.certFactory.createCertificateRequest(cert, this.keyPair);
    }

    protected void verifyDelegatedCert(X509Certificate certificate)
        throws GeneralSecurityException {
        RSAPublicKey pubKey = (RSAPublicKey)certificate.getPublicKey();
        RSAPrivateKey privKey = (RSAPrivateKey)this.keyPair.getPrivate();
                
        if (!pubKey.getModulus().equals(privKey.getModulus())) {
            throw new GeneralSecurityException(i18n.getMessage("keyMismatch"));
        }
    }

    protected void checkContext() 
        throws GSSException {
        if (this.conn == null || !isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }
        
        if (this.checkContextExpiration.booleanValue() && getLifetime() <= 0) {
            throw new GSSException(GSSException.CONTEXT_EXPIRED);
        }
    }

    protected int getDelegationType(X509Certificate issuer) 
        throws GeneralSecurityException, GSSException {

        int certType = BouncyCastleUtil.getCertificateType(issuer, this.tc);
        int dType = this.delegationType.intValue();

        if (logger.isDebugEnabled()) {
            logger.debug("Issuer type: " + certType + " delg. type requested: " + dType);
        }

        if (certType == GSIConstants.EEC) {
            if (dType == GSIConstants.DELEGATION_LIMITED) {
                if (CertUtil.isGsi2Enabled()) {
                    return GSIConstants.GSI_2_LIMITED_PROXY;
                } else if (CertUtil.isGsi3Enabled()) {
                    return GSIConstants.GSI_3_LIMITED_PROXY;
                } else {
                    return GSIConstants.GSI_4_LIMITED_PROXY;
                }
            } else if (dType == GSIConstants.DELEGATION_FULL) {
                if (CertUtil.isGsi2Enabled()) {
                    return GSIConstants.GSI_2_PROXY;
                } else if (CertUtil.isGsi3Enabled()) {
                    return GSIConstants.GSI_3_IMPERSONATION_PROXY;
                } else {
                    return GSIConstants.GSI_4_IMPERSONATION_PROXY;
                }
            } else if (CertUtil.isProxy(dType)) {
                return dType;
            }
        } else if (CertUtil.isGsi2Proxy(certType)) {
            if (dType == GSIConstants.DELEGATION_LIMITED) {
                return GSIConstants.GSI_2_LIMITED_PROXY;
            } else if (dType == GSIConstants.DELEGATION_FULL) {
                return GSIConstants.GSI_2_PROXY;
            } else if (CertUtil.isGsi2Proxy(dType)) {
                return dType;
            }
        } else if (CertUtil.isGsi3Proxy(certType)) {
            if (dType == GSIConstants.DELEGATION_LIMITED) {
                return GSIConstants.GSI_3_LIMITED_PROXY;
            } else if (dType == GSIConstants.DELEGATION_FULL) {
                return GSIConstants.GSI_3_IMPERSONATION_PROXY;
            } else if (CertUtil.isGsi3Proxy(dType)) {
                return dType;
            }
        } else if (CertUtil.isGsi4Proxy(certType)) {
            if (dType == GSIConstants.DELEGATION_LIMITED) {
                return GSIConstants.GSI_4_LIMITED_PROXY;
            } else if (dType == GSIConstants.DELEGATION_FULL) {
                return GSIConstants.GSI_4_IMPERSONATION_PROXY;
            } else if (CertUtil.isGsi4Proxy(dType)) {
                return dType;
            }
        }
        throw new GSSException(GSSException.FAILURE);
    }


    // -----------------------------------

    protected void setGssMode(Object value) 
        throws GSSException {
        if (!(value instanceof Integer)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object [] {"GSS mode", Integer.class});
        }
        Integer v = (Integer)value;
        if (v == GSIConstants.MODE_GSI || 
            v == GSIConstants.MODE_SSL) {
            this.gssMode = v;
        } else {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION,
                                         "badGssMode");
        }
    }

    protected void setDelegationType(Object value) 
        throws GSSException {
        if (!(value instanceof Integer)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"delegation type",  Integer.class});
        }
        Integer v = (Integer)value;
        if (v == GSIConstants.DELEGATION_TYPE_FULL ||
            v == GSIConstants.DELEGATION_TYPE_LIMITED) {
            this.delegationType = v;
        } else {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION,
                                         "badDelegType");
        }
    }

    protected void setCheckContextExpired(Object value) 
        throws GSSException {
        if (!(value instanceof Boolean)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"check context expired", Boolean.class});
        }
        this.checkContextExpiration = (Boolean)value;
    }

    protected void setRejectLimitedProxy(Object value) 
        throws GSSException {
        if (!(value instanceof Boolean)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"reject limited proxy", Boolean.class});
        }
        this.rejectLimitedProxy = (Boolean)value;
    }

    protected void setRequireClientAuth(Object value) 
        throws GSSException {
        if (!(value instanceof Boolean)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"require client auth", Boolean.class});
        }
        this.requireClientAuth = (Boolean)value;
    }

    protected void setRequireAuthzWithDelegation(Object value) 
        throws GSSException {
        
        if (!(value instanceof Boolean)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"require authz with delehation", Boolean.class});
        }
        this.requireAuthzWithDelegation = (Boolean)value;
    }

    protected void setAcceptNoClientCerts(Object value)
        throws GSSException {
        if (!(value instanceof Boolean)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"accept no client certs", Boolean.class});
        }
        this.acceptNoClientCerts = (Boolean)value;
    }

    protected void setGrimPolicyHandler(Object value) 
        throws GSSException {
        if (!(value instanceof ProxyPolicyHandler)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"GRIM policy handler", 
                                                       ProxyPolicyHandler.class});
        }
        if (this.proxyPolicyHandlers == null) {
            this.proxyPolicyHandlers = new HashMap();
        }
        this.proxyPolicyHandlers.put("1.3.6.1.4.1.3536.1.1.1.7", value);
    }

    protected void setProxyPolicyHandlers(Object value) 
        throws GSSException {
        if (!(value instanceof Map)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                        new Object[] {"Proxy policy handlers", 
                                                      Map.class});
        }
        this.proxyPolicyHandlers = (Map)value;
    }

    protected void setTrustedCertificates(Object value) 
        throws GSSException {
        if (!(value instanceof TrustedCertificates)) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_OPTION_TYPE,
                                         "badType",
                                         new Object[] {"Trusted certificates", 
                                                       TrustedCertificates.class});
        }
        this.tc = (TrustedCertificates)value;
    }
    
    public void setOption(Oid option, Object value)
        throws GSSException {
        if (option == null) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_ARGUMENT,
                                         "nullOption");
        }
        if (value == null) {
            throw new GlobusGSSException(GSSException.FAILURE,
                                         GlobusGSSException.BAD_ARGUMENT,
                                         "nullOptionValue");
        }
        
        if (option.equals(GSSConstants.GSS_MODE)) {
            setGssMode(value);
        } else if (option.equals(GSSConstants.DELEGATION_TYPE)) {
            setDelegationType(value);
        } else if (option.equals(GSSConstants.CHECK_CONTEXT_EXPIRATION)) {
            setCheckContextExpired(value);
        } else if (option.equals(GSSConstants.REJECT_LIMITED_PROXY)) {
            setRejectLimitedProxy(value);
        } else if (option.equals(GSSConstants.REQUIRE_CLIENT_AUTH)) {
            setRequireClientAuth(value);
        } else if (option.equals(GSSConstants.GRIM_POLICY_HANDLER)) {
            setGrimPolicyHandler(value);
        } else if (option.equals(GSSConstants.TRUSTED_CERTIFICATES)) {
            setTrustedCertificates(value);
        } else if (option.equals(GSSConstants.PROXY_POLICY_HANDLERS)) {
            setProxyPolicyHandlers(value);
        } else if (option.equals(GSSConstants.ACCEPT_NO_CLIENT_CERTS)) {
            setAcceptNoClientCerts(value);
        } else if (option.equals(GSSConstants
                                 .AUTHZ_REQUIRED_WITH_DELEGATION)) {
            setRequireAuthzWithDelegation(value);
        } else {
            throw new GlobusGSSException(GSSException.FAILURE, 
                                         GlobusGSSException.UNKNOWN_OPTION,
                                         "unknownOption",
                                         new Object[] {option});
        }
    }
    
    public Object getOption(Oid option) 
        throws GSSException {
        if (option == null) {
            throw new GlobusGSSException(GSSException.FAILURE, 
                                         GlobusGSSException.BAD_ARGUMENT,
                                         "nullOption");
        }
        
        if (option.equals(GSSConstants.GSS_MODE)) {
            return this.gssMode;
        } else if (option.equals(GSSConstants.DELEGATION_TYPE)) {
            return this.delegationType;
        } else if (option.equals(GSSConstants.CHECK_CONTEXT_EXPIRATION)) {
            return this.checkContextExpiration;
        } else if (option.equals(GSSConstants.REJECT_LIMITED_PROXY)) {
            return this.rejectLimitedProxy;
        } else if (option.equals(GSSConstants.REQUIRE_CLIENT_AUTH)) {
            return this.requireClientAuth;
        } else if (option.equals(GSSConstants.TRUSTED_CERTIFICATES)) {
            return this.tc;
        } else if (option.equals(GSSConstants.PROXY_POLICY_HANDLERS)) {
            return this.proxyPolicyHandlers;
        } else if (option.equals(GSSConstants.ACCEPT_NO_CLIENT_CERTS)) {
            return this.acceptNoClientCerts;
        }
        
        return null;
    }

    /**
     * Initiate the delegation of a credential.
     *
     * This function drives the initiating side of the credential
     * delegation process. It is expected to be called in tandem with the
     * {@link #acceptDelegation(int, byte[], int, int) acceptDelegation}
     * function.
     * <BR>
     * The behavior of this function can be modified by 
     * {@link GSSConstants#DELEGATION_TYPE GSSConstants.DELEGATION_TYPE} 
     * and 
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE} context
     * options. 
     * The {@link GSSConstants#DELEGATION_TYPE GSSConstants.DELEGATION_TYPE}
     * option controls delegation type to be performed. The
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE} 
     * option if set to 
     * {@link GSIConstants#MODE_SSL GSIConstants.MODE_SSL}
     * results in tokens that are not wrapped.
     * 
     * @param credential
     *        The credential to be delegated. May be null
     *        in which case the credential associated with the security
     *        context is used.
     * @param mechanism
     *        The desired security mechanism. May be null.
     * @param lifetime
     *        The requested period of validity (seconds) of the delegated
     *        credential. 
     * @return A token that should be passed to <code>acceptDelegation</code> if 
     *         <code>isDelegationFinished</code> returns false. May be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public byte[] initDelegation(GSSCredential credential, 
                                 Oid mechanism,
                                 int lifetime,
                                 byte[] buf, int off, int len) 
        throws GSSException {

        logger.debug("Enter initDelegation: " + delegationState);

        if (mechanism != null && !mechanism.equals(getMech())) {
            throw new GSSException(GSSException.BAD_MECH);
        }

        if (this.gssMode != GSIConstants.MODE_SSL && buf != null && len > 0) {
            buf = unwrap(buf, off, len);
            off = 0;
            len = buf.length;
        }
        
        byte [] token = null;

        switch (delegationState) {

        case DELEGATION_START:

            this.delegationFinished = false;
            token = DELEGATION_TOKEN;
            this.delegationState = DELEGATION_SIGN_CERT;
            break;

        case DELEGATION_SIGN_CERT:

            ByteArrayInputStream inData
                = new ByteArrayInputStream(buf, off, len);
            
            if (credential == null) {
                // get default credential
                GSSManager manager = new GlobusGSSManagerImpl();
                credential = manager.createCredential(GSSCredential.INITIATE_AND_ACCEPT);
            }

            if (!(credential instanceof GlobusGSSCredentialImpl)) {
                throw new GSSException(GSSException.DEFECTIVE_CREDENTIAL);
            }

            GlobusCredential cred = 
                ((GlobusGSSCredentialImpl)credential).getGlobusCredential();

            X509Certificate [] chain = cred.getCertificateChain();
            
            int time = (lifetime == GSSCredential.DEFAULT_LIFETIME) ? -1 : lifetime;
            
            try {
                X509Certificate cert = 
                    this.certFactory.createCertificate(inData,
                                                       chain[0],
                                                       cred.getPrivateKey(),
                                                       time,
                                                       getDelegationType(chain[0]));
                
                ByteArrayOutputStream out 
                    = new ByteArrayOutputStream();

                out.write(cert.getEncoded());
                for (int i=0;i<chain.length;i++) {
                    out.write(chain[i].getEncoded());
                }

                token = out.toByteArray();
            } catch (Exception e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }
            
            this.delegationState = DELEGATION_START;
            this.delegationFinished = true;
            break;

        default:
            throw new GSSException(GSSException.FAILURE);
        }

        logger.debug("Exit initDelegation");
        
        if (this.gssMode != GSIConstants.MODE_SSL && token != null) {
            return wrap(token, 0, token.length);
        } else {
            return token;
        }
    }

    /**
     * Accept a delegated credential.
     *
     * This function drives the accepting side of the credential
     * delegation process. It is expected to be called in tandem with the
     * {@link #initDelegation(GSSCredential, Oid, int, byte[], int, int) 
     * initDelegation} function.
     * <BR>
     * The behavior of this function can be modified by 
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE} context
     * option. The
     * {@link GSSConstants#GSS_MODE GSSConstants.GSS_MODE} 
     * option if set to 
     * {@link GSIConstants#MODE_SSL GSIConstants.MODE_SSL}
     * results in tokens that are not wrapped.
     *
     * @param lifetime
     *        The requested period of validity (seconds) of the delegated
     *        credential. 
     * @return A token that should be passed to <code>initDelegation</code> if 
     *        <code>isDelegationFinished</code> returns false. May be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public byte[] acceptDelegation(int lifetime,
                                   byte[] buf, int off, int len)
        throws GSSException {

        logger.debug("Enter acceptDelegation: " + delegationState);
        
        if (this.gssMode != GSIConstants.MODE_SSL && buf != null && len > 0) {
            buf = unwrap(buf, off, len);
            off = 0;
            len = buf.length;
        }

        byte [] token = null;

        switch (delegationState) {

        case DELEGATION_START:

            this.delegationFinished = false;

            if (len != 1 && buf[off] != GSIConstants.DELEGATION_CHAR) {
                throw new GlobusGSSException(GSSException.FAILURE,
                                             GlobusGSSException.DELEGATION_ERROR,
                                             "delegError00",
                                             new Object[] {new Character((char)buf[off])});
            }
            
            try {
                Vector certChain = this.conn.getCertificateChain();
                if (certChain == null || certChain.size() == 0) {
                    throw new GlobusGSSException(GSSException.FAILURE, 
                                                 GlobusGSSException.DELEGATION_ERROR,
                                                 "noClientCert");
                }
            
                X509Certificate tmpCert = 
                    PureTLSUtil.convertCert((X509Cert)certChain.lastElement());

                token = generateCertRequest(tmpCert);
            } catch (GeneralSecurityException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            } catch (IOException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }

            this.delegationState = DELEGATION_COMPLETE_CRED;
            break;
            
        case DELEGATION_COMPLETE_CRED:

            ByteArrayInputStream in = 
                new ByteArrayInputStream(buf, off, len);

            X509Certificate [] chain = null;
            LinkedList certList = new LinkedList();
            X509Certificate cert = null;
            try {
                while(in.available() > 0) {
                    cert = CertUtil.loadCertificate(in);
                    certList.add(cert);
                }

                chain = new X509Certificate[certList.size()];
                chain = (X509Certificate[])certList.toArray(chain);

                verifyDelegatedCert(chain[0]);

            } catch (GeneralSecurityException e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }

            GlobusCredential proxy = 
                new GlobusCredential(this.keyPair.getPrivate(), chain);

            this.delegatedCred = 
                new GlobusGSSCredentialImpl(proxy, 
                                            GSSCredential.INITIATE_AND_ACCEPT);

            this.delegationState = DELEGATION_START;
            this.delegationFinished = true;
            break;

        default:
            throw new GSSException(GSSException.FAILURE);
        }

        logger.debug("Exit initDelegation");

        if (this.gssMode != GSIConstants.MODE_SSL && token != null) {
            return wrap(token, 0, token.length);
        } else {
            return token;
        }
    }

    public GSSCredential getDelegatedCredential() {
        return this.delegatedCred;
    }
    
    public boolean isDelegationFinished() {
        return this.delegationFinished;
    }

    /**
     * Retrieves arbitrary data about this context.
     * Currently supported oid: <UL>
     * <LI>
     * {@link GSSConstants#X509_CERT_CHAIN GSSConstants.X509_CERT_CHAIN}
     * returns certificate chain of the peer (<code>X509Certificate[]</code>).
     * </LI>
     * </UL>
     *
     * @param oid the oid of the information desired.
     * @return the information desired. Might be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public Object inquireByOid(Oid oid) 
        throws GSSException {
        if (oid == null) {
            throw new GlobusGSSException(GSSException.FAILURE, 
                                         GlobusGSSException.BAD_ARGUMENT,
                                         "nullOption");
        }
        
        if (oid.equals(GSSConstants.X509_CERT_CHAIN)) {
            if (isEstablished()) {
                // converting certs is slower but keeping coverted certs
                // takes lots of memory.
                try {
                    Vector peerCerts = this.conn.getCertificateChain();
                    if (peerCerts != null && peerCerts.size() > 0) {
                        return PureTLSUtil.certificateChainToArray(peerCerts);
                    } else {
                        return null;
                    }
                } catch (Exception e) {
                    throw new GlobusGSSException(
                             GSSException.DEFECTIVE_CREDENTIAL,
                             e
                    );
                }
            }
        } else if (oid.equals(GSSConstants.RECEIVED_LIMITED_PROXY)) {
            return this.peerLimited;
        }
        
        return null;
    }


    // ==================================================================
    // Not implemented below
    // ==================================================================
    
    /**
     * Currently not implemented.
     */
    public int getWrapSizeLimit(int qop, boolean confReq,
                                int maxTokenSize) 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public void wrap(InputStream inStream, OutputStream outStream,
                     MessageProp msgProp) 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public void unwrap(InputStream inStream, OutputStream outStream,
                       MessageProp msgProp) 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public void getMIC(InputStream inStream, OutputStream outStream,
                       MessageProp msgProp) 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }
 
    /**
     * Currently not implemented.
     */
    public void verifyMIC(InputStream tokStream, InputStream msgStream,
                          MessageProp msgProp) 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public void setChannelBinding(ChannelBinding cb) 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }
 
    /**
     * Currently not implemented.
     */
    public boolean isTransferable() 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public byte [] export() 
        throws GSSException {
        throw new GSSException(GSSException.UNAVAILABLE);
    }
    
}
