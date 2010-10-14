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
package org.globus.myproxy;

import java.nio.charset.Charset;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Writer;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.EOFException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.net.Socket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.cert.X509Certificate;
import java.security.MessageDigest;
import java.security.GeneralSecurityException;

import java.security.Signature;

import javax.security.auth.x500.X500Principal;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.globus.common.CoGProperties;

import org.globus.util.Base64;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.CertUtil;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.IdentityAuthorization;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;

import org.gridforum.jgss.ExtendedGSSManager;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This class provides an API for communicating with MyProxy servers.
 * It provides main functions for retrieving, removing and
 * storing credentials on MyProxy server. It also provides functions
 * for getting credential information and changing passwords.
 * <P>
 * More information about MyProxy is available on the 
 * <a href="http://myproxy.ncsa.uiuc.edu/">MyProxy Home Page</a>.
 * <P>
 */
public class MyProxy  {

    static Log logger = 
        LogFactory.getLog(MyProxy.class.getName());

    public static final int MIN_PASSWORD_LENGTH = 
        MyProxyConstants.MIN_PASSWORD_LENGTH;
    
    public static final String MYPROXY_PROTOCOL_VERSION = 
        MyProxyConstants.MYPROXY_PROTOCOL_VERSION;
    
    private static final String RESPONSE   = "RESPONSE=";
    private static final String ERROR      = "ERROR=";
    private static final String AUTHZ_DATA = "AUTHORIZATION_DATA=";

    private static final String CRED            = "CRED_";
    private static final String OWNER           = "OWNER=";
    private static final String START_TIME      = "START_TIME=";
    private static final String END_TIME        = "END_TIME=";
    private static final String DESC            = "DESC=";
    private static final String RETRIEVER       = "RETRIEVER=";
    private static final String RENEWER         = "RENEWER=";
    private static final String TRUSTROOTS      = "TRUSTED_CERTS=";

    private static final String CRED_START_TIME = CRED + START_TIME;
    private static final String CRED_END_TIME   = CRED + END_TIME;
    private static final String CRED_OWNER      = CRED + OWNER;
    private static final String CRED_DESC       = CRED + DESC;
    private static final String CRED_RETRIEVER  = CRED + RETRIEVER;
    private static final String CRED_RENEWER    = CRED + RENEWER;
    private static final String CRED_NAME       = CRED + "NAME=";

    /** The default MyProxy server port (7512). */
    public static final int DEFAULT_PORT = 7512;

    /** The default key size (1024 bits). */
    public static final int DEFAULT_KEYBITS = 1024;

    /** The integer command number for the MyProxy 'Get' command (0). */
    public static final int GET_PROXY       = 0;
    /** The integer command number for the MyProxy 'Put' command (1). */
    public static final int PUT_PROXY       = 1;
    /** The integer command number for the MyProxy 'Info' command (2). */
    public static final int INFO_PROXY      = 2;
    /** The integer command number for the MyProxy 'Destroy' command (3). */
    public static final int DESTROY_PROXY   = 3;
    /** The integer command number for the MyProxy Password Change
     * command (4). */
    public static final int CHANGE_PASSWORD = 4;
    /** The integer command number for the MyProxy 'Store' command (5). */
    public static final int STORE_CREDENTIAL = 5;
    /** The integer command number for the MyProxy 'Retrieve' command (6). */
    public static final int RETRIEVE_CREDENTIAL = 6;
    /** The integer command number for the MyProxy 'Get Trustroots' command (7). */
    public static final int GET_TRUSTROOTS = 7;

    /** The hostname of the target MyProxy server. */
    protected String host;
    /** The port of the target MyProxy server (default 7512). */
    protected int port = DEFAULT_PORT;
    /** The authorization policy in effect for the target MyProxy server. */
    protected Authorization authorization;
    /** The GSSContext for communication with the MyProxy server. */
    protected GSSContext context;
    /** Trustroot information and path constant. */
    protected String[] trustrootFilenames;
    protected String[] trustrootData;
    private final static String TRUSTED_CERT_PATH = "/.globus/certificates";

    /**
     * Initialize the MyProxy client object with the default
     * authorization policy.
     */
    public MyProxy() {
        setAuthorization(new MyProxyServerAuthorization());
    }

    /**
     * Prepare to connect to the MyProxy server at the specified
     * host and port using the default authorization policy.
     *
     * @param host
     *        The hostname of the MyProxy server.
     * @param port
     *        The port number of the MyProxy server.
     */
    public MyProxy(String host, int port) {
        setHost(host);
        setPort(port);
        setAuthorization(new MyProxyServerAuthorization());
    }

    /**
     * Set MyProxy server hostname.
     * @param host
     *        The hostname of the MyProxy server.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Get MyProxy server hostname.
     * @return The hostname of the MyProxy server.
     */
    public String getHost() {
        return host;
    }

    /**
     * Set MyProxy server port.
     * @param port
     *        The port number of the MyProxy server.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Get MyProxy server port.
     * @return The port number of the MyProxy server.
     */
    public int getPort() {
        return port;
    }

    /**
     * Set MyProxy server authorization mechanism.
     * @param authorization
     *        The authorization mechanism for the MyProxy server.
     */
    public void setAuthorization(Authorization authorization) {
        this.authorization = authorization;
    }

    /**
     * Get MyProxy server authorization mechanism.
     * @return The authorization mechanism for the MyProxy server.
     */
    public Authorization getAuthorization() {
        return this.authorization;
    }
    
    private GssSocket getSocket(GSSCredential credential) 
        throws IOException, GSSException {
        GSSManager manager = ExtendedGSSManager.getInstance();
        
        this.context = manager.createContext(null, 
                                             GSSConstants.MECH_OID,
                                             credential,
                                             GSSContext.DEFAULT_LIFETIME);

        // no delegation
        this.context.requestCredDeleg(false);

        IOException exception = null;
        Socket socket = null;
        String goodAddr = "";
        int hostIdx = 0;
        String hosts[] = host.split(",");
        int socketTimeout = CoGProperties.getDefault().getSocketTimeout();

        search:
            while (hostIdx < hosts.length) {
                InetAddress addrs[] = InetAddress.getAllByName(hosts[hostIdx]);
                for (int addrIdx = 0; addrIdx < addrs.length; addrIdx++) {
                    exception = null;
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("getSocket(): Trying " + 
                                addrs[addrIdx].toString());
                        }
                        socket = new Socket();
                        socket.connect(
                            new InetSocketAddress(addrs[addrIdx],port), 
                            socketTimeout*1000); // seconds to milliseconds

                        goodAddr = addrs[addrIdx].toString();
                        if (logger.isDebugEnabled()) {
                            logger.debug("             Succeeded.");
                        }
                        break search;
                    } catch (IOException e) {
                        exception = e;
                        if (logger.isDebugEnabled()) {
                            logger.debug("             Failed.");
                        }
                    } 
                }
                hostIdx += 1;
            }

        if (exception != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("getSocket(): " +
                    "Unable to connect to a MyProxy host");
            }
            throw exception;
        }
        
        GssSocketFactory gssFactory = GssSocketFactory.getDefault();

        GssSocket gssSocket = 
            (GssSocket)gssFactory.createSocket(socket,
                                               hosts[hostIdx], 
                                               port, 
                                               this.context);

        if (logger.isDebugEnabled()) {
            logger.debug("getSocket(): Connected to " + goodAddr);
        }

        gssSocket.setAuthorization(this.authorization);

        return gssSocket;
    }

    /**
     * Delegate credentials to a MyProxy server.
     *
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username to store the credentials under.
     * @param  passphrase
     *         The passphrase to use to encrypt the stored
     *         credentials.
     * @param  lifetime
     *         The maximum lifetime of credentials delegated by the server
     *         (in seconds).
     * @exception MyProxyException
     *            If an error occurred during the operation.
     */
    public void put(GSSCredential credential,
                    String username,
                    String passphrase,
                    int lifetime) 
        throws MyProxyException {
        InitParams request = new InitParams();
        request.setUserName(username);
        request.setPassphrase(passphrase);
        request.setLifetime(lifetime);
        put(credential, request);
    }

    /**
     * Delegate credentials to a MyProxy server.
     *
     * @param  credential
     *         The GSI credentials to use.
     * @param  params
     *         The parameters for the put operation.
     * @exception MyProxyException
     *            If an error occurred during the operation.
     */
    public void put(GSSCredential credential,
                    InitParams params) 
        throws MyProxyException {

        if (credential == null) {
            throw new IllegalArgumentException("credential == null");
        }

        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }

        if (!(credential instanceof GlobusGSSCredentialImpl)) {
            throw new IllegalArgumentException("wrong type of credentials");
        }

        String msg = params.makeRequest();
         
        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream  in  = null;
        
        try {
            gsiSocket = getSocket(credential);
            
            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();
            
            // send message
            out.write(msg.getBytes());
            out.flush();
           
            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }
           
            handleReply(in);

            BouncyCastleCertProcessingFactory certFactory =
                BouncyCastleCertProcessingFactory.getDefault();

            GlobusGSSCredentialImpl pkiCred =
                (GlobusGSSCredentialImpl)credential;
            
            X509Certificate [] certs = pkiCred.getCertificateChain();

            // read in the cert request from socket and
            // generate a certificate to be sent back to the server
            X509Certificate cert = 
                certFactory.createCertificate(in,
                                              certs[0],
                                              pkiCred.getPrivateKey(),
                                              -1,
                                              GSIConstants.DELEGATION_FULL);
           
            // write the new cert we've generated to the socket to send it back
            // to the server
                
            // must put everything into one message
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(2048);
                
            buffer.write( (byte)(certs.length+1) );
                
            // write signed ceritifcate
            buffer.write(cert.getEncoded());
                
            for (int i=0;i<certs.length;i++) {
                buffer.write( certs[i].getEncoded() );
                    
                // DEBUG: print out subject name of sent certificate
                if (logger.isDebugEnabled()) {
                    logger.debug("Sent cert: " + certs[i].getSubjectDN());
                }
                    
            }
                
            out.write(buffer.toByteArray());
            out.flush();
                
            handleReply(in);
            
        } catch(Exception e) {
            throw new MyProxyException("MyProxy put failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }

    /**
     * Store credentials on a MyProxy server.
     * Copies certificate(s) and private key directly to the server rather
     * than delegating an X.509 proxy credential.
     *
     * @param  credential
     *         The local GSI credentials to use for authentication.
     * @param  certs
     *         The certificate(s) to store.
     * @param  key
     *         The private key to store (typically encrypted).
     * @param  params
     *         The parameters for the store operation.
     * @exception MyProxyException
     *            If an error occurred during the operation.
     */
    public void store(GSSCredential credential,
                      X509Certificate [] certs,
                      OpenSSLKey key,
                      StoreParams params) 
        throws MyProxyException {

        if (credential == null) {
            throw new IllegalArgumentException("credential == null");
        }

        if (certs == null) {
            throw new IllegalArgumentException("certs == null");
        }

        if (key == null) {
            throw new IllegalArgumentException("key == null");
        }

        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }

        if (!(credential instanceof GlobusGSSCredentialImpl)) {
            throw new IllegalArgumentException("wrong type of credentials");
        }

        String msg = params.makeRequest();
         
        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream  in  = null;
        
        try {
            gsiSocket = getSocket(credential);
            
            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();
            
            // send message
            out.write(msg.getBytes());
            out.flush();
           
            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }
           
            handleReply(in);

            CertUtil.writeCertificate(out, certs[0]);
            key.writeTo(out);
            for (int i=1;i<certs.length;i++) {
                // skip the self-signed certificates
                if (certs[i].getSubjectDN().equals(certs[i].getIssuerDN()))
                    continue;
                CertUtil.writeCertificate(out, certs[i]);
            }
            out.write('\0');
            out.flush();
                
            handleReply(in);
            
        } catch(Exception e) {
            throw new MyProxyException("MyProxy store failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }

    /**
     * Removes delegated credentials from the MyProxy server.
     *
     * @param  credential
     *         The local GSI credentials to use for authentication. 
     * @param  username
     *         The username of the credentials to remove.
     * @param  passphrase
     *         The passphrase of the credentials to remove.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public void destroy(GSSCredential credential,
                        String username,
                        String passphrase)
        throws MyProxyException {
        DestroyParams request = new DestroyParams();
        request.setUserName(username);
        request.setPassphrase(passphrase);
        destroy(credential, request);
    }

    /**
     * Removes delegated credentials from the MyProxy server.
     *
     * @param  credential
     *         The local GSI credentials to use for authentication. 
     * @param  params
     *         The parameters for the destroy operation.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public void destroy(GSSCredential credential,
                        DestroyParams params) 
        throws MyProxyException {
        
        if (credential == null) {
            throw new IllegalArgumentException("credential == null");
        }

        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }
        
        String msg = params.makeRequest();
        
        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream  in  = null;
        
        try {
            gsiSocket = getSocket(credential);
            
            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();
            
            // send message
            out.write(msg.getBytes());
            out.flush();
            
            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }
            
            handleReply(in);
            
        } catch(Exception e) {
            throw new MyProxyException("MyProxy destroy failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }

    /**
     * Changes the password of the credential on the
     * MyProxy server.
     *
     * @param  credential
     *         The local GSI credentials to use for authentication. 
     * @param  params
     *         The parameters for the change password operation.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public void changePassword(GSSCredential credential,
                               ChangePasswordParams params)
        throws MyProxyException {

        if (credential == null) {
            throw new IllegalArgumentException("credential == null");
        }

        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }

        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream  in  = null;
        
        String msg = params.makeRequest();

        try {
            gsiSocket = getSocket(credential);
            
            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();
            
            // send message
            out.write(msg.getBytes());
            out.flush();
            
            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }
            
            handleReply(in);
            
        } catch(Exception e) {
            throw new MyProxyException("MyProxy change password failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }


    /**
     * Retrieves credential information from MyProxy server.
     * Only the information of the default credential is returned
     * by this operation.
     *
     * @param  credential
     *         The local GSI credentials to use for authentication. 
     * @param  username
     *         The username of the credentials to remove.
     * @param  passphrase
     *         The passphrase of the credentials to remove.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     * @return The credential information of the default
     *         credential.
     */
    public CredentialInfo info(GSSCredential credential,
                               String username,
                               String passphrase)
        throws MyProxyException {
        InfoParams request = new InfoParams();
        request.setUserName(username);
        request.setPassphrase(passphrase);
        return info(credential, request)[0];
    }
    
    /**
     * Retrieves credential information from MyProxy server.
     *
     * @param  credential
     *         The local GSI credentials to use for authentication. 
     * @param  params
     *         The parameters for the info operation.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     * @return The array of credential information of all
     *         the user's credentials.
     */
    public CredentialInfo[] info(GSSCredential credential,
                                 InfoParams params)
        throws MyProxyException {
        
        if (credential == null) {
            throw new IllegalArgumentException("credential == null");
        }

        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }
        
        String msg = params.makeRequest();
        
        CredentialInfo [] creds = null;
        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream  in  = null;
        
        try {
            gsiSocket = getSocket(credential);
            
            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();
            
            // send message
            out.write(msg.getBytes());
            out.flush();
            
            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }
            
            InputStream reply = handleReply(in);
            String line = null;
            String value = null;
            Map credMap = new HashMap();
            CredentialInfo info = new CredentialInfo();
            while( (line = readLine(reply)) != null ) {
                if (line.startsWith(CRED_START_TIME)) {
                    value = line.substring(CRED_START_TIME.length());
                    info.setStartTime(Long.parseLong(value) * 1000);
                } else if (line.startsWith(CRED_END_TIME)) {
                    value = line.substring(CRED_END_TIME.length());
                    info.setEndTime(Long.parseLong(value) * 1000);
                } else if (line.startsWith(CRED_OWNER)) {
                    info.setOwner(line.substring(CRED_OWNER.length()));
                } else if (line.startsWith(CRED_NAME)) {
                    info.setName(line.substring(CRED_NAME.length()));
                } else if (line.startsWith(CRED_DESC)) {
                    info.setDescription(line.substring(CRED_DESC.length()));
                } else if (line.startsWith(CRED_RENEWER)) {
                    info.setRenewers(line.substring(CRED_RENEWER.length()));
                } else if (line.startsWith(CRED_RETRIEVER)) {
                    info.setRetrievers(line.substring(CRED_RETRIEVER.length()));
                } else if (line.startsWith(CRED)) {
                    int pos = line.indexOf('=', CRED.length());
                    if (pos == -1) {
                        continue;
                    }
                    value = line.substring(pos+1);

                    if (matches(line, pos+1, OWNER)) {
                        String name = getCredName(line, pos, OWNER);
                        getCredentialInfo(credMap, name).setOwner(value);
                    } else if (matches(line, pos+1, START_TIME)) {
                        String name = getCredName(line, pos, START_TIME);
                        getCredentialInfo(credMap, name).setStartTime(Long.parseLong(value) * 1000);
                    } else if (matches(line, pos+1, END_TIME)) {
                        String name = getCredName(line, pos, END_TIME);
                        getCredentialInfo(credMap, name).setEndTime(Long.parseLong(value) * 1000);
                    } else if (matches(line, pos+1, DESC)) {
                        String name = getCredName(line, pos, DESC);
                        getCredentialInfo(credMap, name).setDescription(value);
                    } else if (matches(line, pos+1, RENEWER)) {
                        String name = getCredName(line, pos, RENEWER);
                        getCredentialInfo(credMap, name).setRenewers(value);
                    } else if (matches(line, pos+1, RETRIEVER)) {
                        String name = getCredName(line, pos, RETRIEVER);
                        getCredentialInfo(credMap, name).setRetrievers(value);
                    }
                }
            }
            
            creds = new CredentialInfo[1 + credMap.size()];
            creds[0] = info; // defailt creds at position 0

            if (credMap.size() > 0) {
                int i = 1;
                Iterator iter = credMap.entrySet().iterator();
                while(iter.hasNext()) {
                    Map.Entry entry = (Map.Entry)iter.next();
                    creds[i++] = (CredentialInfo)entry.getValue();
                }
            }

            return creds;
        } catch(Exception e) {
            throw new MyProxyException("MyProxy info failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }
    
    private boolean matches(String line, int pos, String arg) {
        return line.regionMatches(true,
                                  pos - arg.length(),
                                  arg,
                                  0,
                                  arg.length());
    }

    private String getCredName(String line, int pos, String arg) {
        return line.substring(CRED.length(), pos-arg.length());
    }
        
    private CredentialInfo getCredentialInfo(Map map, String name) {
        CredentialInfo info = (CredentialInfo)map.get(name);
        if (info == null) {
            info = new CredentialInfo();
            info.setName(name);
            map.put(name, info);
        }
        return info;
    }

    /**
     * Retrieves delegated credentials from MyProxy server Anonymously
     * (without local credentials)
     *
     * Notes: Performs simple verification of private/public keys of
     *        the delegated credential. Should be improved later.
     *        And only checks for RSA keys.
     *
     * @param  username
     *         The username of the credentials to retrieve.
     * @param  passphrase
     *         The passphrase of the credentials to retrieve.
     * @param  lifetime
     *         The requested lifetime of the retrieved credential (in seconds).
     * @return GSSCredential 
     *         The retrieved delegated credentials.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public GSSCredential get(String username,
                             String passphrase,
                             int lifetime) 
        throws MyProxyException {
        return get(null, username, passphrase, lifetime);
    }

    /**
     * Retrieves delegated credentials from the MyProxy server.
     *
     * Notes: Performs simple verification of private/public keys of
     *        the delegated credential. Should be improved later.
     *        And only checks for RSA keys.
     *
     * @param  credential 
     *         The local GSI credentials to use for authentication.
     *         Can be set to null if no local credentials.
     * @param  username
     *         The username of the credentials to retrieve.
     * @param  passphrase
     *         The passphrase of the credentials to retrieve.
     * @param  lifetime
     *         The requested lifetime of the retrieved credential (in seconds).
     * @return GSSCredential 
     *         The retrieved delegated credentials.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public GSSCredential get(GSSCredential credential,
                             String username,
                             String passphrase,
                             int lifetime) 
        throws MyProxyException {
        GetParams request = new GetParams();
        request.setUserName(username);
        request.setPassphrase(passphrase);
        request.setLifetime(lifetime);
        return get(credential, request);
    }
    
    /**
     * Retrieves delegated credentials from the MyProxy server.
     *
     * @param  credential 
     *         The local GSI credentials to use for authentication.
     *         Can be set to null if no local credentials.
     * @param  params
     *         The parameters for the get operation.
     * @return GSSCredential 
     *         The retrieved delegated credentials.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public GSSCredential get(GSSCredential credential,
                             GetParams params) 
        throws MyProxyException {
         
        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }

        if (credential == null) {
            try {
                credential = getAnonymousCredential();
            } catch (GSSException e) {
                throw new MyProxyException("Failed to create anonymous credentials", e);
            }
        }

        String msg = params.makeRequest();

        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream in   = null;

        try {
            gsiSocket = getSocket(credential);

            if (credential.getName().isAnonymous()) {
                this.context.requestAnonymity(true);
            }

            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();

            // send message
            out.write(msg.getBytes());
            out.flush();

            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }

            // may require authz handshake
            handleReply(in, out, params.getAuthzCreds(),
                        params.getWantTrustroots());

            // start delegation - generate key pair
            KeyPair keyPair = CertUtil.generateKeyPair("RSA", DEFAULT_KEYBITS);

            BouncyCastleCertProcessingFactory certFactory =
                BouncyCastleCertProcessingFactory.getDefault();

            byte [] req = null;
            if (credential.getName().isAnonymous()) {
                req = certFactory.createCertificateRequest("CN=ignore",
                                                           keyPair);
            } else {
                GlobusGSSCredentialImpl pkiCred = 
                    (GlobusGSSCredentialImpl)credential;
                req = certFactory.createCertificateRequest(pkiCred.getCertificateChain()[0],
                                                           keyPair);
            }

            // send the request to server
            out.write(req);
            out.flush();
            
            // read the number of certificates
            int size = in.read();

            if (logger.isDebugEnabled()) {
                logger.debug("Reading " + size + " certs");
            }
            
            X509Certificate [] chain
                = new X509Certificate[size];
            
            for (int i=0;i<size;i++) {
                chain[i] = certFactory.loadCertificate(in);
                // DEBUG: display the cert names
                if (logger.isDebugEnabled()) {
                    logger.debug("Received cert: " + chain[i].getSubjectDN());
                }
            }

            // get the response
            handleReply(in);

            // make sure the private key belongs to the right public key
            // currently only works with RSA keys
            RSAPublicKey pkey   = (RSAPublicKey)chain[0].getPublicKey();
            RSAPrivateKey prkey = (RSAPrivateKey)keyPair.getPrivate();
            
            if (!pkey.getModulus().equals(prkey.getModulus())) {
                throw new MyProxyException("Private/Public key mismatch!");
            }
            
            GlobusCredential newCredential = null;
            
            newCredential = new GlobusCredential(keyPair.getPrivate(),
                                                 chain);
            
            return new GlobusGSSCredentialImpl(newCredential,
                                               GSSCredential.INITIATE_AND_ACCEPT);
            
        } catch(Exception e) {
            throw new MyProxyException("MyProxy get failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }

    /**
     * Retrieves trustroot information from the MyProxy server.
     *
     * @param  credential 
     *         The local GSI credentials to use for authentication.
     *         Can be set to null if no local credentials.
     * @param  params
     *         The parameters for the get-trustroots operation.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public void getTrustroots(GSSCredential credential,
                              GetTrustrootsParams params) 
        throws MyProxyException {

        if (params == null) {
            throw new IllegalArgumentException("params == null");
        }

        if (credential == null) {
            try {
                credential = getAnonymousCredential();
            } catch (GSSException e) {
                throw new MyProxyException("Failed to create anonymous credentials", e);
            }
        }

        Socket gsiSocket = null;
        OutputStream out = null;
        InputStream in   = null;

        String msg = params.makeRequest();

        try {
            gsiSocket = getSocket(credential);

            if (credential.getName().isAnonymous()) {
                this.context.requestAnonymity(true);
            }

            out = gsiSocket.getOutputStream();
            in  = gsiSocket.getInputStream();

            // send message
            out.write(msg.getBytes());
            out.flush();

            if (logger.isDebugEnabled()) {
                logger.debug("Req sent:" + params);
            }

            // get the response
            handleReply(in, out, null, true);

        } catch(Exception e) {
            throw new MyProxyException("MyProxy get-trustroots failed.", e);
        } finally {
            // close socket
            close(out, in, gsiSocket);
        }
    }

    /**
     * Bootstraps trustroot information from the MyProxy server.
     *
     * @exception MyProxyException
     *         If an error occurred during the operation.
     */
    public void bootstrapTrust() throws MyProxyException {
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            MyTrustManager myTrustManager = new MyTrustManager();
            TrustManager[] trustAllCerts = new TrustManager[] { myTrustManager };
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            SSLSocketFactory sf = sc.getSocketFactory();
            SSLSocket socket = (SSLSocket)sf.createSocket(this.host, this.port);
            socket.setEnabledProtocols(new String[] { "SSLv3" });
            socket.startHandshake();
            socket.close();

            X509Certificate[] acceptedIssuers = myTrustManager.getAcceptedIssuers();
            if (acceptedIssuers == null) {
                throw new MyProxyException("Failed to determine MyProxy server trust roots in bootstrapTrust.");
            }
            for (int idx = 0; idx < acceptedIssuers.length; idx++)
            {
                File x509Dir = new File(org.globus.myproxy.MyProxy.getTrustRootPath());
                if (!x509Dir.exists())
                {
                    StringBuffer newSubject = new StringBuffer();
                    String[] subjArr = acceptedIssuers[idx].getSubjectDN().getName().split(", ");
                    for(int i = (subjArr.length - 1); i > -1; i--)
                    {
                        newSubject.append("/");
                        newSubject.append(subjArr[i]);
                    }
                    String subject = newSubject.toString();

                    File tmpDir = new File(getTrustRootPath() + "-" +
                                           System.currentTimeMillis());
                    if (tmpDir.mkdir() == true)
                    {
                        String hash = opensslHash(acceptedIssuers[idx]);
                        String filename = tmpDir.getPath() + tmpDir.separator + hash + ".0";

                        FileOutputStream os = new FileOutputStream(new File(filename));
                        CertUtil.writeCertificate(os, acceptedIssuers[idx]);

                        os.close();
                        if (logger.isDebugEnabled()) {
                            logger.debug("wrote trusted certificate to " + filename);
                        }

                        filename = tmpDir.getPath() + tmpDir.separator + hash + ".signing_policy";

                        os = new FileOutputStream(new File(filename));
                        Writer wr = new OutputStreamWriter(os, Charset.forName("UTF-8"));
                        wr.write("access_id_CA X509 '");
                        wr.write(subject);
                        wr.write("'\npos_rights globus CA:sign\ncond_subjects globus \"*\"\n");
                        wr.flush();
                        wr.close();
                        os.close();

                        if (logger.isDebugEnabled()) {
                            logger.debug("wrote trusted certificate policy to " + filename);
                        }

                        // success.  commit the bootstrapped directory.
                        if (tmpDir.renameTo(x509Dir) == true)
                        {
                            if (logger.isDebugEnabled()) {
                                logger.debug("renamed " + tmpDir.getPath() + " to " +
                                             x509Dir.getPath());
                            }
                        }
                        else
                        {
                            throw new MyProxyException("Unable to rename " + tmpDir.getPath() + " to " +
                                                x509Dir.getPath());
                        }
                    }
                    else
                    {
                        throw new MyProxyException("Cannot create temporary directory: " + tmpDir.getName());
                    }
                }
            }
        } catch(Exception e) {
            throw new MyProxyException("MyProxy bootstrapTrust failed.", e);
        }
    }

    private static String readLine(InputStream is) throws IOException {
        StringBuffer sb = new StringBuffer();
        for (int c = is.read(); c > 0 && c != '\n'; c = is.read()) {
            sb.append((char) c);
        }
        if (sb.length() > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Received line: " + sb);
            }
            return new String(sb);
        }
        return null;
    }

    private InputStream handleReply(InputStream in)
        throws IOException, MyProxyException {
        return handleReply(in, null, null, false);
    }

    
    private InputStream handleReply(InputStream in, 
                                    OutputStream out,
                                    GSSCredential authzcreds,
                                    boolean wantTrustroots)
        throws IOException, MyProxyException {
        String tmp = null;

        /* there was something weird here with the 
           received protocol version sometimes. it
           contains an extra <32 byte. fixed it by
           using endsWith. now i read extra byte at the
           end of each message.
        */

        // protocol version
        tmp = readLine(in);
        if (tmp == null) {
            throw new EOFException();
        }
        if (!tmp.endsWith(MyProxyConstants.VERSION)) {
            throw new MyProxyException("Protocol version mismatch: " + tmp);
        }

        // response
        tmp = readLine(in);
        if (tmp == null) {
            throw new EOFException();
        }

        if (!tmp.startsWith(RESPONSE)) {
            throw new MyProxyException("Invalid reply: no response message");
        }

        boolean error = tmp.charAt(RESPONSE.length()) == '1';
        boolean authzchallenge = tmp.charAt(RESPONSE.length()) == '2';

        if (error) {
            StringBuffer errorStr = new StringBuffer();
            while( (tmp = readLine(in)) != null ) {
                if (tmp.startsWith(ERROR)) {
                    if (errorStr.length() > 0) errorStr.append(' ');
                    errorStr.append(tmp.substring(ERROR.length()));
                }
            }
            if (errorStr.length() == 0) {
                errorStr.append("unspecified server error");
            }
            throw new MyProxyException(errorStr.toString());
        }
        if (authzchallenge) {
            if (authzcreds == null) {
                throw new MyProxyException("Unable to respond to server's authentication challenge. No credentials for renewal.");
            }
            if (out == null) {
                throw new MyProxyException("Internal error. Authz challenge but no OutputStream.");
            }
            String[] authzdata = null;
            while( (tmp = readLine(in)) != null ) {
                if (tmp.startsWith(AUTHZ_DATA)) {
                    int pos = tmp.indexOf(':', AUTHZ_DATA.length()+1);
                    if (pos != -1) {
                        authzdata = new String[2];
                        authzdata[0] = 
                            tmp.substring(AUTHZ_DATA.length(), pos).trim();
                        authzdata[1] =
                            tmp.substring(pos+1).trim();
                    }
                }
            }
            if (authzdata == null) {
                throw new MyProxyException("Unable to parse authorization challenge from server.");
            }
            if (authzdata[0].equals("X509_certificate")) {
                GlobusGSSCredentialImpl pkiCred =
                    (GlobusGSSCredentialImpl)authzcreds;
                try {
                    Signature sig = Signature.getInstance("SHA1withRSA");
                    sig.initSign(pkiCred.getPrivateKey());
                    sig.update(authzdata[1].getBytes());
                    byte[] sigbytes = sig.sign();
                    X509Certificate [] certs =
                        pkiCred.getCertificateChain();
                    ByteArrayOutputStream buffer =
                        new ByteArrayOutputStream(2048);
                    buffer.write(2); // AUTHORIZETYPE_CERT
                    buffer.write(0); buffer.write(0); buffer.write(0); // pad
                    buffer.write(0); buffer.write(0); buffer.write(0); // pad
                    buffer.write(sigbytes.length);
                    buffer.write(sigbytes);
                    buffer.write((byte)certs.length);
                    for (int i=0; i<certs.length; i++) {
                        buffer.write(certs[i].getEncoded());
                    }
                    out.write(buffer.toByteArray());
                    out.flush();
                } catch(Exception e) {
                    throw new MyProxyException("Authz response failed.", e);
                }
                return handleReply(in, out, authzcreds, wantTrustroots);
            } else {
                throw new MyProxyException("Unable to respond to server's authentication challenge. Unimplemented method: " + authzdata[0]);
            }
        }

        if (wantTrustroots == true) {
            while ((tmp = readLine(in)) != null) {
                if (tmp.startsWith(TRUSTROOTS)) {
                    String filenameList = tmp.substring(TRUSTROOTS.length());
                    this.trustrootFilenames = filenameList.split(",");
                    this.trustrootData = new String[this.trustrootFilenames.length];
                    for(int i = 0; i < this.trustrootFilenames.length; i++) {
                    String lineStart = "FILEDATA_" + this.trustrootFilenames[i]
                        + "=";
                    tmp = readLine(in);
                    if (tmp == null) {
                        throw new EOFException();
                    }
                    if (!tmp.startsWith(lineStart)) {
                        throw new MyProxyException(
                            "bad MyProxy protocol RESPONSE: expecting "
                            + lineStart + " but received " + tmp);
                    }
                    this.trustrootData[i] = new String(
                        Base64.decode(tmp.substring(lineStart.length()).getBytes()));
                    }
                }
            }
        }
        
        /* always consume the entire message */
        int avail = in.available();
        byte [] b = new byte[avail];
        if (avail > 0) in.read(b);

        ByteArrayInputStream inn = new ByteArrayInputStream(b);

        return inn;
    }

    private static void close(OutputStream out, 
                              InputStream in,
                              Socket sock) {
        try {
            if (out != null) out.close();
            if (in  != null) in.close();
            if (sock != null) sock.close();
        } catch(IOException ee) {}
    }

    // --------------- OLD MYPROXY API -----------------------------

    /**
     * Stores credentials on MyProxy server.
     *
     * @param  host
     *         The hostname of MyProxy server.
     * @param  port
     *         The port number of MyProxy server.
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username to store the credentials under.
     * @param  passphrase
     *         The passphrase to use to encrypt the stored 
     *         credentials.
     * @param  lifetime
     *         The requested lifetime of the stored credentials (in seconds).
     * @exception MyProxyException
     *         If an error occurred during the put operation.
     * @deprecated Use non-static methods instead.
     */
    public static void put(String host,
                           int port,
                           GSSCredential credential,
                           String username,
                           String passphrase,
                           int lifetime)  
        throws MyProxyException {
        put(host, port, credential, username, passphrase, 
            lifetime, null);
    }
    
    /**
     * Stores credentials on MyProxy server.
     *
     * @param  host
     *         The hostname of MyProxy server.
     * @param  port
     *         The port number of MyProxy server.
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username to store the credentials under.
     * @param  passphrase
     *         The passphrase to use to encrypt the stored
     *         credentials.
     * @param  lifetime
     *         The requested lifetime of the stored credentials (in seconds).
     * @param  subjectDN
     *         The expected subject name of MyProxy server. This
     *         is used for security purposes. If null, host
     *         authentication will be performed.
     * @exception MyProxyException
     *         If an error occurred during the put operation.
     * @deprecated Use non-static methods instead.
     */
    public static void put(String host, 
                           int port,
                           GSSCredential credential,
                           String username,
                           String passphrase,
                           int lifetime,
                           String subjectDN) 
        throws MyProxyException {

        MyProxy myProxy = new MyProxy(host, port);
        myProxy.setAuthorization(getAuthorization(subjectDN));
        myProxy.put(credential,
                    username,
                    passphrase,
                    lifetime);
    }

    /**
     * Removes delegated credentials from the MyProxy server.
     *
     * @param  host
     *         The hostname of MyProxy server.
     * @param  port
     *         The port number of MyProxy server.
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username of the credentials to remove.
     * @param  passphrase
     *         The passphrase of the credentials to remove.
     *         Right now it is ignored by the MyProxy sever.
     * @throws MyProxyException
     *         If an error occurred during the operation.
     * @deprecated Use non-static methods instead.
     */
    public static void destroy(String host,
                               int port,
                               GSSCredential credential,
                               String username,
                               String passphrase)
        throws MyProxyException {
        destroy(host, port, credential, 
                username, passphrase, null);
    }

    /**
     * Removes delegated credentials from MyProxy server.
     *
     * @param  host
     *         The hostname of MyProxy server.
     * @param  port
     *         The port number of MyProxy server.
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username of the credentials to remove.
     * @param  passphrase
     *         The passphrase of the credentials to remove.
     *         Right now it is ignored by the MyProxy sever.
     * @param  subjectDN
     *         The expected subject name of MyProxy server. This
     *         is used for security purposes. If null, host
     *         authentication will be performed.
     * @throws MyProxyException
     *         If an error occurred during the operation.
     * @deprecated Use non-static methods instead.
     */
    public static void destroy(String host,
                               int port,
                               GSSCredential credential,
                               String username,
                               String passphrase,
                               String subjectDN)
        throws MyProxyException {

        MyProxy myProxy = new MyProxy(host, port);
        myProxy.setAuthorization(getAuthorization(subjectDN));
        myProxy.destroy(credential,
                        username,
                        passphrase);
    }
  
    /**
     * Retrieves delegated credentials from MyProxy server.
     *
     * Notes: Performs simple verification of private/public keys of
     *        the delegated credential. Should be improved later.
     *        And only checks for RSA keys.
     *
     * @param  host
     *         The hostname of MyProxy server.
     * @param  port
     *         The port number of MyProxy server.
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username of the credentials to retrieve.
     * @param  passphrase
     *         The passphrase of the credentials to retrieve.
     * @param  lifetime
     *         The requested lifetime of the retrieved credential (in seconds).
     * @return GSSCredential
     *         The retrieved delegated credentials.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     * @deprecated Use non-static methods instead.
     */
    public static GSSCredential get(String host,
                                    int port,
                                    GSSCredential credential,
                                    String username,
                                    String passphrase,
                                    int lifetime)
        throws MyProxyException {
        return get(host, port, credential, 
                   username, passphrase, lifetime, null);
    }

    /**
     * Retrieves delegated credentials from MyProxy server.
     *
     * Notes: Performs simple verification of private/public keys of
     *        the delegated credential. Should be improved later.
     *        And only checks for RSA keys.
     *
     * @param  host
     *         The hostname of MyProxy server.
     * @param  port
     *         The port number of MyProxy server.
     * @param  credential
     *         The GSI credentials to use.
     * @param  username
     *         The username of the credentials to retrieve.
     * @param  passphrase
     *         The passphrase of the credentials to retrieve.
     * @param  lifetime
     *         The requested lifetime of the retrieved credential (in seconds).
     * @param  subjectDN
     *         The expected subject name of MyProxy server. This
     *         is used for security purposes. If null, host
     *         authentication will be performed.
     * @return GSSCredential 
     *         The retrieved delegated credentials.
     * @exception MyProxyException
     *         If an error occurred during the operation.
     * @deprecated Use non-static methods instead.
     */
    public static GSSCredential get(String host,
                                    int port,
                                    GSSCredential credential,
                                    String username,
                                    String passphrase,
                                    int lifetime,
                                    String subjectDN)
        throws MyProxyException {

        MyProxy myProxy = new MyProxy(host, port);
        myProxy.setAuthorization(getAuthorization(subjectDN));
        return myProxy.get(credential,
                           username,
                           passphrase,
                           lifetime);
    }

    private static Authorization getAuthorization(String subjectDN) {
        if (subjectDN == null) {
            return new MyProxyServerAuthorization();
        } else {
            return new IdentityAuthorization(subjectDN);
        }
    }

    private GSSCredential getAnonymousCredential() 
        throws GSSException {
        GSSManager manager = ExtendedGSSManager.getInstance();
        GSSName anonName = manager.createName((String)null, null);
        return manager.createCredential(anonName,
                                        GSSCredential.INDEFINITE_LIFETIME,
                                        (Oid)null,
                                        GSSCredential.INITIATE_AND_ACCEPT);
    }

    /**
     * Returns the trusted certificates directory location where
     * writeTrustRoots() will store certificates.
     * It first checks the X509_CERT_DIR system property.
     * If that property is not set, it uses
     * ${user.home}/.globus/certificates.
     * Note that, unlike CoGProperties.getCaCertLocations(),
     * it does not return /etc/grid-security/certificates or
     * ${GLOBUS_LOCATION}/share/certificates.
     */
    public static String getTrustRootPath() {
        String path = System.getProperty("X509_CERT_DIR");
        if (path == null) {
            path = System.getProperty("user.home") + TRUSTED_CERT_PATH;
        }
        return path;
    }

    /**
     * Writes the retrieved trust roots to the Globus trusted certificates
     * directory.
     * @return true if trust roots are written successfully, false if no
     *         trust roots are available to be written
     */
    public boolean writeTrustRoots() throws IOException {
        return writeTrustRoots(getTrustRootPath());
    }

    /**
     * Writes the retrieved trust roots to a trusted certificates directory.
     * @param directory
     *        path where the trust roots should be written
     * @return true if trust roots are written successfully, false if no
     *         trust roots are available to be written
     */
    public boolean writeTrustRoots(String directory) throws IOException {
        if (this.trustrootFilenames == null || this.trustrootData == null) {
            return false;
        }
        File rootDir = new File(directory);
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }
        for (int i = 0; i < trustrootFilenames.length; i++) {
            FileOutputStream out = new FileOutputStream(
                directory + File.separator + this.trustrootFilenames[i]);
            out.write(this.trustrootData[i].getBytes());
            out.close();
        }
        return true;
    }

    /*
      the following methods are based off code to compute the subject
      name hash from:
      http://blog.piefox.com/2008/10/javaopenssl-ca-generation.html
    */
    private static String opensslHash(X509Certificate cert) {
        try {
            return openssl_X509_NAME_hash(cert.getSubjectX500Principal());
        }
        catch (Exception e) {
            throw new Error("MD5 isn't available!", e);
        }
    }

    /**
     * Generates a hex X509_NAME hash (like openssl x509 -hash -in cert.pem)
     * Based on openssl's crypto/x509/x509_cmp.c line 321
     */
    private static String openssl_X509_NAME_hash(X500Principal p) throws Exception {
        // This code replicates OpenSSL's hashing function
        // DER-encode the Principal, MD5 hash it, then extract the first 4 bytes and reverse their positions
        byte[] derEncodedSubject = p.getEncoded();
        byte[] md5 = MessageDigest.getInstance("MD5").digest(derEncodedSubject);

        // Reduce the MD5 hash to a single unsigned long
        byte[] result = new byte[] { md5[3], md5[2], md5[1], md5[0] };
        return toHex(result);
    }

    // encode binary to hex
    private static String toHex(final byte[] bin) {
        if (bin == null || bin.length == 0)
            return "";

        char[] buffer = new char[bin.length * 2];

        final char[] hex = "0123456789abcdef".toCharArray();

        // i tracks input position, j tracks output position
        for (int i = 0, j = 0; i < bin.length; i++)
        {
            final byte b = bin[i];
            buffer[j++] = hex[(b >> 4) & 0x0F];
            buffer[j++] = hex[b & 0x0F];
        }
        return new String(buffer);
    }
}
