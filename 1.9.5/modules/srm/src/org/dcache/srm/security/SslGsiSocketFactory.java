// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.4  2006/06/06 21:18:18  tdh
// Made delegateCredential return GSIGssSocket so that cell delegated to can return an object.
//
// Revision 1.3.10.1  2006/05/25 21:11:28  tdh
// Made delegateCredential return GSIGssSocket so that cell delegated to can return an object.
//
// Revision 1.3.0.10 2006/05/25 16:14:38 tdh
// Made delegateCredential return GSIGssSocket so that cell delegated to can return an object.
//
// Revision 1.3  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.2  2005/01/25 19:04:01  timur
// added string definition compatible with glue server
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.41  2005/01/07 20:55:31  timur
// changed the implementation of the built in client to use apache axis soap toolkit
//
// Revision 1.40  2004/12/09 19:26:26  timur
// added new updated jglobus libraries, new group commands for canceling srm requests
//
// Revision 1.39  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.38  2004/10/28 02:41:32  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.37  2004/10/20 21:30:59  timur
// wrapped the returned socket with one more layer, which delays the Handshake till the time when the socket is used
//
// Revision 1.36  2004/09/07 04:14:28  timur
// fixed a memory leak
//
// Revision 1.35  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.34.2.4  2004/07/29 22:17:30  timur
// Some functionality for disk srm is working
//
// Revision 1.34.2.3  2004/06/30 20:37:24  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.34.2.2  2004/06/16 19:44:34  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

package org.dcache.srm.security;

import org.dcache.srm.util.Configuration;
import java.util.Hashtable;
import electric.net.socket.ISocketFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.SocketAddress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;


//import org.globus.net.GSIServerSocketFactory;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;

import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.GlobusCredentialException;

import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;


import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;

//import org.dcache.srm.AbstractStorageElement;
import electric.util.Context;
import org.dcache.srm.Logger;
import org.ietf.jgss.GSSName;
/**
 *
 * @author  timur
 */
public class SslGsiSocketFactory implements ISocketFactory {
    public static final String ID_POSTFIX=":ID";
    public static final String DELEG_CRED_POSTFIX=":DELEG";
    public static final String GSSCONTEXT=":CONTEXT";
    
    private Hashtable client_sockets = new Hashtable();
    private boolean user_cred_locked;
    private Object user_cred_lock = new Object();
    private Thread owner;
    private GSSCredential user_credential;
    public static final int BUFFER_SIZE = 1024;
    public String name;
    private Logger logger;
    private String x509ServiceCert;
    private String x509ServiceKey;
    private String x509TrastedCACerts;
    private String x509UserProxy;
    private boolean doDelegation=false;
    private boolean fullDelegation=false;
    
    // if this is true then the user of the factory
    // would need to create or obtain by delegation the credentials
    // and pass it to the factory vi lockCredential
    
    private final boolean use_user_credential;
    
    //Configuration configuration;
    //GSIServerSocketFactory factory;
    
    /** Creates a new instance of GSSSocketFactory */
    //this constructor is used on a server
    public SslGsiSocketFactory(
    Configuration configuration) {
        this(
        configuration.getStorage(),
        null, //proxy
        configuration.getX509ServiceCert(),
        configuration.getX509ServiceKey(),
        configuration.getX509TrastedCACerts(),
        true);
    }
    
    public SslGsiSocketFactory(Logger logger,
    String x509UserProxy,
    String x509ServiceCert,String x509ServiceKey,String x509TrastedCACerts) {
        this(logger,x509UserProxy,x509ServiceCert,x509ServiceKey,x509TrastedCACerts,false);
    }
    
    public SslGsiSocketFactory(Logger logger,
    String x509UserProxy,
    String x509ServiceCert,String x509ServiceKey,String x509TrastedCACerts,
    boolean use_user_credential) {
        name = "SslGsiSocketFactory";
        //this.configuration = configuration;
        this.logger = logger;
        this.x509UserProxy = x509UserProxy;
        this.x509ServiceCert =  x509ServiceCert;
        this.x509ServiceKey = x509ServiceKey;
        this.x509TrastedCACerts = x509TrastedCACerts;
        this.use_user_credential = use_user_credential;
        say("SslGsiSocketFactory created");
    }
    
    public static GlobusCredential service_cred;
    public static TrustedCertificates trusted_certs;
    
    public static GSSCredential getServiceCredential(
    String x509ServiceCert,
    String x509ServiceKey,int usage) throws GSSException {
        
        try {
            if(service_cred != null) {
                service_cred.verify();
            }
        }
        catch(GlobusCredentialException gce) {
            service_cred = null;
            
        }
        
        
        if(service_cred == null) {
            try {
                service_cred =new GlobusCredential(
                x509ServiceCert,
                x509ServiceKey
                );
            }
            catch(GlobusCredentialException gce) {
                throw new GSSException(GSSException.NO_CRED ,
                0,
                "could not load host globus credentials "+gce.toString());
            }
        }
        
        GSSCredential cred = new GlobusGSSCredentialImpl(service_cred, usage);
        
        return cred;
    }
    
    
    public static GSSContext getServiceContext(
    String x509ServiceCert,
    String x509ServiceKey,
    String x509TrastedCACerts) throws GSSException {
        GSSCredential cred = getServiceCredential(x509ServiceCert, x509ServiceKey,
        GSSCredential.ACCEPT_ONLY);
        
        if(trusted_certs == null) {
            trusted_certs =
            TrustedCertificates.load(x509TrastedCACerts);
        }
        
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
        (ExtendedGSSContext) manager.createContext(cred);
        
        context.setOption(GSSConstants.GSS_MODE,
        GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
        trusted_certs);
        return context;
    }
    
    private GSSContext getServiceContext() throws GSSException {
        try {
            return getServiceContext(x509ServiceCert, x509ServiceKey,  x509TrastedCACerts);
        }
        catch(GSSException gsse) {
            esay(gsse);
            throw gsse;
        }
    }
    
    public static GSSCredential createUserCredential(
    String x509UserProxy)  throws GlobusCredentialException, GSSException {
        if(x509UserProxy != null) {
            GlobusCredential gcred = new GlobusCredential(x509UserProxy);
            GSSCredential cred =
            new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
            return cred;
        }
        GlobusCredential gcred = GlobusCredential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
        
    }
    
    public static GSSCredential createUserCredential(String x509ServiceCert, String x509ServiceKey)
    throws GlobusCredentialException, GSSException {
            if(x509ServiceCert != null && x509ServiceKey != null) {
            GlobusCredential gcred =new GlobusCredential(
            x509ServiceCert,
            x509ServiceKey
            );
            GSSCredential cred =
            new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
            return cred;
        }
        
        GlobusCredential gcred = GlobusCredential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
    }

    public GSSCredential createUserCredential()
    throws GlobusCredentialException, GSSException {
        if(x509UserProxy != null) {
            return createUserCredential(x509UserProxy);
        }
        else if(x509ServiceCert != null && x509ServiceKey != null) {
            return createUserCredential(x509ServiceCert,x509ServiceKey);
        }
        
        GlobusCredential gcred = GlobusCredential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
    }

    public static GSSCredential createUserCredential(String x509UserProxy, String x509ServiceCert, String x509ServiceKey)
    throws GlobusCredentialException, GSSException {
        
        if(x509UserProxy != null) {
            return createUserCredential(x509UserProxy);
        }
        else if(x509ServiceCert != null && x509ServiceKey != null) {
            return createUserCredential(x509ServiceCert,x509ServiceKey);
        }
        
        GlobusCredential gcred = GlobusCredential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
    }
    
    
    

    private void say(String s) {
    /*
     //temp. not needed, assumed to work fine
     */
        if(logger != null) {
            
            logger.log("SslGsiSocketFactory :"+s);
        }
        /*   */
    }
    
    private void esay(String s) {
        if(logger != null) {
            logger.elog("SslGsiSocketFactory :"+s);
        }
    }
    
    private void esay(Throwable t) {
        if(logger != null) {
            logger.elog("SslGsiSocketFactory :");
            logger.elog(t);
        }
    }
    
    public java.net.ServerSocket createServerSocket(int port,
    int backlog)
    throws IOException {
        //if(factory == null)
        //{
        //    factory = GSIServerSocketFactory.getDefault ();
        //
        //}
        return new GsiServerSocketWrap(port,  backlog);
    }
    
    public java.net.Socket createSocket(java.net.InetAddress inetAddress,
    int port)
    throws IOException {
        say("createSocket("+inetAddress+","+port+")");
        Socket s =null;
        try {
            GSSCredential credential = consumeCredential();
            if(credential == null) {
                esay("createSocket: credential is null !!!");
                throw new IOException("credential is null !!!");
            }
            say("createSocket() user credential is "+credential.getName());
            GSSManager manager = ExtendedGSSManager.getInstance();
            org.globus.gsi.gssapi.auth.GSSAuthorization gssAuth = 
            org.globus.gsi.gssapi.auth.HostAuthorization.getInstance();
            GSSName targetName = gssAuth.getExpectedName(null, inetAddress.getCanonicalHostName());
            ExtendedGSSContext context =
            (ExtendedGSSContext) manager.createContext(targetName,
                GSSConstants.MECH_OID,
                credential,
                GSSContext.DEFAULT_LIFETIME);
            context.setOption(GSSConstants.GSS_MODE,
            GSIConstants.MODE_GSI);
            context.requestCredDeleg(doDelegation);
            if(doDelegation) {
                
                if(fullDelegation) {
                    context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_FULL);
                }
                else {
                    context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_LIMITED);
                }
            }
            s = new Socket(inetAddress,port);
            //say("createSocket() created pain socket "+s);
            GSIGssSocket gsiSocket = new GSIGssSocket(s, context);
            //say("createSocket() gsiSocket "+gsiSocket);
            gsiSocket.setUseClientMode(true);
            gsiSocket.setAuthorization(gssAuth);
            //say("createSocket() setting wrap mode to SSL_MODE");
            gsiSocket.setWrapMode(GssSocket.SSL_MODE);
            //say("createSocket() staring handshake");
            gsiSocket.startHandshake();
            //say("createSocket() handshake complete");
            s = gsiSocket;
        }
        catch(Exception e) {
            //esay("createSocket() failed with exception:");
            //esay(e);
            if(s!=null) {
                try {
                    s.close();
                }
                catch(Exception e1) {
                }
            }
            // I now think we should not unlock socket here, since the 
            // toolkit that uses the factory tries more than one time to create socket
            // without propogation of the exception
            // we just need to make sure that the code, which uses this sockets (SRMClientV1)
            // handles errors correctly
            //saveSocketAndUnlock(null);
            //say("createSocket() exception: "+e);
            throw new IOException(e.toString());
        }
        saveSocketAndUnlock(s);
        //say("createSocket() returning socket "+s);
        return s;
        
    }
    
    public java.net.Socket createSocket(java.net.Socket socket,
    String host,
    int port,
    boolean autoclose)
    throws IOException {
        say("not implemented createSocket(java.net.Socket socket,"+
        "String host,int port, boolean autoclose)");
        throw new IOException("not implemented");
    }
    
    /**
     * the goal of this class is to delay the retreaval of the 
     *  socket inputStream  and outputStream till the moment when it is just about to be used
     *  Retreaval of the input stream or output stream triggers the 
     *  SSL handshake for globus GSIGssSocket, which can sometimes block indefenetely 
     * or throw the exceptions, and it is causing the server shutdown 
     *  for the glue web service server.
     *  On other hand the usage of the input stream and output sream is performed in
     * the code that will not affect the whole server
     */
    private static class SocketInputStreamWrapper extends InputStream {
        private Socket s;
        private InputStream in;
        public SocketInputStreamWrapper(Socket s) {
            if(s == null) {
                throw new IllegalArgumentException("s is null");
            }
            this.s = s;
        }
        
        private synchronized void retrieveInputIfNeeded() throws IOException {
            if(in == null) {
                in= s.getInputStream();
            }
        }
        
        public int read() throws IOException {
            retrieveInputIfNeeded();
            return in.read();
        }
        
        public int read(byte b[]) throws IOException {
            retrieveInputIfNeeded();
            return in.read(b);
        }
        public int read(byte b[], int off, int len) throws IOException {
            retrieveInputIfNeeded();
            return in.read(b,off,len);
        }
        public long skip(long n) throws IOException {
            retrieveInputIfNeeded();
            return in.skip(n);
        }
        public int available() throws IOException {
            retrieveInputIfNeeded();
            return in.available();
            
        }
        public void close() throws IOException {
            retrieveInputIfNeeded();
            in.close();
        }
        public synchronized void mark(int readlimit) {
            try
            {
                retrieveInputIfNeeded();
                in.mark(readlimit);
            }
            catch(IOException e){
                e.printStackTrace();
            }
        }
        
        public synchronized void reset() throws IOException {
            retrieveInputIfNeeded();
            in.reset();
        }
        public boolean markSupported() {
            try
            {
                retrieveInputIfNeeded();
                return in.markSupported();
            }
            catch(IOException e){
                return false;
            }
        }
    }

    
    /**
     * the goal of this class is to delay the retreaval of the 
     *  socket inputStream  and outputStream till the moment when it is just about to be used
     *  Retreaval of the input stream or output stream triggers the 
     *  SSL handshake for globus GSIGssSocket, which can sometimes block indefenetely 
     * or throw the exceptions, and it is causing the server shutdown 
     *  for the glue web service server.
     *  On other hand the usage of the input stream and output sream is performed in
     * the code that will not affect the whole server
     */
    private static class SocketOutputStreamWrapper extends OutputStream {
        private Socket s;
        private OutputStream out;
        public SocketOutputStreamWrapper(Socket s) {
            if(s == null) {
                throw new IllegalArgumentException("s is null");
            }
            this.s = s;
        }
        
        private synchronized void retrieveOutputIfNeeded() throws IOException {
            if(out == null) {
                out= s.getOutputStream();
            }
        }
        
        public void write(int b) throws IOException {
            retrieveOutputIfNeeded();
            out.write(b);
        }
        
        public void write(byte b[]) throws IOException {
            retrieveOutputIfNeeded();
            out.write(b);
        }
        
        public void write(byte b[], int off, int len) throws IOException {
            retrieveOutputIfNeeded();
            out.write(b,off,len);
        }
        public void flush() throws IOException {
            retrieveOutputIfNeeded();
            out.flush();
        }
        public void close() throws IOException {
            retrieveOutputIfNeeded();
            out.close();
        }
    }
    
    private static class SocketDelayedInputOutputRetrievalWrapper extends Socket
    {
        Socket s;
        public SocketDelayedInputOutputRetrievalWrapper(Socket s) throws SocketException {
            super((SocketImpl)null);
            if(s == null) {
                throw new IllegalArgumentException("s is null");
            }
                
            this.s = s;
        }
        
       public InetAddress getInetAddress() {
           return s.getInetAddress();
       }
        
       public InetAddress getLocalAddress() {
           return s.getLocalAddress();
       }
       
       public int getPort() {
           return s.getPort();
       }
       
        public int getLocalPort() {
            return s.getLocalPort();
        }
        
        public SocketAddress getRemoteSocketAddress() {
            return s.getRemoteSocketAddress();
        }
        
        public SocketAddress getLocalSocketAddress() {
            return s.getRemoteSocketAddress();
        }
        
        //this method is a reason we had to go through this exersise!!!
        public InputStream getInputStream() throws IOException {
            return new SocketInputStreamWrapper(s);
        }
        
        //this method is a reason we had to go through this exersise!!!
        public OutputStream getOutputStream() throws IOException {
            return new SocketOutputStreamWrapper(s);
        }
        
        public void setTcpNoDelay(boolean on) throws SocketException {
            s.setTcpNoDelay(on);
        }
        
        public boolean getTcpNoDelay() throws SocketException {
            return s.getTcpNoDelay();
        }
        
        public void setSoLinger(boolean on, int linger) throws SocketException {
            s.setSoLinger(on, linger);
        }
        public int getSoLinger() throws SocketException {
            return s.getSoLinger();
        }
        public synchronized void setSoTimeout(int timeout) throws SocketException {
            s.setSoTimeout(timeout);
        }
        public synchronized int getSoTimeout() throws SocketException {
            return s.getSoTimeout();
        }
        public synchronized void setSendBufferSize(int size)
        throws SocketException{
            s.setSendBufferSize(size);
        }

        public synchronized int getSendBufferSize() throws SocketException {
            return s.getSendBufferSize();
        }
        public synchronized void setReceiveBufferSize(int size)
        throws SocketException{
            s.setSendBufferSize(size);
        }
        public synchronized int getReceiveBufferSize() 
        throws SocketException{
            return s.getReceiveBufferSize();
        }
        public void setKeepAlive(boolean on) throws SocketException {
            s.setKeepAlive(on);
        }
        public boolean getKeepAlive() throws SocketException {
            return s.getKeepAlive();
        }
        public synchronized void close() throws IOException {
            s.close();
        }
        public void shutdownInput() throws IOException {
            s.shutdownInput();
        }
        public void shutdownOutput() throws IOException {
            s.shutdownOutput();
        }
        public String toString() {
            return s.toString();
        }

    }
    private class GsiClientSocket extends GSIGssSocket {
        private String key;
        private GSSCredential deleg_cred;
        private boolean tried_to_retrieve_deleg_cred = false;
        public GsiClientSocket(Socket s, GSSContext context) {
            super(s, context);
            key =
            s.getLocalAddress().getHostAddress()+s.getLocalPort()+
            s.getInetAddress().getHostAddress()+s.getPort();
            final String idkey = key+ID_POSTFIX;
            final String delegatedkey = key+DELEG_CRED_POSTFIX;
            final String gsscontextkey=key+GSSCONTEXT;
            final Context appContext = Context.application();
            appContext.removeProperty(key);
            appContext.removeProperty(idkey);
            appContext.removeProperty(delegatedkey);
            appContext.removeProperty(gsscontextkey);
            
            setUseClientMode(false);
                    
            setAuthorization(
                    new Authorization() {
                        public void authorize(GSSContext context, String host)
                        throws AuthorizationException {
                            try {
                                String secureId = context.getSrcName().toString();
                                
                                
                                say("SslGsiSocketFactory: storing "
                                +idkey+","+secureId);
                                appContext.setProperty(idkey,secureId);
                                say("SslGsiSocketFactory: storing gss context");
                                appContext.setProperty(gsscontextkey,context);
                            }
                            catch(GSSException gsse) {
                                esay(gsse);
                                throw new AuthorizationException(
                                "failed to obtain secure id :"+
                                gsse.toString());
                            }
                        }
                    }
                    
                    );
            setWrapMode(GssSocket.SSL_MODE);
            
        }
        
        private synchronized void retrieveAndStoreDelegatedCredentials()
         throws Throwable
        {
        
                if(!tried_to_retrieve_deleg_cred)
                {
                    tried_to_retrieve_deleg_cred = true;
                    try
                    {
                        deleg_cred =context.getDelegCred();
                        if(deleg_cred != null) {
                            Context appContext = Context.application();
                            String idkey = key+ID_POSTFIX;
                            String delegatedkey = key+DELEG_CRED_POSTFIX;
                            say("SslGsiSocketFactory: storing delegated credentials "+
                            delegatedkey+","+deleg_cred.getName());
                            appContext.setProperty(delegatedkey,deleg_cred);
                        }
                    }catch (GSSException gsse) {
                        esay(gsse);
                    }

                }
        }
        public synchronized OutputStream getOutputStream() 
            throws IOException {
            try
            {
                OutputStream  out  = super.getOutputStream();  
                retrieveAndStoreDelegatedCredentials();
                return out;
            }
            catch(IOException e){
                esay(e);
                esay("propogating the exception to the caller");
                throw e;
            }
            catch(Throwable t){
                esay(t);
                esay("throwing io exception insted");
                throw new IOException("getOutputStream failed with error "+t);
            }
            
        }

        public synchronized InputStream getInputStream() 
        throws IOException {
            try
            {
                InputStream in = super.getInputStream();
                retrieveAndStoreDelegatedCredentials();
                return in;
           }
            catch(IOException e){
                esay(e);
                esay("propogating the exception to the caller");
                throw e;
            }
            catch(Throwable t){
                esay(t);
                esay("throwing io exception insted");
                throw new IOException("getInputStream failed with error "+t);
            }
            
        }
        
        public void close() throws IOException {
            super.close();
            if(key != null) {
                    final String idkey = key+ID_POSTFIX;
                    final String delegatedkey = key+DELEG_CRED_POSTFIX;
                    final String gsscontextkey = key+GSSCONTEXT;
                Context appContext = Context.application();
                appContext.removeProperty(key);
                appContext.removeProperty(idkey);
                appContext.removeProperty(delegatedkey);
                appContext.removeProperty(gsscontextkey);
                //say("GsiClientSocket finalize called");
                key=null;
            }
        }
        protected void finalize() throws Throwable {
            super.finalize();
            if(key != null) {
                    final String idkey = key+ID_POSTFIX;
                    final String delegatedkey = key+DELEG_CRED_POSTFIX;
                    final String gsscontextkey = key+GSSCONTEXT;
                Context appContext = Context.application();
                appContext.removeProperty(key);
                appContext.removeProperty(idkey);
                appContext.removeProperty(delegatedkey);
                appContext.removeProperty(gsscontextkey);
                //say("GsiClientSocket finalize called");
            }
            
        }
        
    }
    private class GsiServerSocketWrap extends java.net.ServerSocket {
        public GsiServerSocketWrap(int port, int backlog) throws IOException {
            super(port,backlog);
        }
        /* the crashes of accept crash the whole GLUE which we do not want
         * so I implement accept which does not do it
         */
        public Socket accept()
        throws IOException {
            
            while(true) {
                Socket s = null;
                try {
                    say("GsiSslServerSocket, waiting for incomming connection...");
                    s = super.accept();
                    
                    say("GsiSslServerSocket accepted socket from host ="+ s.getInetAddress());
                    GSSContext context = getServiceContext();
                    return  new SocketDelayedInputOutputRetrievalWrapper( 
                            new GsiClientSocket(s, context));
                    
                }
                catch(Exception e) {
                    e.printStackTrace();
                    try {
                        if(s != null) {
                            s.close();
                        }
                    }
                    catch(Throwable t) {
                        //just ignore any error
                    }
                    
                }
            }
        }
        
    }
    
    /*
     * The following function were created to provide synchronisation
     * between the given user credentials and the client connection.
     * We do not have control over the creation of client sockets,
     * which are created in the abys of the glue, and we want to support
     * the creation os sockets with multiple credentials, corresponding
     * to the multitude of the incomming srmCopy connections
     */
    public void lockCredential(GSSCredential  user_credential)
    throws InterruptedException {
        
        if(!use_user_credential) {
            return;
            /*String error =
            "lockCredential() illeagal state: use_user_credential is "+
            use_user_credential;
            esay(error);
            throw new IllegalStateException(error);*/
        }
        //say(" lockCredential()");
        
        while (true) {
            try {
                synchronized(user_cred_lock) {
                    //say(" lockCredential() user_cred_locked  ="+user_cred_locked);
                    
                    if(user_cred_locked) {
                        //say(" lockCredential() waiting");
                        user_cred_lock.wait();
                        //say(" lockCredential() done waiting");
                    }
                    
                    
                    //say(" lockCredential() setting user_cred_locked to true");
                    user_cred_locked = true;
                    //say(" lockCredential() setting owner to current thread");
                    owner = Thread.currentThread();
                    //say(" lockCredential() setting user_credential to "+user_credential);
                    this.user_credential = user_credential;
                    //say(" lockCredential() returns");
                    return;
                }
            }
            catch(InterruptedException ie) {
                esay(" InterruptedException: ");
                esay(ie);
                throw ie;
            }
        }
    }
    
    public void unlockCredential() {
       if(!use_user_credential) {
        /*    String error =
            "unlockCredential() illeagal state: use_user_credential is "+
            use_user_credential;
            esay(error);
            throw new IllegalStateException(error);
         */
        }
        
        //say("unlockCredential() ");
        synchronized(user_cred_lock) {
            //say("unlockCredential() user_cred_locked == "+user_cred_locked);
            Thread current_thread = Thread.currentThread();
            //say("unlockCredential() owner == current_thread = "+
            //(owner == current_thread));
            if( user_cred_locked && owner == current_thread ) {
                //say("unlockCredential() setting user_credential to null");
                user_credential = null;
                //say("unlockCredential() setting user_cred_locked to false");
                user_cred_locked = false;
                //say("unlockCredential() setting owner to null");
                owner = null;
                //say("unlockCredential() calling user_cred_lock.notify()");
                user_cred_lock.notify();
                client_sockets.remove(current_thread);
            }
        }
    }
    
    public void unlockCredentialAndCloseSocket() {
        
        if(!use_user_credential) {
            return;
           /* String error =
            "unlockCredentialAndCloseSocket() illeagal state: use_user_credential is "+
            use_user_credential;
            esay(error);
            throw new IllegalStateException(error);
            */
        }
        
        //say("unlockCredentialAndCloseSocket() ");
        synchronized(user_cred_lock) {
            //say("unlockCredentialAndCloseSocket() user_cred_locked == "+
            //user_cred_locked);
            Thread current_thread = Thread.currentThread();
            //say("unlockCredentialAndCloseSocket() owner == current_thread = "+
            //(owner == current_thread));
            if( user_cred_locked && owner == current_thread ) {
                //say("unlockCredentialAndCloseSocket() setting user_credential to null");
                user_credential = null;
                //say("unlockCredentialAndCloseSocket() setting user_cred_locked to false");
                user_cred_locked = false;
                //say("unlockCredentialAndCloseSocket() setting owner to null");
                owner = null;
                //say("unlockCredentialAndCloseSocket() calling user_cred_lock.notify()");
                user_cred_lock.notify();
            }
            
            Socket s = (Socket) client_sockets.remove(current_thread);
            if(s !=null) {
                try {
                    s.close();
                }
                catch(IOException ioe) {
                }
            }
        }
    }
    
    private GSSCredential consumeCredential()
    throws GlobusCredentialException, GSSException {
        //say("consumeCredential()");
        if(!use_user_credential) {
            return createUserCredential();
        }
        synchronized(user_cred_lock) {
            if(user_cred_locked) {
                //say("consumeCredential() returning user_context");
                return user_credential;
            }
        }
        return null;
    }
    
    public void saveSocketAndUnlock(Socket s) {
        if(!use_user_credential) {
            return ;
        }
        
        synchronized(user_cred_lock) {
            //say("saveSocketAndUnlock() user_cred_locked == "+user_cred_locked);
            if(user_cred_locked) {
                if(s!= null) {
                    //say("saveSocketAndUnlock() storing socket "+s);
                    client_sockets.put(owner,s);
                }
                //say("saveSocketAndUnlock() setting user_cred_locked to false");
                user_cred_locked = false;
                //say("saveSocketAndUnlock() setting owner to null");
                owner = null;
                //say("saveSocketAndUnlock() setting user_proxy to null");
                user_credential = null;
                //say("saveSocketAndUnlock() calling user_cred_lock.notify()");
                user_cred_lock.notify();
            }
        }
    }
    
    public static GSIGssSocket delegateCredential(java.net.InetAddress inetAddress,
    int port,GSSCredential credential,boolean fulldelegation)
    throws Exception {
        // say("createSocket("+inetAddress+","+port+")");
        Socket s =null;
      GSIGssSocket gsiSocket = null;
        try {
            //   say("delegateCredentials() user credential is "+credential);
            GSSManager manager = ExtendedGSSManager.getInstance();
            org.globus.gsi.gssapi.auth.GSSAuthorization gssAuth = 
            org.globus.gsi.gssapi.auth.HostAuthorization.getInstance();
            GSSName targetName = gssAuth.getExpectedName(null, inetAddress.getCanonicalHostName());
            ExtendedGSSContext context =
            (ExtendedGSSContext) manager.createContext(targetName,
                GSSConstants.MECH_OID,
                credential,
                GSSContext.DEFAULT_LIFETIME);
            context.setOption(GSSConstants.GSS_MODE,
            GSIConstants.MODE_GSI);
            context.requestCredDeleg(true);
            if(fulldelegation) {
                context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_FULL);
            }
            else {
                context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_LIMITED);
            }
            //context.setOption(
            s = new Socket(inetAddress,port);
            gsiSocket = new GSIGssSocket(s, context);
            gsiSocket.setUseClientMode(true);
            gsiSocket.setAuthorization(
            //org.globus.ogsa.impl.security.authorization.HostAuthorization.
            gssAuth
            );
            gsiSocket.setWrapMode(GssSocket.SSL_MODE);
            gsiSocket.startHandshake();
        }
        catch(Exception e) {
            if(s!=null) {
                try {
                    s.close();
                }
                catch(Exception e1) {
                }
            }
            throw e;
        }

      return gsiSocket;
    }
    
    //String http_header="";
    public static final void main(String [] args) throws Exception {
        String proxy = args[0];
        String trusted_certs = args[1];
        String host = args[2];
        int port = Integer.parseInt(args[3]);
        
        SslGsiSocketFactory factory = new SslGsiSocketFactory(
        new org.dcache.srm.Logger() {
            public void log(String s) {
                System.out.println(s);
            }
            public void elog(String s) {
                System.err.println(s);
            }
            public void elog(Throwable t) {
                t.printStackTrace();
            }
        },
        proxy,
        null,
        null,
        trusted_certs);
        java.net.InetAddress inetAddress = java.net.InetAddress.getByName(host);
        Socket s = factory.createSocket(inetAddress, port);
        
        
    }
    
    /** Getter for property doDelegation.
     * @return Value of property doDelegation.
     *
     */
    public boolean isDoDelegation() {
        return doDelegation;
    }
    
    /** Setter for property doDelegation.
     * @param doDelegation New value of property doDelegation.
     *
     */
    public void setDoDelegation(boolean doDelegation) {
        this.doDelegation = doDelegation;
    }
    
    /** Getter for property fullDelegation.
     * @return Value of property fullDelegation.
     *
     */
    public boolean isFullDelegation() {
        return fullDelegation;
    }
    
    /** Setter for property fullDelegation.
     * @param fullDelegation New value of property fullDelegation.
     *
     */
    public void setFullDelegation(boolean fullDelegation) {
        this.fullDelegation = fullDelegation;
    }
    
}





