package org.dcache.util;

import org.eclipse.jetty.http.HttpSchemes;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.bio.SocketEndPoint;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.net.GssInputStream;
import org.globus.gsi.gssapi.net.GssOutputStream;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CDC;

import org.dcache.commons.util.NDC;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.dcache.util.Files.checkDirectory;
import static org.dcache.util.Files.checkFile;
import static org.globus.axis.gsi.GSIConstants.*;

/**
 * GSI Socket Connector for Jetty.
 *
 * Uses the JGlobus GSI implementation. The connector initializes the
 * following request attributes:
 *
 * * javax.servlet.request.X509Certificate
 * * org.globus.gsi.credentials
 * * org.globus.gsi.authorized.user.dn
 * * org.globus.gsi.context
 *
 */
public class JettyGSIConnector
    extends SocketConnector
{
    private static final Logger _log =
        LoggerFactory.getLogger(JettyGSIConnector.class);

    protected static final String MODE_SSL = "ssl";
    protected static final String MODE_GSI = "gsi";

    private GSSCredential _credentials;
    private TrustedCertificates _trustedCerts;
    private GSSManager _manager;

    // public static final String GSI_CONTEXT="org.gglobus.gsi.context";
    // public static final String GSI_USER_DN="org.globus.gsi.authorized.user.name";
    // public static final String GSI_CREDENTIALS="org.globus.gsi.credentials";



    private String _serverCert;
    private String _serverKey;
    private String _serverProxy;
    private String _caCertDir;
    private boolean _encrypt;
    private long _hostCertRefreshInterval = 12;
    private TimeUnit _hostCertRefreshIntervalUnit = HOURS;
    private long _trustAnchorRefreshInterval = 4;
    private TimeUnit _trustAnchorRefreshIntervalUnit = HOURS;
    private long _hostCertRefreshTimestamp = 0;
    private long _trustAnchorRefreshTimestamp = 0;

    private volatile boolean _autoFlush;
    private volatile boolean _requireClientAuth = true;
    private volatile boolean _acceptNoClientCerts = false;
    private volatile boolean _checkContextExpiration = false;
    private volatile boolean _rejectLimitedProxy = false;
    private volatile Integer _mode = GSIConstants.MODE_SSL;
    private volatile int _handshakeTimeout = 0; // 0 means use maxIdleTime
    private String[] _excludedCipherSuites = {};

    /**
     * Throws an IllegalStateException if the connector is open.
     */
    private void guardNotOpen()
        throws IllegalStateException
    {
        if (_serverSocket != null) {
            throw new IllegalStateException("Cannot change this parameter while the connector is open");
        }
    }

    /**
     * Gets the path to the host certificate.
     */
    public String getHostCertificatePath()
    {
        return _serverCert;
    }

    /**
     * Sets the path to the host certificate.
     */
    public void setHostCertificatePath(String serverCert) throws IOException
    {
        guardNotOpen();
        checkFile(serverCert);
        _serverCert = serverCert;
    }

    /**
     * Gets the path to the host key.
     */
    public String getHostKeyPath()
    {
        return _serverKey;
    }

    /**
     * Sets the path to the host key.
     */
    public void setHostKeyPath(String serverKey) throws IOException
    {
        guardNotOpen();
        checkFile(serverKey);
        _serverKey = serverKey;
    }

    /**
     * Gets the server proxy certificate path.
     */
    public String getProxy()
    {
        return _serverProxy;
    }

    /**
     * Sets the server proxy certificate path.
     */
    public void setProxy(String serverProxy) throws IOException
    {
        guardNotOpen();
        checkFile(serverProxy);
        _serverProxy = serverProxy;
    }

    /**
     * Gets the path to the CA certificate directory.
     */
    public String getCaCertificatePath()
    {
        return _caCertDir;
    }

    /**
     * Sets the path to the CA certificate directory.
     */
    public void setCaCertificatePath(String caCertDir) throws IOException
    {
        guardNotOpen();
        checkDirectory(caCertDir);
        _caCertDir = caCertDir;
    }

    public boolean getEncrypt()
    {
        return _encrypt;
    }

    public void setEncrypt(boolean encrypt)
    {
        guardNotOpen();
        _encrypt = encrypt;
    }

    public boolean getAutoFlush()
    {
        return _autoFlush;
    }

    public void setAutoFlush(boolean autoFlush)
    {
        _autoFlush = autoFlush;
    }

    public String getGssMode()
    {
        if (GSIConstants.MODE_GSI.equals(_mode)) {
            return MODE_GSI;
        } else {
            return MODE_SSL;
        }
    }

    public void setGssMode(String s)
    {
        if (s == null) {
            // assumes SSL as default
            _mode = GSIConstants.MODE_SSL;
        } else if (s.equalsIgnoreCase(MODE_SSL)) {
            _mode = GSIConstants.MODE_SSL;
        } else if (s.equalsIgnoreCase(MODE_GSI)) {
            _mode = GSIConstants.MODE_GSI;
        } else {
            throw new IllegalArgumentException("Unsupported mode: " + s);
        }
    }

    public boolean getRequireClientAuth()
    {
        return _requireClientAuth;
    }

    public void setRequireClientAuth(boolean requireClientAuth)
    {
        _requireClientAuth = requireClientAuth;
    }

    public boolean getAcceptNoClientCerts()
    {
        return _acceptNoClientCerts;
    }

    public void setAcceptNoClientCerts(boolean acceptNoClientCerts)
    {
        _acceptNoClientCerts = acceptNoClientCerts;
    }

    public boolean getCheckContextExpiration()
    {
        return _checkContextExpiration;
    }

    public void setCheckContextExpiration(boolean checkContextExpiration)
    {
        _checkContextExpiration = checkContextExpiration;
    }

    public boolean getRejectLimitedProxy()
    {
        return _rejectLimitedProxy;
    }

    public void setRejectLimitedProxy(boolean rejectLimitedProxy)
    {
        _rejectLimitedProxy = rejectLimitedProxy;
    }

    /**
     * Returns the handshake timeout.
     */
    public int getHandshakeTimeout()
    {
        return _handshakeTimeout;
    }

    /**
     * Set the time in milliseconds for so_timeout during ssl/gsi
     * handshaking
     *
     * @param msec a non-zero value will be used to set so_timeout during
     * ssl handshakes. A zero value means the maxIdleTime is used instead.
     */
    public void setHandshakeTimeout (int msec)
    {
        _handshakeTimeout = msec;
    }

    public void setExcludeCipherSuites(String[] cipherSuites)
    {
        _excludedCipherSuites = checkNotNull(cipherSuites);
    }

    protected ExtendedGSSContext createGSSContext()
        throws GSSException
    {
        ExtendedGSSContext context =
            (ExtendedGSSContext)_manager.createContext(_credentials);
        context.setOption(GSSConstants.GSS_MODE, _mode);
        context.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
                          _requireClientAuth);
        context.setOption(GSSConstants.ACCEPT_NO_CLIENT_CERTS,
                          _acceptNoClientCerts);
        context.setOption(GSSConstants.CHECK_CONTEXT_EXPIRATION,
                          _checkContextExpiration);
        context.setOption(GSSConstants.REJECT_LIMITED_PROXY,
                          _rejectLimitedProxy);

        // if (_trustedCerts != null) {
        //     context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
        //                       _trustedCerts);
        // }

        context.setBannedCiphers(_excludedCipherSuites);
        context.requestConf(_encrypt);
        return context;
    }

    /**
     * Read proxy/server certificates and create a credential for the GSI
     * connection from it.
     * @throws IOException Loading the credentials fails
     */
    private synchronized void loadServerCredentials() throws IOException {

        long timeSinceLastServerRefresh = (System.currentTimeMillis() -
                _hostCertRefreshTimestamp);

        try {
            if (_credentials == null || _manager == null ||
               (timeSinceLastServerRefresh >= _hostCertRefreshIntervalUnit.toMillis(_hostCertRefreshInterval))) {
                    _log.info("Time since last server cert refresh {}",
                              timeSinceLastServerRefresh);
                    _log.info("Loading server certificates. Current refresh interval: {} {}",
                              _hostCertRefreshInterval, _hostCertRefreshIntervalUnit);

                    X509Credential cred;
                    if (_serverProxy != null && !_serverProxy.equals("")) {
                        _log.info("Server Proxy: {}", _serverProxy);
                        cred = new X509Credential(_serverProxy);
                    } else if (_serverCert != null && _serverKey != null) {
                        _log.info("Server Certificate: {}", _serverCert);
                        _log.info("Server Key: {}", _serverKey);
                        cred = new X509Credential(_serverCert, _serverKey);
                    } else {
                        throw new IllegalStateException("Server credentials" +
                                                        "have not been configured");
                    }

                    _credentials =
                        new GlobusGSSCredentialImpl(cred, GSSCredential.ACCEPT_ONLY);


                    _manager = ExtendedGSSManager.getInstance();
                    _hostCertRefreshTimestamp = System.currentTimeMillis();
            }
        } catch (CredentialException | GSSException e) {
            throw new IOException("Failed to load credentials", e);
        }
    }

    /**
     * Reload the trusted certificates from the position specified in
     * caCertDir
     */
    private synchronized void loadTrustAnchors()
    {
        long timeSinceLastTARefresh = (System.currentTimeMillis() -
                _trustAnchorRefreshTimestamp);

        if (_caCertDir != null && (_trustedCerts == null ||
                (timeSinceLastTARefresh >= _trustAnchorRefreshIntervalUnit.toMillis(_trustAnchorRefreshInterval)))) {
            _log.info("Time since last TA Refresh {}", timeSinceLastTARefresh);
            _log.info("Loading trust anchors. Current refresh interval: {} {}",
                   _trustAnchorRefreshInterval, _trustAnchorRefreshIntervalUnit);
            _log.info("CA certificate directory: {}", _caCertDir);
            _trustedCerts = TrustedCertificates.load(_caCertDir);
            _trustAnchorRefreshTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * Accepts and dispatches a new connection. Loads/reloads the server
     * credentials and the trusted certificates if they are not
     * present or the refresh interval has expired.
     */
    @Override
    public void accept(int acceptorID)
        throws IOException, InterruptedException
    {
        Socket socket = _serverSocket.accept();

        try {
            loadServerCredentials();
            loadTrustAnchors();

            configure(socket);

            GsiSocket gsiSocket = new GsiSocket(socket, createGSSContext());
            gsiSocket.setUseClientMode(false);
            gsiSocket.setAuthorization(null);
            gsiSocket.setAutoFlush(_autoFlush);

            ConnectorEndPoint connection = new GsiConnection(gsiSocket);
            connection.dispatch();
        } catch (GSSException e) {
            _log.error("Failed to initialize GSS Context: " , e);
            throw new IOException("Failed to initialize GSS context", e);
        } catch (IOException e) {
            _log.warn("Failed to accept connection: " + e);
            throw e;
        }
    }

    /**
     * By default, we're confidential when encryption is enabled. But,
     * if we've been told about an confidential port, and said port is
     * not our port, then we're not. This allows separation of
     * listeners providing INTEGRAL versus CONFIDENTIAL constraints,
     * such as one SSL listener configured to require client certs
     * providing CONFIDENTIAL, whereas another SSL listener not
     * requiring client certs providing mere INTEGRAL constraints.
     */
    @Override
    public boolean isConfidential(Request request)
    {
        final int confidentialPort = getConfidentialPort();
        return (confidentialPort == 0 || confidentialPort == request.getServerPort()) && _encrypt;
    }

    /**
     * By default, we're integral when encryption is enabled. But, if
     * we've been told about an integral port, and said port is not
     * our port, then we're not. This allows separation of listeners
     * providing INTEGRAL versus CONFIDENTIAL constraints, such as one
     * SSL listener configured to require client certs providing
     * CONFIDENTIAL, whereas another SSL listener not requiring client
     * certs providing mere INTEGRAL constraints.
     */
    @Override
    public boolean isIntegral(Request request)
    {
        final int integralPort = getIntegralPort();
        return (integralPort == 0 || integralPort == request.getServerPort()) && _encrypt;
    }

    /**
     * Allow the Listener a chance to customise the request. before
     * the server does its stuff.
     *
     * This allows the required attributes to be set for SSL
     * requests.
     *
     * The requirements of the Servlet specs are:
     *
     * * an attribute named "javax.servlet.request.ssl_id" of type
     *   String (since Spec 3.0).
     * * an attribute named "javax.servlet.request.cipher_suite" of
     *   type String.
     * * an attribute named "javax.servlet.request.key_size" of type
     *   Integer.
     * * an attribute named "javax.servlet.request.X509Certificate" of
     *   type java.security.cert.X509Certificate[]. This is an array of
     *   objects of type X509Certificate, the order of this array is
     *   defined as being in ascending order of trust. The first
     *   certificate in the chain is the one set by the client, the
     *   next is the one used to authenticate the first, and so on.
     *
     * Of those we currently only support
     * javax.servlet.request.X509Certificate. In addition we add
     *
     * * an attribute named "org.globus.gsi.credentials" of type
     *   GSSCredentials.
     * * an attribute named "org.globus.gsi.authorized.user.dn" of
     *   type String.
     * * an attribute named "org.globus.gsi.context" of type GSSContext.
     *
     * @param endpoint The Socket the request arrived on.  This should
     *        be a {@link SocketEndPoint} wrapping a {@link
     *        GsiSocket}.
     * @param request HttpRequest to be customised.
     */
    @Override
    public void customize(EndPoint endpoint, Request request)
        throws IOException
    {
        super.customize(endpoint, request);
        request.setScheme(HttpSchemes.HTTPS);

        SocketEndPoint socketEndPoint = (SocketEndPoint) endpoint;
        GsiSocket gsiSocket = (GsiSocket)socketEndPoint.getTransport();

        try {
            ExtendedGSSContext context =
                (ExtendedGSSContext) gsiSocket.getContext();

            /* The certificate chain.
             */
            X509Certificate[] chain =
                (X509Certificate[]) context.inquireByOid(GSSConstants.X509_CERT_CHAIN);
            if (chain != null) {
                request.setAttribute("javax.servlet.request.X509Certificate", chain);
            } else if (_requireClientAuth && !_acceptNoClientCerts) {
                // Sanity check
                throw new IllegalStateException("no client auth");
            }

            /* The GSS context.
             */
            request.setAttribute(GSI_CONTEXT, context);

            /* The DN.
             */
            String dn = context.getSrcName().toString();
            if (dn != null) {
                request.setAttribute(GSI_USER_DN, dn);
            }

            /* The delegated credentials.
             */
            GSSCredential delegated = context.getDelegCred();
            if (delegated != null) {
                request.setAttribute(GSI_CREDENTIALS, delegated);
            }
        } catch (GSSException e) {
            throw new IOException("Failed to retrieve context properties", e);
        }
    }

    public void setHostCertRefreshInterval(int value)
    {
        _hostCertRefreshInterval = value;
    }

    public void setHostCertRefreshIntervalUnit(TimeUnit unit)
    {
        _hostCertRefreshIntervalUnit = unit;
    }

    public void setTrustAnchorRefreshInterval(int value)
    {
        _trustAnchorRefreshInterval = value;
    }

    public void setTrustAnchorRefreshIntervalUnit(TimeUnit unit)
    {
        _trustAnchorRefreshIntervalUnit = unit;
    }

    public class GsiConnection extends ConnectorEndPoint
    {
        public GsiConnection(Socket socket) throws IOException
        {
            super(socket);
        }

        @Override
        public void run()
        {
            try (CDC ignored = new CDC()) {
                try {
                    NDC.push(_socket.getInetAddress().getHostAddress() + ":" +
                            _socket.getPort());
                    int handshakeTimeout = getHandshakeTimeout();
                    int oldTimeout = _socket.getSoTimeout();
                    if (handshakeTimeout > 0) {
                        _socket.setSoTimeout(handshakeTimeout);
                    }

                    GsiSocket gsiSocket = (GsiSocket) _socket;
                    gsiSocket.startHandshake();

                    if (handshakeTimeout > 0) {
                        _socket.setSoTimeout(oldTimeout);
                    }

                    super.run();
                } catch (EOFException ignoredException) {
                    _log.debug("Client disconnected while establishing " +
                            "secure connection");
                } catch (IOException e) {
                    _log.warn("Problem while establishing secure connection: {}",
                            e.toString());
                    try {
                        close();
                    } catch (IOException e2) {
                        _log.warn("Failed to close local socket: {}", e2.toString());
                    }
                }
            }
        }
    }

    /**
     * The JGlobus GSIGssSocket class performs the handshake as a side
     * effect of creating the input and output streams. This presents
     * a problem for the JettyGSIConnector, as Jetty pulls the streams
     * in the accept thread. We would however like to postpone the
     * handshake till the IO processing thread.
     *
     * The GsiSocket wrapper class wraps the input and output streams
     * such that we do not call getInputStream and getOutputStream of
     * GSIGssSocket until we really have to. This allows us to delay
     * the handshake.
     *
     * If JettyGSIConnector is ever integrated into JGlobus-FX, then
     * we should try to fix GSIGssSocket rather than wrapping it here.
     *
     * It is worth mentioning that Sun's SSLSocket does not perform
     * the handshake as part of stream creation: It postpones the
     * handshake until the first read or write call.
     */
    private static class GsiSocket extends GSIGssSocket
    {
        private InputStream _in;
        private OutputStream _out;
        private boolean _autoFlush;

        public GsiSocket(Socket socket, GSSContext context)
        {
            super(socket, context);

            _in = new GsiInputStream();
            _out = new GsiOutputStream();
        }

        @Override
        public SocketAddress getRemoteSocketAddress()
        {
            return getWrappedSocket().getRemoteSocketAddress();
        }

        @Override
        public OutputStream getOutputStream()
            throws IOException
        {
            return _out;
        }

        @Override
        public InputStream getInputStream()
            throws IOException
        {
            return _in;
        }

        public void setAutoFlush(boolean autoFlush)
        {
            _autoFlush = autoFlush;
        }

        public boolean getAutoFlush()
        {
            return _autoFlush;
        }

        private class GsiInputStream extends InputStream
        {
            private GssInputStream in;

            private GssInputStream getInputStream()
                throws IOException
            {
                if (in == null) {
                    in = (GssInputStream) GsiSocket.super.getInputStream();
                }
                return in;
            }

            @Override
            public int available()
                throws IOException
            {
                return getInputStream().available();
            }

            @Override
            public void close()
                throws IOException
            {
                getInputStream().close();
            }

            @Override
            public int read()
                throws IOException
            {
                return getInputStream().read();
            }

            @Override
            public int read(byte[] b)
                throws IOException
            {
                return getInputStream().read(b);
            }

            @Override
            public int read(byte[] b, int off, int len)
                throws IOException
            {
                return getInputStream().read(b, off, len);
            }

            @Override
            public long skip(long n)
                throws IOException
            {
                return getInputStream().skip(n);
            }
        }

        private class GsiOutputStream extends OutputStream
        {
            private GssOutputStream out;

            private GssOutputStream getOutputStream()
                throws IOException
            {
                if (out == null) {
                    out = (GssOutputStream) GsiSocket.super.getOutputStream();
                    out.setAutoFlush(_autoFlush);
                }
                return out;
            }

            @Override
            public void close()
                throws IOException
            {
                getOutputStream().close();
            }

            @Override
            public void flush()
                throws IOException
            {
                getOutputStream().flush();
            }

            @Override
            public void write(byte[] b)
                throws IOException
            {
                getOutputStream().write(b);
            }

            @Override
            public void write(byte[] b, int off, int len)
                throws IOException
            {
                getOutputStream().write(b, off, len);
            }

            @Override
            public void write(int b)
                throws IOException
            {
                getOutputStream().write(b);
            }
        }
    }
}
