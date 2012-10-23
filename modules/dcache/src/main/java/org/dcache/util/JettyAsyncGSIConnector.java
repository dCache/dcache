package org.dcache.util;

import java.io.IOException;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.Buffers;
import org.eclipse.jetty.io.Buffers.Type;
import org.eclipse.jetty.io.BuffersFactory;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.bio.SocketEndPoint;
import org.eclipse.jetty.io.nio.DirectNIOBuffer;
import org.eclipse.jetty.io.nio.SelectChannelEndPoint;
import org.eclipse.jetty.io.nio.SelectorManager.SelectSet;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.http.HttpParser;
import org.eclipse.jetty.http.HttpSchemes;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.globus.axis.gsi.GSIConstants.*;
import static org.dcache.util.Files.checkFile;
import static org.dcache.util.Files.checkDirectory;

/**
 * @author tzangerl
 *
 */
public class JettyAsyncGSIConnector extends SelectChannelConnector
{
    private static Logger _logger =
        LoggerFactory.getLogger(JettyAsyncGSIConnector.class);

    protected static final String MODE_SSL = "ssl";
    protected static final String MODE_GSI = "gsi";
    private static final long DEFAULT_HOST_CERT_REFRESH_INTERVAL =
        TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);
    private static final long DEFAULT_TRUST_ANCHOR_REFRESH_INTERVAL =
        TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS);
    private static final int IN_BUFFER_SIZE = 16384;

    /** credentials cached for all connections */
    private static GSSCredential _credentials;
    private static GSSManager _gssManager;
    private static TrustedCertificates _trustedCerts;

    private String _serverCert;
    private String _serverKey;
    private String _serverProxy;
    private String _caCertDir;
    private boolean _encrypt;
    private long _hostCertRefreshInterval;
    private long _trustAnchorRefreshInterval;

    private volatile boolean _autoFlush;
    private volatile boolean _requireClientAuth = true;
    private volatile boolean _acceptNoClientCerts = false;
    private volatile boolean _checkContextExpiration = false;
    private volatile boolean _rejectLimitedProxy = false;
    private volatile Integer _mode = GSIConstants.MODE_GSI;

    private long _hostCertRefreshTimestamp;
    private long _trustAnchorRefreshTimestamp;
    private Buffers _gsiBuffers;

    /**
     * Assing default values to the certificate refresh intervals
     */
    public JettyAsyncGSIConnector()
    {
        _hostCertRefreshInterval = DEFAULT_HOST_CERT_REFRESH_INTERVAL;
        _trustAnchorRefreshInterval = DEFAULT_TRUST_ANCHOR_REFRESH_INTERVAL;
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
     * @param endpoint The multiplexable select-channel the request arrived on.
     *        This should
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

        GSISelectChannelEndPoint selectChannelEndpoint =
                                    (GSISelectChannelEndPoint) endpoint;
        ExtendedGSSContext context =
            (ExtendedGSSContext) selectChannelEndpoint.getContext();

        try {

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

    /* ---------------------------------------------------------- */
    private void loadServerCredentials() throws IOException {
        try {
            _logger.debug("Loading credentials");

            GlobusCredential cred;
            if (_serverProxy != null && !_serverProxy.equals("")) {
                _logger.info("Server Proxy: {}", _serverProxy);
                cred = new GlobusCredential(_serverProxy);
            } else if (_serverCert != null && _serverKey != null) {
                _logger.info("Server Certificate: {}", _serverCert);
                _logger.info("Server Key: {}", _serverKey);
                cred = new GlobusCredential(_serverCert, _serverKey);
            } else {
                throw new IllegalStateException("Server credentials have not been configured");
            }

            _credentials =
                new GlobusGSSCredentialImpl(cred, GSSCredential.ACCEPT_ONLY);
        } catch (GlobusCredentialException | GSSException e) {
            throw new IOException("Failed to load credentials", e);
        }

        _gssManager = ExtendedGSSManager.getInstance();
    }

    /**
     * Reload the trusted certificates from the position specified in
     * caCertDir
     */
    private void loadTrustAnchors()
    {
        if (_caCertDir != null) {
            _logger.info("CA certificate directory: {}", _caCertDir);
            _trustedCerts = TrustedCertificates.load(_caCertDir);
        }
    }

    protected ExtendedGSSContext createGSSContext() throws GSSException {
        ExtendedGSSContext context =
            (ExtendedGSSContext)_gssManager.createContext(_credentials);
        context.setOption(GSSConstants.GSS_MODE, _mode);
        context.setOption(GSSConstants.REQUIRE_CLIENT_AUTH,
                          _requireClientAuth);
        context.setOption(GSSConstants.ACCEPT_NO_CLIENT_CERTS,
                          _acceptNoClientCerts);
        context.setOption(GSSConstants.CHECK_CONTEXT_EXPIRATION,
                          _checkContextExpiration);
        context.setOption(GSSConstants.REJECT_LIMITED_PROXY,
                          _rejectLimitedProxy);

        if (_trustedCerts != null) {
            context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
                              _trustedCerts);
        }

        context.requestConf(_encrypt);
        return context;
    }

    @Override
    protected void doStart() throws Exception
    {
        Type type = getUseDirectBuffers() ? Type.DIRECT : Type.INDIRECT;
        _gsiBuffers = BuffersFactory.newBuffers(type, IN_BUFFER_SIZE,
                                                type, IN_BUFFER_SIZE,
                                                type, getMaxBuffers());
        super.doStart();
    }

    @Override
    protected SelectChannelEndPoint newEndPoint(SocketChannel channel, SelectSet selectSet, SelectionKey key) throws IOException
    {
        _logger.info("Trying to create a GSS context!");
        GSSContext context;

        try {

            synchronized(this) {
                long timeSinceLastServerRefresh = (System.currentTimeMillis() -
                                                  _hostCertRefreshTimestamp);
                long timeSinceLastTARefresh = (System.currentTimeMillis() -
                                                  _trustAnchorRefreshTimestamp);

                if (_trustedCerts == null || (timeSinceLastTARefresh >=
                            _trustAnchorRefreshInterval)) {

                    _logger.info("Time since last TA Refresh {}", timeSinceLastTARefresh);
                    _logger.info("Loading trust anchors. Current refresh interval: {} ms",
                                 _trustAnchorRefreshInterval);
                    loadTrustAnchors();
                    _trustAnchorRefreshTimestamp = System.currentTimeMillis();
                }

                if (_credentials == null || _gssManager == null ||
                        (timeSinceLastServerRefresh >= _hostCertRefreshInterval)) {
                    _logger.info("Time since last server cert refresh {}", timeSinceLastServerRefresh);
                    /* reload the server credentials */
                    _logger.info("Loading server certificates. Current refresh interval: {} ms",
                                 _hostCertRefreshInterval);

                    loadServerCredentials();
                    _hostCertRefreshTimestamp = System.currentTimeMillis();
                }
            }

            context = createGSSContext();

        } catch (GSSException gssex) {
            _logger.warn("Failed to initialize GSS Context: {}", gssex);
            throw new IOException("Failed to initialize GSS context", gssex);
        }

        GSISelectChannelEndPoint endp = new GSISelectChannelEndPoint(_gsiBuffers,channel,selectSet,key, context);
        return endp;
    }

    /* ------------------------------------------------------------------------------- */
    @Override
    protected Connection newConnection(SocketChannel channel, SelectChannelEndPoint endpoint)
    {
        HttpConnection connection=(HttpConnection)super.newConnection(channel,endpoint);
        ((HttpParser)connection.getParser()).setForceContentBuffer(true);
        return connection;
    }

    /* ------------------------------------------------------------ */
    /**
     * By default, we're confidential, given we speak SSL. But, if we've been
     * told about an confidential port, and said port is not our port, then
     * we're not. This allows separation of listeners providing INTEGRAL versus
     * CONFIDENTIAL constraints, such as one SSL listener configured to require
     * client certs providing CONFIDENTIAL, whereas another SSL listener not
     * requiring client certs providing mere INTEGRAL constraints.
     */
    @Override
    public boolean isConfidential(Request request)
    {
        final int confidentialPort=getConfidentialPort();
        return confidentialPort==0||confidentialPort==request.getServerPort();
    }

    /* ------------------------------------------------------------ */
    /**
     * By default, we're integral, given we speak SSL. But, if we've been told
     * about an integral port, and said port is not our port, then we're not.
     * This allows separation of listeners providing INTEGRAL versus
     * CONFIDENTIAL constraints, such as one SSL listener configured to require
     * client certs providing CONFIDENTIAL, whereas another SSL listener not
     * requiring client certs providing mere INTEGRAL constraints.
     */
    @Override
    public boolean isIntegral(Request request)
    {
        final int integralPort=getIntegralPort();
        return integralPort==0||integralPort==request.getServerPort();
    }

    /* setters needed for spring */
    public void setAcceptNoClientCerts(boolean acceptNoClientCerts)
    {
        _acceptNoClientCerts = acceptNoClientCerts;
    }

    public void setCheckContextExpiration(boolean checkContextExpiration)
    {
        _checkContextExpiration = checkContextExpiration;
    }

    public void setRejectLimitedProxy(boolean rejectLimitedProxy)
    {
        _rejectLimitedProxy = rejectLimitedProxy;
    }

    public void setRequireClientAuth(boolean requireClientAuth)
    {
        _requireClientAuth = requireClientAuth;
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
        checkDirectory(caCertDir);
        _caCertDir = caCertDir;
    }

    public boolean getEncrypt()
    {
        return _encrypt;
    }

    /**
     * Set encryption. Applies only to endpoints instantiated after
     * invoking this.
     * @param encrypt
     */
    public void setEncrypt(boolean encrypt)
    {
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

    public void setMillisecBetweenHostCertRefresh(int ms)
    {
        _hostCertRefreshInterval = ms;
    }

    public void setMillisecBetweenTrustAnchorRefresh(int ms)
    {
        _trustAnchorRefreshInterval = ms;
    }
}
