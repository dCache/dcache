package org.dcache.ssl;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.Callable;

import org.dcache.util.Args;
import org.dcache.util.Crypto;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.toArray;
import static java.util.Arrays.asList;

public class CanlSslSocketCreator extends SocketFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanlSslSocketCreator.class);

    private static final String SERVICE_KEY = "service_key";
    private static final String SERVICE_CERT = "service_cert";
    private static final String SERVICE_TRUSTED_CERTS = "service_trusted_certs";
    private static final String CIPHER_FLAGS = "ciphers";
    private static final String CRL_MODE = "crl-mode";
    private static final String OCSP_MODE = "ocsp-mode";
    private static final Set<String> bannedProtocols = ImmutableSet.of("SSL", "SSLv2", "SSLv2Hello", "SSLv3");

    private final Set<String> bannedCiphers;
    private final Callable<SSLContext> factory;

    public CanlSslSocketCreator(String arguments) throws IOException
    {
        this(new Args(arguments));
    }

    public CanlSslSocketCreator(Args args) throws IOException
    {
        this(new File(args.getOption(SERVICE_KEY)),
             new File(args.getOption(SERVICE_CERT)),
             new File(args.getOption(SERVICE_TRUSTED_CERTS)),
             Crypto.getBannedCipherSuitesFromConfigurationValue(args.getOption(CIPHER_FLAGS)),
             CrlCheckingMode.valueOf(args.getOption(CRL_MODE)),
             OCSPCheckingMode.valueOf(args.getOption(OCSP_MODE)));
    }

    public CanlSslSocketCreator(File keyPath,
                                File certPath,
                                File caPath,
                                String[] bannedCiphers,
                                CrlCheckingMode crlMode,
                                OCSPCheckingMode ocspMode) throws IOException
    {
        try {
            LOGGER.info("service_key {}", keyPath);
            LOGGER.info("service_certs {}", certPath);
            LOGGER.info("service_trusted_certs {}", caPath);

            this.bannedCiphers = ImmutableSet.copyOf(bannedCiphers);

            factory = CanlContextFactory.custom()
                                        .withCertificateAuthorityPath(caPath.toPath())
                                        .withCrlCheckingMode(crlMode)
                                        .withOcspCheckingMode(ocspMode)
                                        .withKeyPath(keyPath.toPath())
                                        .withCertificatePath(certPath.toPath())
                                        .withLazy(false)
                                        .buildWithCaching();
            factory.call();
        } catch (Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Failed to create CanlSslSocketCreator: " + e.getMessage(), e);
        }
    }

    @Override
    public Socket createSocket() throws IOException
    {
        SSLSocket socket = (SSLSocket) getSocketFactory().createSocket();
        setCipherSuiteAndProtocol(socket);
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException
    {
        SSLSocket socket = (SSLSocket) getSocketFactory().createSocket(host, port);
        setCipherSuiteAndProtocol(socket);
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddr, int localPort)
            throws IOException
    {
        SSLSocket socket = (SSLSocket) getSocketFactory().createSocket(host, port, localAddr, localPort);
        setCipherSuiteAndProtocol(socket);
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int port) throws IOException
    {
        SSLSocket socket = (SSLSocket) getSocketFactory().createSocket(inetAddress, port);
        setCipherSuiteAndProtocol(socket);
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddr, int localPort) throws IOException
    {
        SSLSocket socket = (SSLSocket) getSocketFactory().createSocket(address, port, localAddr, localPort);
        setCipherSuiteAndProtocol(socket);
        return socket;
    }

    private void setCipherSuiteAndProtocol(SSLSocket socket) {
        String[] cipherSuites = toArray(filter(asList(socket.getSupportedCipherSuites()),
                                               not(in(bannedCiphers))), String.class);
        String[] protocols = toArray(filter(asList(socket.getSupportedProtocols()),
                                            not(in(bannedProtocols))), String.class);
        socket.setEnabledCipherSuites(cipherSuites);
        socket.setEnabledProtocols(protocols);
        socket.setUseClientMode(true);
    }

    public SSLSocketFactory getSocketFactory() throws IOException
    {
        try {
            return factory.call().getSocketFactory();
        } catch(Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Failed to create SSL Server Socket Factory: " + e.getMessage(), e);
        }
    }
}
