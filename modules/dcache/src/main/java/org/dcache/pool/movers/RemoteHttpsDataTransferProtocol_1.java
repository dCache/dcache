package org.dcache.pool.movers;

import eu.emi.security.authn.x509.X509CertChainValidator;
import eu.emi.security.authn.x509.helpers.ssl.SSLTrustManager;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

/**
 * A mover for transferring a file using HTTP over a TLS/SSL connection.
 */
public class RemoteHttpsDataTransferProtocol_1 extends RemoteHttpDataTransferProtocol_1
{
    private final TrustManager trustManager;
    private final SecureRandom secureRandom;

    private PrivateKey privateKey;
    private X509Certificate[] chain;

    public RemoteHttpsDataTransferProtocol_1(CellEndpoint cell, X509CertChainValidator validator,
                                             SecureRandom secureRandom)
    {
        super(cell);
        this.secureRandom = secureRandom;
        this.trustManager = new SSLTrustManager(validator);
    }

    @Override
    public void runIO(FileAttributes attributes, RepositoryChannel channel,
                      ProtocolInfo genericInfo, Allocator allocator, IoMode access)
            throws CacheException, IOException, InterruptedException
    {
        RemoteHttpsDataTransferProtocolInfo info =
                (RemoteHttpsDataTransferProtocolInfo) genericInfo;
        privateKey = info.getPrivateKey();
        chain = info.getCertificateChain();
        super.runIO(attributes, channel, genericInfo, allocator, access);
    }

    @Override
    public CloseableHttpClient createHttpClient() throws CacheException
    {
        try {
            KeyAndCertCredential credential = new KeyAndCertCredential(privateKey, chain);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(
                    new KeyManager[]{credential.getKeyManager()},
                    new TrustManager[]{trustManager},
                    secureRandom);
            return HttpClients.custom().setUserAgent(USER_AGENT).setSSLContext(context).build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new CacheException("failed to build http client: " + e.getMessage(), e);
        }
    }
}
