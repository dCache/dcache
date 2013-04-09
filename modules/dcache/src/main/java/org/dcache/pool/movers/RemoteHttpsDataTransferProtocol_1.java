package org.dcache.pool.movers;

import com.google.common.base.Throwables;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.globus.gsi.provider.GlobusProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStore.LoadStoreParameter;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Properties;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.cells.nucleus.Environments;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

import static org.globus.gsi.provider.KeyStoreParametersFactory.createTrustStoreParameters;

/**
 *  A mover for transferring a file using HTTP over a TLS/SSL connection.
 */
public class RemoteHttpsDataTransferProtocol_1 extends RemoteHttpDataTransferProtocol_1 implements EnvironmentAware
{
    // Cached values of our trust stores
    private static KeyStore trustStore;

    private PrivateKey privateKey;
    private X509Certificate[] chain;
    private String _caPath;

    static {
        Security.addProvider(new GlobusProvider());
    }

    // constructor needed by Pool mover contract.
    public RemoteHttpsDataTransferProtocol_1(CellEndpoint cell)
    {
        super(cell);
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        Properties config = Environments.toProperties(environment);
        _caPath = getRequiredProperty(config, "pool.authn.capath");
    }


    private static String getRequiredProperty(Properties config,
            String key)
    {
        String value = config.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Required property '" + key +
                    "' not found");
        }
        return value;
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

    private KeyStore buildTrustStore() throws KeyStoreException, IOException, CertificateException
    {
        try {
            KeyStore store = KeyStore.getInstance(GlobusProvider.KEYSTORE_TYPE, GlobusProvider.PROVIDER_NAME);
            LoadStoreParameter param = createTrustStoreParameters(_caPath + "/*.[0-9]");
            store.load(param);
            if (store.size() == 0) {
                throw new IOException("Failed to load any CA certificates: " + _caPath);
            }
            return store;
        } catch (NoSuchProviderException | NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }
    }

    private synchronized KeyStore getTrustStore() throws KeyStoreException, IOException, CertificateException
    {
        if (trustStore == null) {
            trustStore = buildTrustStore();
        }

        return trustStore;
    }

    private KeyStore buildKeyStore(char[] password) throws KeyStoreException
    {
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());

        try {
            store.load(null); // null => initialise as empty KeyStore
        } catch (IOException | CertificateException | NoSuchAlgorithmException e) {
            Throwables.propagate(e);
        }

        if (privateKey != null && chain != null) {
            store.setKeyEntry("client credential", privateKey, password, chain);
        }

        return store;
    }

    @Override
    public CloseableHttpClient createHttpClient() throws CacheException
    {
        char[] password = "too-many-secrets".toCharArray(); // Dummy value to satisfy JSSE.

        try {
            SSLContext context = SSLContexts.custom().
                    loadKeyMaterial(buildKeyStore(password), password).
                    loadTrustMaterial(getTrustStore()).
                    build();

            return HttpClients.custom().setUserAgent(USER_AGENT).setSslcontext(context).build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException
                | UnrecoverableKeyException | CertificateException | IOException e) {
            throw new CacheException("failed to build http client: " +
                    e.getMessage(), e);
        }
    }
}
