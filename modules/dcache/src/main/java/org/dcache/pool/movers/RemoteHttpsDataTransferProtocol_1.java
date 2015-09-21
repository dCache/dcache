package org.dcache.pool.movers;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.globus.gsi.provider.GlobusProvider;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStore.LoadStoreParameter;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RemoteHttpsDataTransferProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.EnvironmentAware;
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
    private static final LoadingCache<String,KeyStore> trustStoreCache =
            CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(
                    new CacheLoader<String, KeyStore>()
                    {
                        @Override
                        public KeyStore load(String path) throws Exception
                        {
                            KeyStore store = KeyStore.getInstance(GlobusProvider.KEYSTORE_TYPE, GlobusProvider.PROVIDER_NAME);
                            LoadStoreParameter param = createTrustStoreParameters(path + "/*.[0-9]");
                            store.load(param);
                            if (store.size() == 0) {
                                throw new IOException("Failed to load any CA certificates: " + path);
                            }
                            return store;
                        }
                    }
            );

    static {
        Security.addProvider(new GlobusProvider());
    }

    private PrivateKey privateKey;
    private X509Certificate[] chain;

    private String caPath;

    // constructor needed by Pool mover contract.
    public RemoteHttpsDataTransferProtocol_1(CellEndpoint cell)
    {
        super(cell);
    }

    @Override
    public void setEnvironment(final Map<String,Object> environment)
    {
        Replaceable replaceable = new Replaceable() {
            @Override
            public String getReplacement(String name)
            {
                Object value =  environment.get(name);
                return (value == null) ? null : value.toString().trim();
            }
        };
        String value = Objects.toString(environment.get("pool.authn.capath"), null);
        if (value == null) {
            throw new IllegalArgumentException("Required property 'pool.authn.capath' not found");
        }
        caPath = Formats.replaceKeywords(value, replaceable);
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

    private KeyStore getTrustStore() throws KeyStoreException, IOException, CertificateException
    {
        try {
            return trustStoreCache.get(caPath);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.propagateIfInstanceOf(cause, KeyStoreException.class);
            Throwables.propagateIfInstanceOf(cause, IOException.class);
            throw Throwables.propagate(e);
        }
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
                    loadTrustMaterial(getTrustStore(), null).
                    build();

            return HttpClients.custom().setUserAgent(USER_AGENT).setSSLContext(context).build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException
                | UnrecoverableKeyException | CertificateException | IOException e) {
            throw new CacheException("failed to build http client: " +
                    e.getMessage(), e);
        }
    }
}
