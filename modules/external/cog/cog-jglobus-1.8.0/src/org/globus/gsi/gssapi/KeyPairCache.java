package org.globus.gsi.gssapi;

import java.util.Map;
import java.util.Hashtable;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.common.CoGProperties;

/**
 * Simple cache for key pairs. The cache is used to avoid excessive
 * CPU consumption from key pair generation. In particular for
 * purposes of delegation, reusing a key pair is safe.
 * 
 * @author Gerd Behrmann (behrmann@ndgf.org)
 */
public class KeyPairCache {

    static class KeyPairCacheEntry {
        private long created_at;
        private KeyPair keys;

        public KeyPairCacheEntry(KeyPair keys, long created_at) {
            this.keys = keys;
            this.created_at = created_at;
        }
	
        public long getCreatedAt() {
            return created_at;
        }
	
        public KeyPair getKeyPair() {
            return keys;
        }
    }

    private static Log logger = 
        LogFactory.getLog(GlobusGSSContextImpl.class.getName());

    public static final String DEFAULT_ALGORITHM = "RSA";
    public static final String DEFAULT_PROVIDER = "BC";

    private final String algorithm;
    private final String provider;
    private final long lifetime;
    private static KeyPairCache keyPairCache;

    /** 
     * Hash table of cache entries. The use of <code>Hashtable</code>
     * is significant, since we rely on access to the table being
     * synchronized.
     */
    private final Map entries = new Hashtable();

    /**
     * Creates a KeyPairCache object for the specified algorithm, as
     * supplied from the specified provider.
     *
     * @param algorithm the standard string name of the algorithm. See
     * Appendix A in the Java Cryptography Architecture API
     * Specification &amp; Reference for information about standard
     * algorithm names.
     * @param provider the string name of the provider. 
     * @param lifetime the lifetime of the cache in milliseconds.
     */
    private KeyPairCache(String algorithm, String provider, long lifetime) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.lifetime = lifetime;
    }

    public static synchronized KeyPairCache getKeyPairCache() {

        if (keyPairCache == null) {
            keyPairCache = new KeyPairCache(DEFAULT_ALGORITHM, DEFAULT_PROVIDER,
                                            CoGProperties.getDefault().
                                                getDelegationKeyCacheLifetime());
        }
        return keyPairCache;
    }

    public static synchronized KeyPairCache getKeyPairCache(String algorithm,
                                                            String provider,
                                                            long lifetime) {

        if (keyPairCache == null) {
            keyPairCache = new KeyPairCache(algorithm, provider, lifetime);
        }

        return keyPairCache;
    }

    /**
     * Returns a key pair of size <code>bits</code>. The same key pair
     * may be returned several times within a period of the cache
     * lifetime.
     *
     * If lifetime was set to zero or less than zero, no keys are cached.
     *
     * @param bits the keysize. This is an algorithm-specific metric,
     * such as modulus length, specified in number of bits.
     * @throws NoSuchAlgorithmException if the algorithm is not
     * available in the environment.
     * @throws NoSuchProviderException if the provider is not
     * available in the environment.
     */
    public KeyPair getKeyPair(int bits)
        throws NoSuchAlgorithmException, NoSuchProviderException {

        if (this.lifetime < 1) {

            logger.debug("Cache lifetime is less than 1, generating new " +
                         "keypair each time");
            KeyPairGenerator generator =
                KeyPairGenerator.getInstance(this.algorithm, this.provider);
            generator.initialize(bits);
            return generator.generateKeyPair();
        }

        long st = System.currentTimeMillis();
        Integer keysize = new Integer(bits);
        KeyPairCacheEntry entry = (KeyPairCacheEntry)entries.get(keysize);
        if (entry == null || st - entry.getCreatedAt() >= lifetime) {
            logger.debug("Creating " + bits + " bits keypair");

            KeyPairGenerator generator = 
                KeyPairGenerator.getInstance(algorithm, provider);
            generator.initialize(bits);
            logger.debug("Time to generate key pair: " + 
                         (System.currentTimeMillis() - st));
            
            entry = new KeyPairCacheEntry(generator.generateKeyPair(), st);
            entries.put(keysize, entry);
        }
        return entry.getKeyPair();
    }
}