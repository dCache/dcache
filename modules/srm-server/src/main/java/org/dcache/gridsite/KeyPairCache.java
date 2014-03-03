/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gridsite;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A source of KeyPairs, where successive requests for the same length will
 * return the same KeyPair.  This class takes inspiration from
 * org.globus.gsi.gssapi.KeyPairCache from JGlobus but uses Guava's Cache
 * support rather than implementing a custom cache.
 */
public class KeyPairCache
{
    private static final Logger LOG = LoggerFactory.getLogger(KeyPairCache.class);

    private static final String DEFAULT_ALGORITHM = "RSA";
    private static final String DEFAULT_PROVIDER = "BC";

    // Number of days of inactivity after which a cached entry is removed
    private static final int EXPIRE_AFTER = 1;

    private final Executor _executor = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("KeyPair-generator-%d").build());

    private final LoadingCache<Integer,KeyPair> _cache;
    private String algorithm = DEFAULT_ALGORITHM;
    private String provider = DEFAULT_PROVIDER;

    public KeyPairCache(int lifetime)
    {
        if(lifetime > 0) {
            _cache = CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(EXPIRE_AFTER, TimeUnit.DAYS)
                    .refreshAfterWrite(lifetime, TimeUnit.SECONDS)
                    .build(
                        new CacheLoader<Integer,KeyPair>() {
                            @Override
                            public KeyPair load(Integer keySize) throws
                                    NoSuchAlgorithmException,
                                    NoSuchProviderException
                            {
                                return generate(keySize);
                            }

                            @Override
                            public ListenableFuture<KeyPair> reload(final
                                    Integer keySize, KeyPair previous)
                            {
                                ListenableFutureTask<KeyPair> task =
                                        ListenableFutureTask.create(
                                        new Callable<KeyPair>()
                                    {
                                        @Override
                                        public KeyPair call() throws
                                                NoSuchAlgorithmException,
                                                NoSuchProviderException
                                        {
                                            return generate(keySize);
                                        }
                                    });
                                _executor.execute(task);
                                return task;
                            }
                        }
                    );
        } else {
            _cache = null;
        }
    }

    public String getAlgorithm()
    {
        return algorithm;
    }

    public void setAlgorithm(String value)
    {
        algorithm = value;
    }

    public String getProvider()
    {
        return provider;
    }

    public void setProvider(String value)
    {
        provider = value;
    }

    public KeyPair getKeyPair(int bits)
        throws NoSuchAlgorithmException, NoSuchProviderException {

        if (_cache == null) {
            return generate(bits);
        } else {
            try {
                return _cache.get(bits);
            } catch (ExecutionException e) {
                // propagate
                throw new RuntimeException();
            }
        }
    }

    private KeyPair generate(int bits) throws NoSuchAlgorithmException,
            NoSuchProviderException
    {
        LOG.debug("Generating KeyPair for {} bits", bits);

        KeyPairGenerator generator =
            KeyPairGenerator.getInstance(this.algorithm, this.provider);
        generator.initialize(bits);
        return generator.generateKeyPair();
    }
}
