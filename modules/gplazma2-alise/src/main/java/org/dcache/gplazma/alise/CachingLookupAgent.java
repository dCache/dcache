/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.alise;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.security.Principal;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A simple caching layer to prevents dCache from hammering ALISE, if there are
 * repeated requests for the same identity.
 *
 * The primary use-case is when there are multiple clients using dCache with
 * different (distinct) OIDC tokens but that have the same underlying identity
 * ('iss' and 'sub' claim).  The dCache doors will forward each OIDC token to
 * gPlazma as the bearer tokens are distinct, but repeated calls to ALISE for
 * the same identity makes no sense.
 */
public class CachingLookupAgent implements LookupAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachingLookupAgent.class);

    private final LookupAgent inner;
    private final ExecutorService executor = new BoundedCachedExecutor(5);

    private final LoadingCache<Identity, Result<Collection<Principal>, String>> lookupResults = CacheBuilder.newBuilder()
            .maximumSize(1_000)
            .refreshAfterWrite(10, TimeUnit.SECONDS)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<Identity, Result<Collection<Principal>, String>>() {
                @Override
                public Result<Collection<Principal>, String> load(Identity identity) {
                    LOGGER.debug("Populating cache with identity {}", identity);
                    return inner.lookup(identity);
                }

                @Override
                public ListenableFuture<Result<Collection<Principal>, String>> reload(Identity identity, Result<Collection<Principal>,String> prevResult) {
                    LOGGER.debug("Scheduling refresh of identity {}", identity);
                    var task = ListenableFutureTask.create(() -> inner.lookup(identity));
                    executor.execute(task);
                    return task;
                }
            });

    public CachingLookupAgent(LookupAgent inner) {
        this.inner = requireNonNull(inner);
    }

    @Override
    public Result<Collection<Principal>, String> lookup(Identity identity) {
        try {
            return lookupResults.get(identity);
        } catch (ExecutionException e) {
            Throwable reported = e.getCause() == null ? e : e.getCause();
            return Result.failure("Cache lookup failed: " + reported);
        }
    }

    @Override
    public void shutdown() {
        LOGGER.debug("Shutting down executor service.");
        executor.shutdown();
        inner.shutdown();
    }
}
