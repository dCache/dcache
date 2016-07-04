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
package org.dcache.space;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import diskCacheV111.services.space.Space;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class for creating caches for SpaceManager information.  Typically,
 * these caches reside in doors that need to query SpaceManager for information
 * about space reservations.
 */
public class ReservationCaches
{
    private static final Logger _log = LoggerFactory.getLogger(ReservationCaches.class);

    public static class GetSpaceTokensKey
    {
        private final Set<Principal> principals;
        private final String description;

        public GetSpaceTokensKey(Set<Principal> principals, String description)
        {
            this.principals = checkNotNull(principals);
            this.description = description;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GetSpaceTokensKey that = (GetSpaceTokensKey) o;
            return principals.equals(that.principals) &&
                   (description == null ? that.description == null : description.equals(that.description));

        }

        @Override
        public int hashCode()
        {
            int result = principals.hashCode();
            result = 31 * result + (description != null ? description.hashCode() : 0);
            return result;
        }
    }

    private ReservationCaches()
    {
        // prevent instantiation
    }

    /**
     * Builds a loading cache for looking up space tokens by owner and
     * description.
     */
    public static LoadingCache<GetSpaceTokensKey, long[]> buildOwnerDescriptionLookupCache(CellStub spaceManager, Executor executor)
    {
        return CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(30, SECONDS)
                .refreshAfterWrite(10, SECONDS)
                .recordStats()
                .build(new CacheLoader<GetSpaceTokensKey, long[]>()
                {
                    @Override
                    public long[] load(GetSpaceTokensKey key) throws Exception
                    {
                        try {
                            return spaceManager.sendAndWait(createRequest(key)).getSpaceTokens();
                        } catch (TimeoutCacheException e) {
                            throw new SRMInternalErrorException("Space manager timeout", e);
                        } catch (InterruptedException e) {
                            throw new SRMInternalErrorException("Operation interrupted", e);
                        } catch (CacheException e) {
                            _log.warn("GetSpaceTokens failed with rc={} error={}", e.getRc(), e.getMessage());
                            throw new SRMException("GetSpaceTokens failed with rc=" + e.getRc() +
                                                   " error=" + e.getMessage(), e);
                        }
                    }

                    private GetSpaceTokens createRequest(GetSpaceTokensKey key)
                    {
                        GetSpaceTokens message = new GetSpaceTokens(key.description);
                        message.setSubject(new Subject(true, key.principals,
                                                       Collections.emptySet(), Collections.emptySet()));
                        return message;
                    }

                    @Override
                    public ListenableFuture<long[]> reload(GetSpaceTokensKey key, long[] oldValue) throws Exception
                    {
                        final SettableFuture<long[]> future = SettableFuture.create();
                        CellStub.addCallback(
                                spaceManager.send(createRequest(key)),
                                new AbstractMessageCallback<GetSpaceTokens>()
                                {
                                    @Override
                                    public void success(GetSpaceTokens message)
                                    {
                                        future.set(message.getSpaceTokens());
                                    }

                                    @Override
                                    public void failure(int rc, Object error)
                                    {
                                        CacheException exception = CacheExceptionFactory.exceptionOf(
                                                rc, Objects.toString(error, null));
                                        future.setException(exception);
                                    }
                                }, executor);
                        return future;
                    }
                });
    }

    /**
     * Build a loading cache for looking up space reservations by space token.
     */
    public static LoadingCache<String,Optional<Space>> buildSpaceLookupCache(CellStub spaceManager, Executor executor)
    {
        return CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(10, MINUTES)
                    .refreshAfterWrite(30, SECONDS)
                    .recordStats()
                    .build(
                            new CacheLoader<String, Optional<Space>>()
                            {
                                @Override
                                public Optional<Space> load(String token)
                                        throws CacheException, NoRouteToCellException, InterruptedException
                                {
                                    Space space =
                                            spaceManager.sendAndWait(new GetSpaceMetaData(token)).getSpaces()[0];
                                    return Optional.fromNullable(space);
                                }

                                @Override
                                public ListenableFuture<Optional<Space>> reload(String token, Optional<Space> oldValue)
                                {
                                    final SettableFuture<Optional<Space>> future = SettableFuture.create();
                                    CellStub.addCallback(
                                            spaceManager.send(new GetSpaceMetaData(token)),
                                            new AbstractMessageCallback<GetSpaceMetaData>()
                                            {
                                                @Override
                                                public void success(GetSpaceMetaData message)
                                                {
                                                    future.set(Optional.fromNullable(message.getSpaces()[0]));
                                                }

                                                @Override
                                                public void failure(int rc, Object error)
                                                {
                                                    CacheException exception = CacheExceptionFactory.exceptionOf(
                                                            rc, Objects.toString(error, null));
                                                    future.setException(exception);
                                                }
                                            }, executor);
                                    return future;
                                }
                            });
    }

    /**
     * Cache queries to discover if a directory has the "WriteToken" tag set.
     */
    public static LoadingCache<FsPath,java.util.Optional<String>> buildWriteTokenLookupCache(PnfsHandler pnfs, Executor executor)
    {
        return CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(10, MINUTES)
                    .refreshAfterWrite(5, MINUTES)
                    .recordStats()
                    .build(new CacheLoader<FsPath,java.util.Optional<String>>()
                            {
                                private java.util.Optional<String> writeToken(FileAttributes attr)
                                {
                                    StorageInfo info = attr.getStorageInfo();
                                    return java.util.Optional.ofNullable(info.getMap().get("writeToken"));
                                }

                                @Override
                                public java.util.Optional<String> load(FsPath path)
                                        throws CacheException, NoRouteToCellException, InterruptedException
                                {
                                    return writeToken(pnfs.getFileAttributes(path, EnumSet.of(FileAttribute.STORAGEINFO)));
                                }

                                @Override
                                public ListenableFuture<java.util.Optional<String>> reload(FsPath path, java.util.Optional<String> old)
                                {
                                    PnfsGetFileAttributes message = new PnfsGetFileAttributes(path.toString(), EnumSet.of(FileAttribute.STORAGEINFO));
                                    SettableFuture<java.util.Optional<String>> future = SettableFuture.create();
                                    CellStub.addCallback(pnfs.requestAsync(message),
                                            new AbstractMessageCallback<PnfsGetFileAttributes>()
                                            {
                                                @Override
                                                public void success(PnfsGetFileAttributes message)
                                                {
                                                    future.set(writeToken(message.getFileAttributes()));
                                                }

                                                @Override
                                                public void failure(int rc, Object error)
                                                {
                                                    CacheException exception = CacheExceptionFactory.exceptionOf(
                                                            rc, Objects.toString(error, null));
                                                    future.setException(exception);
                                                }
                                            }, executor);
                                    return future;
                                }
                            });
    }
}
