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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.NoRouteToCellException;
import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.security.auth.Subject;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating caches for SpaceManager information.  Typically, these caches reside
 * in doors that need to query SpaceManager for information about space reservations.
 */
public class ReservationCaches {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReservationCaches.class);

    public static class GetSpaceTokensKey {

        private final Set<Principal> principals;
        private final String description;

        public GetSpaceTokensKey(Set<Principal> principals, String description) {
            this.principals = requireNonNull(principals);
            this.description = description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GetSpaceTokensKey that = (GetSpaceTokensKey) o;
            return principals.equals(that.principals) &&
                  (description == null ? that.description == null
                        : description.equals(that.description));

        }

        @Override
        public int hashCode() {
            int result = principals.hashCode();
            result = 31 * result + (description != null ? description.hashCode() : 0);
            return result;
        }
    }

    private ReservationCaches() {
        // prevent instantiation
    }

    /**
     * Builds a loading cache for looking up space tokens by owner and description.
     */
    public static AsyncLoadingCache<GetSpaceTokensKey, long[]> buildOwnerDescriptionLookupCache(
          CellStub spaceManager, Executor executor) {
        return Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(30, SECONDS)
              .refreshAfterWrite(10, SECONDS).recordStats().executor(executor)
              .buildAsync(new AsyncCacheLoader<>() {
                  private GetSpaceTokens createRequest(GetSpaceTokensKey key) {
                      GetSpaceTokens message = new GetSpaceTokens(key.description);
                      message.setSubject(new Subject(true, key.principals, Collections.emptySet(),
                            Collections.emptySet()));
                      return message;
                  }

                  @Override
                  public CompletableFuture<long[]> asyncLoad(GetSpaceTokensKey key,
                        Executor executor) throws Exception {
                      try {
                          return CompletableFuture.completedFuture(
                                spaceManager.sendAndWait(createRequest(key)).getSpaceTokens());
                      } catch (TimeoutCacheException e) {
                          throw new SRMInternalErrorException("Space manager timeout", e);
                      } catch (InterruptedException e) {
                          throw new SRMInternalErrorException("Operation interrupted", e);
                      } catch (CacheException e) {
                          LOGGER.warn("GetSpaceTokens failed with rc={} error={}", e.getRc(),
                                e.getMessage());
                          throw new SRMException(
                                "GetSpaceTokens failed with rc=" + e.getRc() + " error="
                                      + e.getMessage(), e);
                      }
                  }

                  @Override
                  public CompletableFuture<long[]> asyncReload(GetSpaceTokensKey key,
                        long[] oldValue, Executor executor) {
                      // A future we are going to complete in the Cell Callback.
                      final CompletableFuture<long[]> future = new CompletableFuture<>();

                      CellStub.addCallback(spaceManager.send(createRequest(key)),
                            new AbstractMessageCallback<>() {
                                @Override
                                public void success(GetSpaceTokens message) {
                                    future.complete(message.getSpaceTokens());
                                }

                                @Override
                                public void failure(int rc, Object error) {
                                    CacheException exception = CacheExceptionFactory.exceptionOf(rc,
                                          Objects.toString(error, null));
                                    future.completeExceptionally(exception);
                                }
                            }, executor);
                      return future;
                  }
              });
    }

    /**
     * Build a loading cache for looking up space reservations by space token.
     */
    public static AsyncLoadingCache<String, Optional<Space>> buildSpaceLookupCache(
          CellStub spaceManager,
          Executor executor) {
        return Caffeine.newBuilder()
              .maximumSize(1000)
              .expireAfterWrite(10, MINUTES)
              .refreshAfterWrite(30, SECONDS)
              .recordStats()
              .executor(executor)
              .buildAsync(new AsyncCacheLoader<>() {
                  @Override
                  public CompletableFuture<Optional<Space>> asyncLoad(String token,
                        Executor executor)
                        throws CacheException, NoRouteToCellException, InterruptedException {

                      Space space = spaceManager.sendAndWait(new GetSpaceMetaData(token))
                            .getSpaces()[0];

                      return CompletableFuture.completedFuture(Optional.ofNullable(space));
                  }

                  @Override
                  public CompletableFuture<Optional<Space>> asyncReload(String token,
                        Optional<Space> oldValue, Executor executor) {
                      // A future we are going to complete in the Cell Callback.
                      final CompletableFuture<Optional<Space>> future = new CompletableFuture<>();

                      CellStub.addCallback(
                            spaceManager.send(new GetSpaceMetaData(token)),
                            new AbstractMessageCallback<>() {
                                @Override
                                public void success(GetSpaceMetaData message) {
                                    future.complete(Optional.ofNullable(message.getSpaces()[0]));
                                }

                                @Override
                                public void failure(int rc, Object error) {
                                    CacheException exception = CacheExceptionFactory.exceptionOf(
                                          rc, Objects.toString(error, null));
                                    future.completeExceptionally(exception);
                                }
                            }, executor);
                      return future;
                  }
              });
    }

    /**
     * Cache queries to discover if a directory has the "WriteToken" tag set.
     */
    public static AsyncLoadingCache<FsPath, Optional<String>> buildWriteTokenLookupCache(
          PnfsHandler pnfs, Executor executor) {
        return Caffeine.newBuilder()
              .maximumSize(1000)
              .expireAfterWrite(10, MINUTES)
              .refreshAfterWrite(5, MINUTES)
              .recordStats()
              .executor(executor)
              .buildAsync(new AsyncCacheLoader<>() {
                  private Optional<String> writeToken(FileAttributes attr) {
                      StorageInfo info = attr.getStorageInfo();
                      return Optional.ofNullable(info.getMap().get("writeToken"));
                  }

                  @Override
                  public CompletableFuture<Optional<String>> asyncLoad(FsPath path,
                        Executor executor)
                        throws CacheException {
                      return CompletableFuture.completedFuture(writeToken(
                            pnfs.getFileAttributes(path, EnumSet.of(FileAttribute.STORAGEINFO))));
                  }

                  @Override
                  public CompletableFuture<Optional<String>> asyncReload(FsPath path,
                        Optional<String> old, Executor executor) {
                      // A future we are going to complete in the Cell Callback.
                      final CompletableFuture<Optional<String>> future = new CompletableFuture<>();

                      PnfsGetFileAttributes message = new PnfsGetFileAttributes(path.toString(),
                            EnumSet.of(FileAttribute.STORAGEINFO));

                      CellStub.addCallback(pnfs.requestAsync(message),
                            new AbstractMessageCallback<>() {
                                @Override
                                public void success(PnfsGetFileAttributes message) {
                                    future.complete(writeToken(message.getFileAttributes()));
                                }

                                @Override
                                public void failure(int rc, Object error) {
                                    CacheException exception = CacheExceptionFactory.exceptionOf(
                                          rc, Objects.toString(error, null));
                                    future.completeExceptionally(exception);
                                }
                            }, executor);
                      return future;
                  }
              });
    }
}
