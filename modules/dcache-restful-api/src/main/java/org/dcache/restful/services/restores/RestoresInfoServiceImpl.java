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
package org.dcache.restful.services.restores;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import dmg.util.command.Command;
import org.dcache.restful.providers.restores.RestoreInfo;
import org.dcache.restful.providers.restores.RestoresList;
import org.dcache.restful.services.admin.CellDataCollectingService;
import org.dcache.restful.util.restores.RestoreCollector;
import org.dcache.restful.util.transfers.TransferCollector;

/**
 * <p>Service layer responsible for collecting information from
 * the pool manager on current staging requests and caching it.</p>
 *
 * <p>All synchronization is done on the object reference rather
 *      than the main map and snapshot cache, in order to
 *      allow the cache to be rebuilt.
 * </p>
 */
public final class RestoresInfoServiceImpl extends
                CellDataCollectingService<ListenableFuture<RestoreHandlerInfo[]>,
                                RestoreCollector>
                implements RestoresInfoService {
    @Command(name = "restores set timeout",
                    hint = "Set the timeout interval between refreshes",
                    description = "Changes the interval between "
                                    + "collections of restore queue information.")
    class RestoresSetTimeoutCommand extends SetTimeoutCommand {
    }

    @Command(name = "restores run",
                    hint = "Query for current tape restore queue info",
                    description = "Interrupts current wait to run query "
                                    + "immediately.")
    class RestoresRefreshCommand extends RefreshCommand {
    }

    /**
     * <p>Map of transfer information extracted using
     * the {@link TransferCollector}.</p>
     */
    private final Map<String, RestoreInfo> restores = new TreeMap<>();

    /**
     * <p>Cached snapshots of the transfers.</p>
     */
    private Cache<UUID, List<RestoreInfo>> snapshots;

    /**
     * <p>Cache settings</p>
     */
    private long maxCacheSize = 1000;

    @Override
    public RestoresList get(UUID token, Integer offset, Integer limit,
                            PnfsId pnfsid) {
        if (offset == null) {
            offset = 0;
        }

        if (limit == null) {
            limit = Integer.MAX_VALUE;
        }

        if (token == null) {
            token = storeSnapshot();
        }

        RestoresList result = new RestoresList();
        result.setItems(getSnapshot(token, offset, limit, pnfsid));
        result.setCurrentToken(token);
        result.setCurrentOffset(offset);

        int size = result.getItems().size();

        if (size < limit) {
            size = -1;
        }

        result.setNextOffset(size);
        return result;
    }

    public void setMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    @Override
    protected synchronized void configure() {
        Map<UUID, List<RestoreInfo>> current = snapshots == null ?
                        Collections.EMPTY_MAP : snapshots.asMap();

        snapshots = CacheBuilder.newBuilder()
                                .maximumSize(maxCacheSize)
                                .expireAfterAccess(timeout, TimeUnit.MINUTES)
                                .build();

        snapshots.putAll(current);
    }

    @Override
    protected void update(ListenableFuture<RestoreHandlerInfo[]> future) {
        Map<String, RestoreInfo> newInfo = new HashMap<>();

        try {
            RestoreHandlerInfo[] refreshed = future.get();
            for (RestoreHandlerInfo restore : refreshed) {
                RestoreInfo info = new RestoreInfo(restore);
                collector.setPath(info);
                newInfo.put(info.getKey(), info);
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Update was interrupted.");
        } catch (CacheException e) {
            LOGGER.warn("Update could not complete: {}, {}.", e, e.getCause());
        } catch (ExecutionException e) {
            LOGGER.warn("Update could not complete: {}, {}.", e, e.getCause());
        }

        synchronized (this) {
            for (Iterator<String> k = restores.keySet().iterator(); k.hasNext(); ) {
                String key = k.next();
                if (!newInfo.containsKey(key)) {
                    k.remove();
                }
            }
            restores.putAll(newInfo);
        }
    }

    private synchronized RestoreInfo get(String key) {
        return restores.get(key);
    }

    private synchronized List<RestoreInfo> getSnapshot(UUID token,
                                                       int offset,
                                                       int limit,
                                                       PnfsId pnfsId) {
        if (!snapshots.asMap().containsKey(token)) {
            return Collections.emptyList();
        }

        List<RestoreInfo> filtered = new ArrayList<>();
        List<RestoreInfo> actual = snapshots.getIfPresent(token);

        int end = actual.size();

        String id = pnfsId == null ? null : pnfsId.toString();

        for (int i = offset; i < end && filtered.size() < limit; ++i) {
            RestoreInfo info = actual.get(i);

            if (id != null && !info.getPnfsId().equals(id)) {
                continue;
            }

            /*
             *  The staging request may actually have been removed.
             *  Indicate this by setting state.
             *  Do not remove from snapshot because the indexing needs to
             *  remain unaltered.
             */
            if (!restores.containsKey(info.getKey())) {
                info.setStatus("Staging Completed.");
            }

            filtered.add(info);
        }

        return filtered;
    }

    private synchronized UUID storeSnapshot() {
        List<RestoreInfo> values = restores.values()
                                           .stream()
                                           .collect(Collectors.toList());
        UUID uuid = UUID.randomUUID();
        snapshots.put(uuid, values);
        return uuid;
    }
}
