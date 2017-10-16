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

import com.google.common.util.concurrent.ListenableFuture;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import dmg.util.command.Command;
import org.dcache.restful.providers.SnapshotList;
import org.dcache.restful.providers.restores.RestoreInfo;
import org.dcache.restful.util.admin.SnapshotDataAccess;
import org.dcache.restful.util.restores.RestoreCollector;
import org.dcache.services.collector.CellDataCollectingService;

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

    @Command(name = "restores refresh",
                    hint = "Query for current tape restore queue info",
                    description = "Interrupts current wait to run query "
                                    + "immediately.")
    class RestoresRefreshCommand extends RefreshCommand {
    }

    /**
     * <p>Data store providing snapshots.</p>
     */
    private final SnapshotDataAccess<String, RestoreInfo>
                    access = new SnapshotDataAccess<>();

    @Override
    public SnapshotList<RestoreInfo> get(UUID token,
                                         Integer offset,
                                         Integer limit,
                                         PnfsId pnfsid)
                    throws InvocationTargetException,
                    IllegalAccessException,
                    NoSuchMethodException {
        Method getPnfsid = RestoreInfo.class.getMethod("getPnfsId");
        Method[] methods = pnfsid == null? null : new Method[]{getPnfsid};
        Object[] values = pnfsid == null? null : new Object[]{pnfsid};
        return access.getSnapshot(token, offset, limit, methods, values);
    }

    @Override
    protected void update(ListenableFuture<RestoreHandlerInfo[]> future) {
        Map<String, RestoreInfo> newInfo = new HashMap<>();
        Throwable thrownDuringExecution = null;

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
            Throwable t = e.getCause();
            LOGGER.warn("Update could not complete: {}, {}.",
                        e.getMessage(),
                        t == null ? "" : t.toString());
        } catch (ExecutionException e) {
            thrownDuringExecution = e.getCause();
            Throwable t = thrownDuringExecution.getCause();
            LOGGER.warn("Update could not complete: {}, {}.",
                        thrownDuringExecution.getMessage(),
                        t == null ? "" : t.toString());
        }

        access.refresh(newInfo);
    }
}
