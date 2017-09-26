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
package org.dcache.restful.util.transfers;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import diskCacheV111.util.TransferInfo;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginBrokerSubscriber;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import org.dcache.util.collector.CellMessagingCollector;

import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;

/**
 * <p>Adapted from {@link org.dcache.util.TransferCollector}.
 *      (Will eventually replace that class.)</p>
 *
 * <p>May be overridden for testing purposes.</p>
 */
public class TransferCollector extends CellMessagingCollector<Map<String, TransferInfo>> {
    private static final String   DOOR_INFO_CMD  = "get door info -binary";
    private static final String   LM_INFO_CMD    = "get children -binary";
    private static final String   MOVER_INFO_CMD = "mover ls -binary";

    /**
     * <p>For obtaining current door information.</p>
     */
    private LoginBrokerSubscriber loginBrokerSource;

    /**
     * <p>Gathers endpoints for the login managers, pulls
     * in their children and queueries each for their door instances;
     * from these the targeted pool endpoints are obtained, which
     * are then queried for their movers.  The door and mover
     * info is encapsulated into {@link TransferInfo}.</p>
     *
     * @return map of transfer key, transfer info.
     */
    public Map<String, TransferInfo> collectData() throws InterruptedException {
        try {
            Collection<LoginBrokerInfo> loginBrokerInfos
                            = loginBrokerSource.doors();
            Set<CellPath> cellPaths = TransferCollectionUtils.getLoginManagers(loginBrokerInfos);
            Collection<LoginManagerChildrenInfo> loginManagerInfos
                            = collectLoginManagerInfo(cellPaths).get();
            Collection<IoDoorInfo> doorInfos
                            = collectDoorInfo(
                            TransferCollectionUtils.getDoors(loginManagerInfos)).get();
            cellPaths = TransferCollectionUtils.getPools(doorInfos);
            Collection<IoJobInfo> movers = collectMovers(cellPaths).get();
            List<TransferInfo> transfers = TransferCollectionUtils.transfers(doorInfos, movers);
            Map<String, TransferInfo> refreshedData = new HashMap<>();
            for (TransferInfo info : transfers) {
                refreshedData.put(TransferCollectionUtils.transferKey(info.getCellName(),
                                                                      info.getSerialId()), info);
            }
            return refreshedData;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public void setLoginBrokerSource(LoginBrokerSubscriber loginBrokerSource) {
        this.loginBrokerSource = loginBrokerSource;
    }

    private ListenableFuture<Collection<IoDoorInfo>>
    collectDoorInfo(Set<CellPath> doors) {
        return transform(query(doors,
                               DOOR_INFO_CMD,
                               IoDoorInfo.class,
                               "door",
                               null),
                         TransferCollectionUtils.removeNulls());
    }

    private ListenableFuture<Collection<LoginManagerChildrenInfo>>
    collectLoginManagerInfo(Set<CellPath> loginManagers) {
        return transform(query(loginManagers,
                               LM_INFO_CMD,
                               LoginManagerChildrenInfo.class,
                               "login manager",
                               null),
                         TransferCollectionUtils.removeNulls());
    }

    private ListenableFuture<Collection<IoJobInfo>>
    collectMovers(Set<CellPath> pools) {
        return transform(query(pools,
                               MOVER_INFO_CMD,
                               IoJobInfo[].class,
                               "pool",
                               new IoJobInfo[0]),
                         TransferCollectionUtils.flatten());
    }

    private <T> ListenableFuture<List<T>> query(Collection<CellPath> cells,
                                                String query,
                                                Class<T> returnType,
                                                String endpoint,
                                                T defaultValue) {
        List<ListenableFuture<T>> futures =
                        cells.stream()
                             .map(cell -> catchingAsync(
                                             stub.send(cell, query,
                                                       returnType),
                                             Throwable.class,
                                             t -> {
                                                 LOGGER.debug("Failed to query {}: {}",
                                                              endpoint,
                                                              t.toString());
                                                 return immediateFuture(
                                                                 defaultValue);
                                             }))
                             .collect(Collectors.toList());
        return allAsList(futures);
    }
}
