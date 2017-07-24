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
package org.dcache.restful.util.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.restful.services.cells.CellInfoServiceImpl;
import org.dcache.vehicles.cells.json.CellData;

/**
 * <p>Used in conjunction with the {@link CellInfoCollector} as message
 * post-processor.  Updates the cell data based on the info received.</p>
 */
public final class CellInfoFutureProcessor {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(CellInfoFutureProcessor.class);

    private final Map<String, ListenableFutureWrapper<CellInfo>> futureMap
                    = new HashMap<>();

    private final Map<String, CellData> next
                    = Collections.synchronizedMap(new HashMap<>());

    /**
     * <p>Injected</p>
     */
    private Executor executor;
    private CellInfoServiceImpl service;

    /**
     * <p>Cancels all futures in map.</p>
     */
    public synchronized void cancel() {
        futureMap.values().stream()
                 .forEach(wrapper -> wrapper.getFuture().cancel(true));
        futureMap.clear();
    }

    public synchronized boolean isProcessing() {
        return !futureMap.isEmpty();
    }

    /**
     * <p>Adds listener to each future returned.</p>
     *
     * @param futureMap returned from most recent collection.
     * @throws IllegalStateException if the processor is still
     *                               running a previous pass.
     */
    public synchronized void process(
                    Map<String, ListenableFutureWrapper<CellInfo>> futureMap)
                    throws IllegalStateException, IllegalArgumentException {
        if (!this.futureMap.isEmpty()) {
            String error = "Cannot execute process; previous processing "
                            + "has not completed.";
            throw new IllegalStateException(error);
        }

        /*
         * Avoid potential concurrent modification exception when
         * removing, possibly while still adding listeners (below).
         */
        this.futureMap.clear();
        this.futureMap.putAll(futureMap);

        /*
         * Inject a listener into the futures.
         */
        futureMap.entrySet().stream().forEach(this::addListener);
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public void setService(CellInfoServiceImpl service) {
        this.service = service;
    }

    /**
     * <p>Adds listener which is responsible for transforming the cell info
     * into the stored object.</p>
     *
     * @param entry holding the listenable future for the cell info.
     */
    private void addListener(Map.Entry<String,
                    ListenableFutureWrapper<CellInfo>> entry) {
        entry.getValue().getFuture().addListener(() -> {
            String key = entry.getKey();
            ListenableFutureWrapper<CellInfo> wrapper = entry.getValue();
            try {
                CellInfo received = wrapper.getFuture().get();

                if (received == null) {
                    remove(key);
                    return;
                }

                CellData cellData = new CellData();
                if (cellData != null) {
                    cellData.setRoundTripTime(System.currentTimeMillis()
                                                              - wrapper.getSent());
                }

                update(cellData, received);
                next.put(key, cellData);
            } catch (InterruptedException e) {
                LOGGER.trace("Listener runnable was interrupted; returning ...");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof NoRouteToCellException) {
                    LOGGER.trace("Endpoint currently unavailable: {}.", key);
                } else {
                    LOGGER.warn("Update of cell data for {} failed: {} / {}.",
                                key, e.getMessage(), e.getCause());
                }
            }

            remove(key);
        }, executor);
    }

    private synchronized void remove(String key) {
        futureMap.remove(key);

        /*
         *  If future map is empty, swap current for next.
         *  This is to avoid stale entries in the cache.
         */
        if (futureMap.isEmpty()) {
            service.updateCache(next);
            next.clear();
        }
    }

    private static void update(CellData cellData, CellInfo received) {
        cellData.setCreationTime(received.getCreationTime());
        cellData.setDomainName(received.getDomainName());
        cellData.setCellType(received.getCellType());
        cellData.setCellName(received.getCellName());
        cellData.setCellClass(received.getCellClass());
        cellData.setEventQueueSize(received.getEventQueueSize());
        cellData.setExpectedQueueTime(received.getExpectedQueueTime());
        cellData.setLabel("Cell Info");
        CellVersion version = received.getCellVersion();
        cellData.setRelease(version.getRelease());
        cellData.setRevision(version.getRevision());
        cellData.setVersion(version.toString());
        cellData.setState(received.getState());
        cellData.setThreadCount(received.getThreadCount());
    }
}
