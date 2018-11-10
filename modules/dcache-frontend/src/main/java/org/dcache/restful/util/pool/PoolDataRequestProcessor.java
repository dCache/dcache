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
package org.dcache.restful.util.pool;

import org.springframework.beans.factory.annotation.Required;

import java.io.Serializable;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.json.CellData;
import org.dcache.pool.json.PoolData;
import org.dcache.pool.json.PoolInfoWrapper;
import org.dcache.restful.services.pool.PoolInfoServiceImpl;
import org.dcache.util.collector.RequestFutureProcessor;
import org.dcache.vehicles.pool.PoolDataRequestMessage;

/**
 * <p>Handles the transformation of message content from pools into a cached
 *    {@link PoolInfoWrapper}.  The data handled by this transformation are
 *    the basic diagnostic information plus the precomputed LRU histogram
 *    from the sweeper.</p>
 *
 * <p>Post-processing aggregates the data according to pool groups and adds
 *    these to the cache as well.</p>
 */
public final class PoolDataRequestProcessor
                extends RequestFutureProcessor<PoolInfoWrapper, PoolDataRequestMessage> {
    private PoolInfoServiceImpl  service;
    private PoolHistoriesHandler handler;

    @Required
    public void setHandler(PoolHistoriesHandler handler) {
        this.handler = handler;
    }

    @Required
    public void setService(PoolInfoServiceImpl service) {
        this.service = service;
    }

    @Override
    protected void postProcess() {
        try {
            handler.aggregateDataForPoolGroups(next,
                                               service.getSelectionUnit());
        } catch (CacheException e) {
            LOGGER.warn("Aggregation of timeseries data failed: {} / {}.",
                        e.getMessage(), e.getCause());
        }

        service.updateJsonData(next);
    }

    @Override
    protected PoolInfoWrapper process(String key,
                                      PoolDataRequestMessage message,
                                      long sent) {
        PoolData poolData = message.getData();

        CellData cellData = poolData == null ? null : poolData.getCellData();

        if (cellData != null) {
            cellData.setRoundTripTime(System.currentTimeMillis() - sent);
        }

        PoolInfoWrapper info = new PoolInfoWrapper();
        info.setKey(key);

        Serializable errorObject = message.getErrorObject();

        /*
         *  NB:  the counts histogram is already part of the sweeper
         *  data object (data.getSweeperData().getLastAccessHistogram()).
         */
        info.setInfo(poolData);

        if (errorObject != null) {
            LOGGER.warn("Problem with retrieval of pool data for {}: {}.",
                        key, errorObject.toString());
            return info;
        }

        try {
            handler.addHistoricalData(info);
        } catch (NoRouteToCellException | InterruptedException | TimeoutCacheException e) {
            LOGGER.debug("Could not add historical data for {}: {}.",
                     key, e.getMessage());
        } catch (CacheException e) {
            LOGGER.error("Could not add historical data for {}: {}.",
                         key, e.getMessage());
        }

        return info;
    }
}
