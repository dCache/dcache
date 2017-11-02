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
package org.dcache.restful.services.pool;

import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.pool.movers.json.MoverData;
import org.dcache.pool.nearline.json.NearlineData;
import org.dcache.restful.providers.pool.PoolGroupInfo;
import org.dcache.restful.providers.pool.PoolInfo;
import org.dcache.services.history.pools.PoolListingService;

/**
 * <p>Defines the internal API for service providing collected/extracted
 * pool data.</p>
 */
public interface PoolInfoService extends PoolListingService {
    /**
     * <p>Request for reposistory listing info for a replica on a pool.</p>
     *
     * @param pool   on which the file copy is located
     * @param pnfsid of the file in question
     * @param info   data should be added to this instance
     */
    void getCacheInfo(String pool, PnfsId pnfsid, PoolInfo info);

    /**
     * <p>Request for extended cell info for the pool, including
     * pool cost, tags, migration jobs, file lifetime, repository stats
     * and other such details concerning the state of the pool.</p>
     *
     * @param name of the pool
     * @param info data should be added to this instance
     */
    void getDiagnosticInfo(String name, PoolInfo info);

    /**
     * <p>Request for histogram data concerning file lifetime.</p>
     * <p>
     * <p>Five histograms are attached:  COUNT, MAX, AVG, MIN, STDDEV; the
     * latter four are time series, the first represents the current
     * lifetimes binned by number of days.</p>
     *
     * @param name of the pool
     * @param info data should be added to this instance
     */
    void getFileStat(String name, PoolInfo info);

    /**
     * <p>Request for aggregate histogram data concerning file lifetime.</p>
     *
     * <p>Five histograms are attached:  COUNT, MAX, AVG, MIN, STDDEV; the
     * latter four are time series, the first represents the current
     * lifetimes binned by number of days.</p>
     *
     * @param name of the pool group
     * @param info data should be added to this instance
     */
    void getFileStat(String name, PoolGroupInfo info);

    /**
     * <p>Request for a list of all the FLUSH/STORE processes on the pool.</p>
     *
     * @param name of the pool
     * @return list of flushes/stores, both active and queued.
     */
    List<NearlineData> getFlush(String name) throws InterruptedException,
                    NoRouteToCellException, CacheException;

    /**
     * <p>Request cell info for all pools in the named group.</p>
     *
     * @param name of the pool group
     * @param info data should be added to this instance
     */
    void getGroupCellInfos(String name, PoolGroupInfo info);

    /**
     * <p>Request queue info for all pools in the named group.</p>
     *
     * @param name of the pool group
     * @param info data should be added to this instance
     */
    void getGroupQueueInfos(String name, PoolGroupInfo info);

    /**
     * <p>Request space info for all pools in the named group, plus
     * the aggregated data for the group.</p>
     *
     * @param name of the pool group
     * @param info data should be added to this instance
     */
    void getGroupSpaceInfos(String name, PoolGroupInfo info);

    /**
     * <p>Request for a list of all the MOVER processes on the pool.</p>
     *
     * @param name of the pool
     * @return list of movers, both active and queued.
     */
    List<MoverData> getMovers(String name) throws InterruptedException,
                    NoRouteToCellException, CacheException;

    /**
     * <p>Request for a list of all the P2P processes on the pool.</p>
     *
     * @param name of the pool
     * @return list of movers, both active and queued,
     *          for both p2p source/server and p2p client/mover.
     */
    List<MoverData> getP2p(String name) throws InterruptedException,
                    NoRouteToCellException, CacheException;

    /**
     * <p>Request for histogram data concerning queues.</p>
     *
     * <p>EIGHT histograms are attached:  for each of MOVERS, P2PS,
     * STORES [FLUSH] and RESTORES [STAGE], a timeseries for ACTIVE
     * and QUEUED is included.</p>
     *
     * @param name of the pool
     * @param info data should be added to this instance
     */
    void getQueueStat(String name, PoolInfo info);

    /**
     * <p>Request for aggregated histogram data concerning queues.</p>
     *
     * <p>EIGHT histograms are attached:  for each of MOVERS, P2PS,
     * STORES [FLUSH] and RESTORES [STAGE], a timeseries for ACTIVE
     * and QUEUED is included.</p>
     *
     * @param name of the pool group
     * @param info data should be added to this instance
     */
    void getQueueStat(String name, PoolGroupInfo info);

    /**
     * <p>Request for a list of all the REMOVE processes on the pool.</p>
     *
     * <p>Two arrays are returned, </p>
     *
     * @param name of the pool
     * @return list of removes, both active and queued.
     */
    List<NearlineData> getRemove(String name) throws InterruptedException,
                    NoRouteToCellException, CacheException;

    /**
     * <p>Request for a list of all the STAGE/RESTORE processes on the pool.</p>
     *
     * @param name of the pool
     * @return list of stages/restores, both active and queued.
     */
    List<NearlineData> getStage(String name) throws InterruptedException,
                    NoRouteToCellException, CacheException;
}
