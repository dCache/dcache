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
package org.dcache.restful.services.billing;

import java.text.ParseException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.restful.providers.PagedList;
import org.dcache.restful.providers.billing.BillingDataGrid;
import org.dcache.restful.providers.billing.DoorTransferRecord;
import org.dcache.restful.providers.billing.HSMTransferRecord;
import org.dcache.restful.providers.billing.P2PTransferRecord;
import org.dcache.util.histograms.Histogram;

/**
 * <p>Defines the internal API for service providing collected/extracted
 * billing data.</p>
 */
public interface BillingInfoService {
    /**
     * <p>Request for the layout of the full grid for time series data.</p>
     *
     * @return grid description object.
     */
    BillingDataGrid getGrid() throws CacheException;

    /**
     * <p>Request for histogram data relating to a
     * particular data specification.</p>
     * <p>
     * <p>The range upper bound is determined by the service implementation,
     * but will generally coincide with the most recent information.</p>
     *
     * @param key referencing the time series data.
     * @return the corresponding Histogram.  Note that this histogram
     * is generic (i.e., simply the JSON wrapper).
     */
    Histogram getHistogram(String key) throws CacheException;

    /**
     * * <p>Request for pool-to-pool transfer records relating to a given file.</p>
     *
     * @param pnfsid of the file
     * @param before  optional upper bound on date of record
     * @param after   optional lower bound on date of record
     * @param limit   maximum number of records to return
     * @param offset  index of result to begin at
     * @param serverPool  filter on pool from which the transfer was served
     * @param clientPool  filter on pool to which the transfer was made
     * @param client  filter ip of the initiating connection
     * @param sort list of fields on which to sort.
     * @return list of records
     * @throws FileNotFoundCacheException
     * @throws CacheException
     */
    PagedList<P2PTransferRecord> getP2ps(PnfsId pnfsid,
                                         String before,
                                         String after,
                                         Integer limit,
                                         int offset,
                                         String serverPool,
                                         String clientPool,
                                         String client,
                                         String sort)
                    throws FileNotFoundCacheException, ParseException,
                    CacheException, NoRouteToCellException,
                    InterruptedException;

    /**
     * * <p>Request for read transfer records relating to a given file.</p>
     *
     * @param pnfsid of the file
     * @param before  optional upper bound on date of record
     * @param after   optional lower bound on date of record
     * @param limit   maximum number of records to return
     * @param offset  index of result to begin at
     * @param pool  filter on pool to which the transfer was made
     * @param door  filter on door through which the transfer was made
     * @param client  filter ip of the initiating connection
     * @param sort list of fields on which to sort.
     * @return list of records
     * @throws FileNotFoundCacheException
     * @throws CacheException
     */
    PagedList<DoorTransferRecord> getReads(PnfsId pnfsid,
                                           String before,
                                           String after,
                                           Integer limit,
                                           int offset,
                                           Long suid,
                                           String pool,
                                           String door,
                                           String client,
                                           String sort)
                    throws FileNotFoundCacheException, ParseException,
                    CacheException, NoRouteToCellException,
                    InterruptedException;

    /**
     * * <p>Request for restore/stage records relating to a given file.</p>
     *
     * @param pnfsid of the file
     * @param before  optional upper bound on date of record
     * @param after   optional lower bound on date of record
     * @param limit   maximum number of records to return
     * @param offset  index of result to begin at
     * @param pool  filter on pool to which the restore was made
     * @param sort list of fields on which to sort.
     * @return list of records
     * @throws FileNotFoundCacheException
     * @throws CacheException
     */
    PagedList<HSMTransferRecord> getRestores(PnfsId pnfsid,
                                             String before,
                                             String after,
                                             Integer limit,
                                             int offset,
                                             String pool,
                                             String sort)
                    throws FileNotFoundCacheException, ParseException,
                    CacheException, NoRouteToCellException,
                    InterruptedException;

    /**
     * * <p>Request for store/flush records relating to a given file.</p>
     *
     * @param pnfsid of the file
     * @param before  optional upper bound on date of record
     * @param after   optional lower bound on date of record
     * @param limit   maximum number of records to return
     * @param offset  index of result to begin at
     * @param pool  filter on pool to which the store was made
     * @param sort list of fields on which to sort.
     * @return list of records
     * @throws FileNotFoundCacheException
     * @throws CacheException
     */
    PagedList<HSMTransferRecord> getStores(PnfsId pnfsid,
                                           String before,
                                           String after,
                                           Integer limit,
                                           int offset,
                                           String pool,
                                           String sort)
                    throws FileNotFoundCacheException, ParseException,
                    CacheException, NoRouteToCellException,
                    InterruptedException;

    /**
     * * <p>Request for write transfer records relating to a given file.</p>
     *
     * @param pnfsid of the file
     * @param before  optional upper bound on date of record
     * @param after   optional lower bound on date of record
     * @param limit   maximum number of records to return
     * @param offset  index of result to begin at
     * @param pool  filter on pool to which the transfer was made
     * @param door  filter on door through which the transfer was made
     * @param client  filter ip of the initiating connection
     * @param sort list of fields on which to sort.
     * @return list of records
     * @throws FileNotFoundCacheException
     * @throws CacheException
     */
    PagedList<DoorTransferRecord> getWrites(PnfsId pnfsid,
                                            String before,
                                            String after,
                                            Integer limit,
                                            int offset,
                                            Long suid,
                                            String pool,
                                            String door,
                                            String client,
                                            String sort)
                    throws FileNotFoundCacheException, ParseException,
                    CacheException, NoRouteToCellException,
                    InterruptedException;
}
