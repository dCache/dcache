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

import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.restful.providers.billing.BillingDataGrid;
import org.dcache.restful.providers.billing.BillingDataGridEntry;
import org.dcache.restful.providers.billing.BillingRecords;
import org.dcache.restful.services.admin.CellDataCollectingService;
import org.dcache.restful.util.billing.BillingInfoCollectionUtils;
import org.dcache.restful.util.billing.BillingInfoCollector;
import org.dcache.util.histograms.Histogram;
import org.dcache.util.histograms.HistogramModel;
import org.dcache.util.histograms.TimeFrame;
import org.dcache.vehicles.billing.BillingDataRequestMessage;
import org.dcache.vehicles.billing.BillingRecordRequestMessage;

/**
 * <p>Supports the REST calls for billing records and billing time series data.</p>
 *
 * <p>Maintains a map of the time series histograms which is refreshed every
 *    time-period interval. </p>
 */
public class BillingInfoServiceImpl
                extends CellDataCollectingService<Map<String, Future<BillingDataRequestMessage>>,
                                                  BillingInfoCollector>
                implements BillingInfoService {
    @Command(name = "billing ls",
                    hint = "list billing records",
                    description = "returns a list of billing records for "
                                    + "a given file.")
    class LsCommand implements Callable<String> {
        @Argument(required = true,
                        usage = "The pnfsid of the file for which to "
                                        + "list the billing records.")
        String pnfsid;

        @Option(name = "before",
                        valueSpec = DATETIME_FORMAT,
                        usage = "List only records whose start time "
                                        + "was before this date-time.")
        String before;

        @Option(name = "after",
                        valueSpec = DATETIME_FORMAT,
                        usage = "List only records whose start time "
                                        + "was after this date-time.")
        String after;

        @Override
        public String call() {
            StringBuilder builder = new StringBuilder();

            try {
                BillingRecords records = getRecords(new PnfsId(pnfsid),
                                                    before,
                                                    after);
                processRecords(records, builder);
            } catch (CacheException e) {
                builder.append(e).append("\n");
            }

            return builder.toString();
        }

        private void processRecords(BillingRecords records,
                                    StringBuilder builder) {
            builder.append(records.getPnfsid()).append("\n");
            if (!records.getReads().isEmpty()) {
                builder.append("\nREADS\n");
                records.getReads().stream().forEach(
                                (r) -> builder.append(r.toDisplayString()));
            }
            if (!records.getWrites().isEmpty()) {
                builder.append("\nWRITES\n");
                records.getWrites().stream().forEach(
                                (r) -> builder.append(r.toDisplayString()));
            }
            if (!records.getP2ps().isEmpty()) {
                builder.append("\nP2PS\n");
                records.getP2ps().stream().forEach(
                                (r) -> builder.append(r.toDisplayString()));
            }
            if (!records.getStores().isEmpty()) {
                builder.append("\nSTORES\n");
                records.getStores().stream().forEach(
                                (r) -> builder.append(r.toDisplayString()));
            }
            if (!records.getRestores().isEmpty()) {
                builder.append("\nRESTORES\n");
                records.getRestores().stream().forEach(
                                (r) -> builder.append(r.toDisplayString()));
            }
        }
    }

    @Command(name = "billing update",
                    hint = "set the update interval",
                    description = "Changes the interval between "
                                    + "collections of billing data.")
    class BillingSetUpdateCommand extends SetUpdateCommand {
    }

    @Command(name = "billing run",
                    hint = "Run the update",
                    description = "Interrupts current wait to run update "
                                    + "immediately.")
    class BillingRunUpdateCommand extends RunUpdateCommand {
    }

    /**
     * <p>A temporary placeholder used during initialization.</p>
     *
     * <p>Billing service may not be available at initialization,
     *    so the map is filled with these markers until the first
     *    successful collection run.</p>
     */
    class PlaceHolderModel extends HistogramModel {

        PlaceHolderModel(BillingDataRequestMessage message) {
            TimeFrame timeFrame = message.getTimeFrame();
            identifier = message.getType().name() + "_"
                         + timeFrame.getTimeframe().name();
            binUnit = timeFrame.getBinWidth();
            binWidth = 1;
            binUnitLabel = timeFrame.getTimebin().name();
            dataUnitLabel = message.getDataType().name();
            lowestBin = 0.0;
            binCount = 0;
            data = new ArrayList<>();
        }

        @Override
        public void configure() {
        }
    }

    /**
     * <p>An internal marker.  Request for billing data mapped to this
     * class will result in an exception raised
     * and propagated to the client.</p>
     */
    class ErrorData extends HistogramModel {
        CacheException exception;

        @Override
        public void configure() {
        }
    }

    private final Map<String, HistogramModel> cachedData = new ConcurrentHashMap<>();

    @Override
    public Histogram getHistogram(String key)
                    throws CacheException {
        if (key == null) {
            throw new CacheException("Must specify series type, "
                                                     + "series data type and "
                                                     + "time frame range.");
        }

        /**
         *  For verification.   If key is invalid, parse will fail.
         */
        new BillingDataGridEntry(key);

        HistogramModel model = cachedData.get(key);

        if (model instanceof ErrorData) {
            throw ((ErrorData) model).exception;
        }

        return model.toHistogram();
    }

    @Override
    public BillingRecords getRecords(PnfsId pnfsId, String before, String after)
                    throws
                    CacheException {
        BillingRecordRequestMessage msg = new BillingRecordRequestMessage();
        msg.setPnfsId(pnfsId);

        try {
            msg.setBefore(getDate(before));
            msg.setAfter(getDate(after));
        } catch (ParseException e) {
            throw new CacheException("could not parse datetime format", e);
        }

        msg = collector.sendRecordRequest(msg);

        Serializable error = msg.getErrorObject();

        if (error != null) {
            if (error instanceof CacheException) {
                throw (CacheException) error;
            }
            throw new CacheException(String.valueOf(error));
        }

        return BillingInfoCollectionUtils.transform(msg);
    }

    @Override
    public BillingDataGrid getGrid() throws CacheException {
        return BillingInfoCollectionUtils.getDataGrid();
    }

    @Override
    protected void configure() {
        /**
         * insertion into map does not need synchronization.
         */
        if (cachedData.isEmpty()) {
            BillingInfoCollectionUtils.generateMessages()
                                      .stream()
                                      .forEach(this::updateCache);
        }
    }

    @Override
    protected void update(Map<String, Future<BillingDataRequestMessage>> data) {
        Map<String, Future<BillingDataRequestMessage>> futures = null;
        synchronized(this) {
            futures = collector.collectData();
        }

        /**
         * insertion into map does not need synchronization.
         */
        waitForRequests(futures);
    }

    private void updateCache(BillingDataRequestMessage message) {
        HistogramModel histogram = message.getHistogram();
        if (histogram == null) {
            histogram = new PlaceHolderModel(message);
        }
        cachedData.put(BillingInfoCollectionUtils.getKey(message),
                       histogram);
    }

    private void updateCache(String key, Exception exception) {
        ErrorData data = new ErrorData();
        if (exception instanceof CacheException) {
            data.exception = (CacheException) exception;
        } else {
            data.exception = new CacheException("Could not retrieve data",
                                                exception);
        }
        cachedData.put(key, data);
    }

    private void waitForRequests(
                    Map<String, Future<BillingDataRequestMessage>> futures) {
        if (futures == null) {
            LOGGER.warn("collector returned a null map of futures.");
            return;
        }

        for (Entry<String, Future<BillingDataRequestMessage>> entry : futures.entrySet()) {
            try {
                Future<BillingDataRequestMessage> future = entry.getValue();
                BillingDataRequestMessage message = future.get();
                updateCache(message);
            } catch (InterruptedException e) {
                updateCache(entry.getKey(), e);
            } catch (ExecutionException e) {
                updateCache(entry.getKey(), e);
            }
        }
    }
}
