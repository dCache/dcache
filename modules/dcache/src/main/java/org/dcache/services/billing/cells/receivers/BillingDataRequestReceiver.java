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
package org.dcache.services.billing.cells.receivers;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.Reply;
import org.dcache.cells.MessageReply;
import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.BaseEntry;
import org.dcache.services.billing.db.data.DcacheReadsDaily;
import org.dcache.services.billing.db.data.DcacheReadsHourly;
import org.dcache.services.billing.db.data.DcacheTimeDaily;
import org.dcache.services.billing.db.data.DcacheTimeHourly;
import org.dcache.services.billing.db.data.DcacheWritesDaily;
import org.dcache.services.billing.db.data.DcacheWritesHourly;
import org.dcache.services.billing.db.data.HSMReadsDaily;
import org.dcache.services.billing.db.data.HSMReadsHourly;
import org.dcache.services.billing.db.data.HSMWritesDaily;
import org.dcache.services.billing.db.data.HSMWritesHourly;
import org.dcache.services.billing.db.data.HitsDaily;
import org.dcache.services.billing.db.data.IHistogramData;
import org.dcache.services.billing.db.data.PoolHitsHourly;
import org.dcache.services.billing.db.data.PoolToPoolTransfersDaily;
import org.dcache.services.billing.db.data.PoolToPoolTransfersHourly;
import org.dcache.services.billing.db.data.SizeEntry;
import org.dcache.services.billing.db.data.TransferredEntry;
import org.dcache.services.billing.db.impl.HourlyAggregateDataHandler;
import org.dcache.util.histograms.Histogram;
import org.dcache.util.histograms.TimeFrame;
import org.dcache.util.histograms.TimeFrame.BinType;
import org.dcache.util.histograms.TimeseriesHistogram;
import org.dcache.vehicles.billing.BillingDataRequestMessage;
import org.dcache.vehicles.billing.BillingDataRequestMessage.SeriesDataType;
import org.dcache.vehicles.billing.BillingDataRequestMessage.SeriesType;

/**
 * <p>Serves up billing time-series data.</p>
 *
 * <p>Requires RDBMS back-end with the Billing DAO API.  Mapping of
 * data types to the underlying classes is fixed here.</p>
 *
 * <p>Message handling is asynchronous via executor; method returns a future.</p>
 * 
 * <p>The data is returned as a {@link Histogram} added to
 * the original request object.</p>
 */
public final class BillingDataRequestReceiver implements CellMessageReceiver {

    private static <T extends BaseEntry> Class<T> mapClass(
                    BillingDataRequestMessage data) {
        Preconditions.checkNotNull(data);
        TimeFrame timeFrame = data.getTimeFrame();
        Preconditions.checkNotNull(timeFrame);
        BinType binType = timeFrame.getTimebin();
        Preconditions.checkNotNull(binType);

        switch (data.getType()) {
            case CACHED:
                return (binType == BinType.HOUR) ?
                                (Class<T>) PoolHitsHourly.class :
                                (Class<T>) HitsDaily.class;
            case P2P:
                return (binType == BinType.HOUR) ?
                                (Class<T>) PoolToPoolTransfersHourly.class :
                                (Class<T>) PoolToPoolTransfersDaily.class;
            case WRITE:
                return (binType == BinType.HOUR) ?
                                (Class<T>) DcacheWritesHourly.class :
                                (Class<T>) DcacheWritesDaily.class;
            case READ:
                return (binType == BinType.HOUR) ?
                                (Class<T>) DcacheReadsHourly.class :
                                (Class<T>) DcacheReadsDaily.class;
            case STORE:
                return (binType == BinType.HOUR) ?
                                (Class<T>) HSMWritesHourly.class :
                                (Class<T>) HSMWritesDaily.class;
            case RESTORE:
                return (binType == BinType.HOUR) ?
                                (Class<T>) HSMReadsHourly.class :
                                (Class<T>) HSMReadsDaily.class;
            case CONNECTION:
                return (binType == BinType.HOUR) ?
                                (Class<T>) DcacheTimeHourly.class :
                                (Class<T>) DcacheTimeDaily.class;
            default:
                throw new IllegalArgumentException("No underlying data class "
                                                                   + "found for "
                                                                   +
                                                                   data.getType());
        }
    }

    private static String mapField(BillingDataRequestMessage data) {
        SeriesDataType dataType = data.getDataType();
        Preconditions.checkNotNull(dataType, "Data type must be specified.");
        SeriesType type = data.getType();
        Preconditions.checkNotNull(type, "Series type must be specified.");

        switch (type) {
            case CACHED:
                switch (dataType) {
                    case HITS:
                        return HitsDaily.CACHED;
                    case MISSES:
                        return HitsDaily.NOT_CACHED;
                }
                break;
            case WRITE:
            case READ:
            case P2P:
                switch (dataType) {
                    case BYTES:
                        return TransferredEntry.TRANSFERRED;
                    case COUNT:
                        return BaseEntry.COUNT;
                }
                break;
            case STORE:
            case RESTORE:
                switch (dataType) {
                    case BYTES:
                        return SizeEntry.SIZE;
                    case COUNT:
                        return BaseEntry.COUNT;
                }
                break;
            case CONNECTION:
                switch (dataType) {
                    case MAXSECS:
                        return DcacheTimeDaily.MAX_TIME;
                    case AVGSECS:
                        return DcacheTimeDaily.AVG_TIME;
                    case MINSECS:
                        return DcacheTimeDaily.MIN_TIME;
                }
            default:
                break;
        }

        throw new IllegalArgumentException("Cannot get " + dataType
                                                           + " + for histogram type "
                                                           + type);

    }

    private IBillingInfoAccess         access;
    private HourlyAggregateDataHandler hourlyAggregateDataHandler;
    private ExecutorService            executor;

    /**
     * <p>Asynchronous.  Returns reply future for the
     * data specified by a histogram request.</p>
     */
    public Reply messageArrived(BillingDataRequestMessage request) {
        MessageReply<Message> reply = new MessageReply<>();

        if (access == null ||
                        hourlyAggregateDataHandler == null ||
                        executor == null) {
            reply.fail(request, -1,
                       "No database connection; cannot "
                                       + "provide histogram data.");
        }

        executor.execute(() -> {
            try {
                getHistogram(request);
                reply.reply(request);
            } catch (Exception e) {
                reply.fail(request, e);
            }
        });

        return reply;

    }

    public void setAccess(IBillingInfoAccess access) {
        this.access = access;
    }

    public void setExecutionService(ExecutorService executor) {
        this.executor = executor;
    }

    public void setHourlyAggregateDataHandler(
                    HourlyAggregateDataHandler handler) {
        hourlyAggregateDataHandler = handler;
    }

    /**
     * <p>The hourly data is kept in memory, so the aggregate data handler
     * is called instead of the database access in that case.  Otherwise,
     * this is essentially a JDOQL pass-through.</p>
     */
    private <T extends BaseEntry> Collection<IHistogramData> getData(
                    Class<T> dataClass,
                    TimeFrame timeFrame) {
        BinType binType = timeFrame.getTimebin();

        if (binType == BinType.HOUR) {
            return hourlyAggregateDataHandler.get(dataClass);
        }

        Collection<T> c = access.get(dataClass,
                                     "date >= date1 && date <= date2",
                                     "java.util.Date date1, "
                                                     + "java.util.Date date2",
                                     timeFrame.getLow(), timeFrame.getHigh());
        Collection<IHistogramData> plotData = new ArrayList<>();
        plotData.addAll(c);
        return plotData;
    }

    private void getHistogram(BillingDataRequestMessage request) {
        Class<? extends BaseEntry> clzz = mapClass(request);
        TimeFrame timeFrame = request.getTimeFrame();
        Collection<IHistogramData> values = getData(clzz, timeFrame);

        String identifier = request.getType().name()
                        + "_" + timeFrame.getTimeframe();

        /*
         * Create histogram from specifications.
         */
        TimeseriesHistogram model = new TimeseriesHistogram();
        model.withTimeFrame(request.getTimeFrame());
        model.setIdentifier(identifier);
        model.setDataUnitLabel(request.getDataType().name());
        model.configure();

        /*
         * The DAO objects map double values to a particular field.
         * So for each such object (in order), we need to extract
         * the correct value and add it to the list of doubles.
         */
        String field = mapField(request);
        values.stream()
              .forEach((v) -> model.replace(v.data().get(field),
                                            v.timestamp().getTime()));

        /*
         * The histogram is a fixed one on the receiving end,
         * so we can eliminate the metadata.
         */
        model.setMetadata(null);

        request.setHistogram(model);
    }
}
