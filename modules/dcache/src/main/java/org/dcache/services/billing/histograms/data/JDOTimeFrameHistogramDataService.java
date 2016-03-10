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
package org.dcache.services.billing.histograms.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

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
import org.dcache.services.billing.db.data.PoolHitsHourly;
import org.dcache.services.billing.db.data.PoolToPoolTransfersDaily;
import org.dcache.services.billing.db.data.PoolToPoolTransfersHourly;
import org.dcache.services.billing.db.data.SizeEntry;
import org.dcache.services.billing.db.data.TransferredEntry;
import org.dcache.services.billing.db.impl.HourlyAggregateDataHandler;
import org.dcache.services.billing.histograms.TimeFrame;
import org.dcache.services.billing.histograms.TimeFrame.BinType;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramData.HistogramDataType;

import static org.dcache.util.ByteUnit.GiB;

/**
 * Implementation of service interface which accesses {@link IBillingInfoAccess}
 * , a JDO-based DAO layer.
 *
 * @author arossi
 */
public final class JDOTimeFrameHistogramDataService implements
                ITimeFrameHistogramDataService {

    private IBillingInfoAccess         access;
    private HourlyAggregateDataHandler hourlyAggregateDataHandler;

    @Override
    public TimeFrameHistogramData[] getDcBytesHistogram(TimeFrame timeFrame,
                    Boolean write) {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.BYTES_UPLOADED
                        : HistogramDataType.BYTES_DOWNLOADED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = hourlyAggregateDataHandler.get(DcacheWritesHourly.class);
            } else {
                plotData = hourlyAggregateDataHandler.get(DcacheReadsHourly.class);
            }
        } else {
            if (write) {
                plotData = getCoarseGrainedData(DcacheWritesDaily.class,
                                timeFrame);
            } else {
                plotData = getCoarseGrainedData(DcacheReadsDaily.class,
                                timeFrame);
            }
        }
        histogram[0].setData(plotData);
        histogram[0].setDfactor(GiB.toBytes(1.0d));
        histogram[0].setField(TransferredEntry.TRANSFERRED);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getDcConnectTimeHistograms(
                    TimeFrame timeFrame) {
        Collection<IHistogramData> plotData;

        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = hourlyAggregateDataHandler.get(DcacheTimeHourly.class);
        } else {
            plotData = getCoarseGrainedData(DcacheTimeDaily.class, timeFrame);
        }

        HistogramDataType[] type = new HistogramDataType[] {
                        HistogramDataType.TIME_MAX,
                        HistogramDataType.TIME_AVG,
                        HistogramDataType.TIME_MIN };

        String[] field = new String[] { DcacheTimeDaily.MAX_TIME,
                        DcacheTimeDaily.AVG_TIME, DcacheTimeDaily.MIN_TIME };

        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData(),
                                             new TimeFrameHistogramData(),
                                             new TimeFrameHistogramData() };
        for (int h = 0; h < histogram.length; h++) {
            histogram[h].setData(plotData);
            histogram[h].setField(field[h]);
            histogram[h].setDfactor(1.0 * TimeUnit.SECONDS.toMillis(1));
            histogram[h].setType(type[h]);
        }
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getDcTransfersHistogram(
                    TimeFrame timeFrame, Boolean write) {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.TRANSFERS_UPLOADED
                        : HistogramDataType.TRANSFERS_DOWNLOADED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = hourlyAggregateDataHandler.get(DcacheWritesHourly.class);
            } else {
                plotData = hourlyAggregateDataHandler.get(DcacheReadsHourly.class);
            }
        } else {
            if (write) {
                plotData = getCoarseGrainedData(DcacheWritesDaily.class,
                                timeFrame);
            } else {
                plotData = getCoarseGrainedData(DcacheReadsDaily.class,
                                timeFrame);
            }
        }
        histogram[0].setData(plotData);
        histogram[0].setField(BaseEntry.COUNT);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getHitHistograms(TimeFrame timeFrame) {
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = hourlyAggregateDataHandler.get(PoolHitsHourly.class);
        } else {
            plotData = getCoarseGrainedData(HitsDaily.class, timeFrame);
        }
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData(),
                                             new TimeFrameHistogramData() };
        histogram[0].setData(plotData);
        histogram[0].setField(HitsDaily.CACHED);
        histogram[0].setType(HistogramDataType.CACHED);
        histogram[1].setData(plotData);
        histogram[1].setField(HitsDaily.NOT_CACHED);
        histogram[1].setType(HistogramDataType.NOT_CACHED);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getHsmBytesHistogram(TimeFrame timeFrame,
                    Boolean write) {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.BYTES_STORED
                        : HistogramDataType.BYTES_RESTORED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = hourlyAggregateDataHandler.get(HSMWritesHourly.class);
            } else {
                plotData = hourlyAggregateDataHandler.get(HSMReadsHourly.class);
            }
        } else {
            if (write) {
                plotData = getCoarseGrainedData(HSMWritesDaily.class, timeFrame);
            } else {
                plotData = getCoarseGrainedData(HSMReadsDaily.class, timeFrame);
            }
        }
        histogram[0].setData(plotData);
        histogram[0].setField(SizeEntry.SIZE);
        histogram[0].setDfactor(GiB.toBytes(1.0d));
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getHsmTransfersHistogram(
                    TimeFrame timeFrame, Boolean write) {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.TRANSFERS_STORED
                        : HistogramDataType.TRANSFERS_RESTORED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = hourlyAggregateDataHandler.get(HSMWritesHourly.class);
            } else {
                plotData = hourlyAggregateDataHandler.get(HSMReadsHourly.class);
            }
        } else {
            if (write) {
                plotData = getCoarseGrainedData(HSMWritesDaily.class, timeFrame);
            } else {
                plotData = getCoarseGrainedData(HSMReadsDaily.class, timeFrame);
            }
        }
        histogram[0].setData(plotData);
        histogram[0].setField(BaseEntry.COUNT);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getP2pBytesHistogram(TimeFrame timeFrame) {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(HistogramDataType.BYTES_P2P);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = hourlyAggregateDataHandler.get(PoolToPoolTransfersHourly.class);
        } else {
            plotData = getCoarseGrainedData(PoolToPoolTransfersDaily.class,
                            timeFrame);
        }
        histogram[0].setData(plotData);
        histogram[0].setField(TransferredEntry.TRANSFERRED);
        histogram[0].setDfactor(GiB.toBytes(1.0d));
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getP2pTransfersHistogram(TimeFrame timeFrame) {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(HistogramDataType.TRANSFERS_P2P);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = hourlyAggregateDataHandler.get(PoolToPoolTransfersHourly.class);
        } else {
            plotData = getCoarseGrainedData(PoolToPoolTransfersDaily.class,
                            timeFrame);
        }
        histogram[0].setData(plotData);
        histogram[0].setField(BaseEntry.COUNT);
        return histogram;
    }

    public void setAccess(IBillingInfoAccess access) {
        this.access = access;
    }

    public void setHourlyAggregateDataHandler(HourlyAggregateDataHandler handler) {
        hourlyAggregateDataHandler = handler;
    }

    private <T extends IHistogramData> Collection<IHistogramData> getCoarseGrainedData(
                    Class<T> clzz, TimeFrame timeFrame) {
        return getData(clzz, "date >= date1 && date <= date2",
                        "java.util.Date date1, java.util.Date date2",
                        timeFrame.getLow(), timeFrame.getHigh());
    }

    private <T extends IHistogramData> Collection<IHistogramData> getData(
                    Class<T> clzz, String filter, String params,
                    Object... values) {
        Collection<T> c = access.get(clzz, filter, params, values);
        Collection<IHistogramData> plotData = new ArrayList<>();
        plotData.addAll(c);
        return plotData;
    }
}
