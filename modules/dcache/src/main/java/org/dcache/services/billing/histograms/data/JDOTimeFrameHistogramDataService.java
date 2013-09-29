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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
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
import org.dcache.services.billing.db.data.SizeEntry;
import org.dcache.services.billing.db.data.HitsDaily;
import org.dcache.services.billing.db.data.HitsHourly;
import org.dcache.services.billing.db.data.MissesHourly;
import org.dcache.services.billing.db.data.PoolToPoolTransfersDaily;
import org.dcache.services.billing.db.data.PoolToPoolTransfersHourly;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.histograms.TimeFrame;
import org.dcache.services.billing.histograms.TimeFrame.BinType;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramData.HistogramDataType;
import org.dcache.services.billing.histograms.exceptions.TimeFrameHistogramException;

/**
 * Implementation of service interface which accesses {@link IBillingInfoAccess}
 * , a JDO-based DAO layer.
 *
 * @author arossi
 */
public final class JDOTimeFrameHistogramDataService implements
                ITimeFrameHistogramDataService {

    /**
     * Stand-in aggregate object for hits data.
     */
    private static class HourlyHitData extends BaseEntry {
        private static final long serialVersionUID = -4776963573729329237L;
        private Long cached = 0L;
        private Long notcached = 0L;

        @Override
        public Map<String, Double> data() {
            Map<String, Double> dataMap = super.data();
            dataMap.put(HitsDaily.CACHED, cached.doubleValue());
            dataMap.put(HitsDaily.NOT_CACHED, notcached.doubleValue());
            return dataMap;
        }

        @Override
        public String toString() {
            return "(" + dateString() + "," + cached + "," + notcached + ")";
        }
    }

    private IBillingInfoAccess access;

    @Override
    public TimeFrameHistogramData[] getDcBytesHistogram(TimeFrame timeFrame,
                    Boolean write) throws TimeFrameHistogramException {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.BYTES_UPLOADED
                        : HistogramDataType.BYTES_DOWNLOADED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = getViewData(DcacheWritesHourly.class);
            } else {
                plotData = getViewData(DcacheReadsHourly.class);
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
        histogram[0].setDfactor(GB);
        histogram[0].setField(SizeEntry.SIZE);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getDcConnectTimeHistograms(
                    TimeFrame timeFrame) throws TimeFrameHistogramException {
        Collection<IHistogramData> plotData;

        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getViewData(DcacheTimeHourly.class);
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
                    TimeFrame timeFrame, Boolean write)
                    throws TimeFrameHistogramException {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.TRANSFERS_UPLOADED
                        : HistogramDataType.TRANSFERS_DOWNLOADED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = getViewData(DcacheWritesHourly.class);
            } else {
                plotData = getViewData(DcacheReadsHourly.class);
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
    public TimeFrameHistogramData[] getHitHistograms(TimeFrame timeFrame)
                    throws TimeFrameHistogramException {
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getHourlyAggregateForHits();
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
                    Boolean write) throws TimeFrameHistogramException {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.BYTES_STORED
                        : HistogramDataType.BYTES_RESTORED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = getViewData(HSMWritesHourly.class);
            } else {
                plotData = getViewData(HSMReadsHourly.class);
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
        histogram[0].setDfactor(GB);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getHsmTransfersHistogram(
                    TimeFrame timeFrame, Boolean write)
                    throws TimeFrameHistogramException {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(write ? HistogramDataType.TRANSFERS_STORED
                        : HistogramDataType.TRANSFERS_RESTORED);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            if (write) {
                plotData = getViewData(HSMWritesHourly.class);
            } else {
                plotData = getViewData(HSMReadsHourly.class);
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
    public TimeFrameHistogramData[] getP2pBytesHistogram(TimeFrame timeFrame)
                    throws TimeFrameHistogramException {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(HistogramDataType.BYTES_P2P);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getViewData(PoolToPoolTransfersHourly.class);
        } else {
            plotData = getCoarseGrainedData(PoolToPoolTransfersDaily.class,
                            timeFrame);
        }
        histogram[0].setData(plotData);
        histogram[0].setField(SizeEntry.SIZE);
        histogram[0].setDfactor(GB);
        return histogram;
    }

    @Override
    public TimeFrameHistogramData[] getP2pTransfersHistogram(TimeFrame timeFrame)
                    throws TimeFrameHistogramException {
        TimeFrameHistogramData[] histogram
            = new TimeFrameHistogramData[] { new TimeFrameHistogramData() };
        histogram[0].setType(HistogramDataType.TRANSFERS_P2P);
        Collection<IHistogramData> plotData;
        if (BinType.HOUR == timeFrame.getTimebin()) {
            plotData = getViewData(PoolToPoolTransfersHourly.class);
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

    private <T extends IHistogramData> Collection<IHistogramData> getCoarseGrainedData(
                    Class<T> clzz, TimeFrame timeFrame)
                    throws TimeFrameHistogramException {
        return getData(clzz, "date >= date1 && date <= date2",
                        "java.util.Date date1, java.util.Date date2",
                        timeFrame.getLow(), timeFrame.getHigh());
    }

    private <T extends IHistogramData> Collection<IHistogramData> getViewData(
                    Class<T> clzz) throws TimeFrameHistogramException {
        try {
            Collection<T> c = access.get(clzz);
            Collection<IHistogramData> plotData = new ArrayList<IHistogramData>();
            plotData.addAll(c);
            return plotData;
        } catch (BillingQueryException t) {
            throw new TimeFrameHistogramException(t);
        }
    }

    private <T extends IHistogramData> Collection<IHistogramData> getData(
                    Class<T> clzz, String filter, String params,
                    Object... values) throws TimeFrameHistogramException {
        Collection<T> c;
        try {
            c = access.get(clzz, filter, params, values);
        } catch (BillingQueryException t) {
            throw new TimeFrameHistogramException(t.getMessage(), t);
        }
        Collection<IHistogramData> plotData = new ArrayList<>();
        plotData.addAll(c);
        return plotData;
    }

    private Collection<IHistogramData> getHourlyAggregateForHits()
                    throws TimeFrameHistogramException {
        Map<String, HourlyHitData> hourlyAggregate
            = new TreeMap<String, HourlyHitData>();
        Collection<IHistogramData> histogramData = getViewData(HitsHourly.class);
        for (IHistogramData d : histogramData) {
            Date date = normalizeForHour(d.timestamp());
            String key = date.toString();
            HourlyHitData hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new HourlyHitData();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            long count = d.data().get(BaseEntry.COUNT).longValue();
            hourlyData.cached += count;
        }
        histogramData = getViewData(MissesHourly.class);
        for (IHistogramData d : histogramData) {
            Date date = normalizeForHour(d.timestamp());
            String key = date.toString();
            HourlyHitData hourlyData = hourlyAggregate.get(key);
            if (hourlyData == null) {
                hourlyData = new HourlyHitData();
                hourlyData.setDate(date);
                hourlyAggregate.put(key, hourlyData);
            }
            long count = d.data().get(BaseEntry.COUNT).longValue();
            hourlyData.notcached += count;
        }
        histogramData = new ArrayList<IHistogramData>();
        histogramData.addAll(hourlyAggregate.values());
        return histogramData;
    }

    /**
     * Rounds down to beginning of the hour in which the timestamp is bounded.
     *
     * @return normalized date (hh:00:00.000)
     */
    private Date normalizeForHour(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
}
