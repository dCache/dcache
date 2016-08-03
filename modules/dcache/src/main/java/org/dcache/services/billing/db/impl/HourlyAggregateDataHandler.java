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
package org.dcache.services.billing.db.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.BaseEntry;
import org.dcache.services.billing.db.data.DcacheReadsHourly;
import org.dcache.services.billing.db.data.DcacheTimeHourly;
import org.dcache.services.billing.db.data.DcacheWritesHourly;
import org.dcache.services.billing.db.data.HSMReadsHourly;
import org.dcache.services.billing.db.data.HSMWritesHourly;
import org.dcache.services.billing.db.data.HitsHourly;
import org.dcache.services.billing.db.data.MissesHourly;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.PoolHitsHourly;
import org.dcache.services.billing.db.data.PoolToPoolTransfersHourly;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.histograms.data.IHistogramData;

/**
 * <p>Stores hourly billing data in memory in a circular buffer (linked deque)
 *    holding 24 bins.</p>
 *
 * <p>Uses the DAO objects representing database views defined for hourly data
 *    to initialize the buffer, but thereafter all updates and fetches
 *    are directly to and from the buffer.</p>
 *
 * <p>Implemented in the interest of shortening latency on histogram fetches
 *    (for plotting).</p>
 */
public final class HourlyAggregateDataHandler {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(HourlyAggregateDataHandler.class);

    private static final long HOUR_IN_MILLIS = TimeUnit.HOURS.toMillis(1);

    /**
     * <p>Data object held by the circular buffer.</p>
     */
    static final class HourlyAggregateData {
        final Date timestamp;

        long bytesRead;
        long bytesWritten;
        long bytesP2p;
        long bytesStored;
        long bytesRestored;

        long maxConnectionTime;
        long minConnectionTime;

        long readCount;
        long writeCount;
        long p2pCount;
        long storeCount;
        long restoreCount;
        long moverCount;
        long cacheHits;
        long cacheMisses;

        double avgConnectionTime;

        HourlyAggregateData(long now) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(now);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            timestamp = cal.getTime();
            bytesRead = 0L;
            bytesWritten = 0L;
            bytesP2p = 0L;
            bytesRestored = 0L;
            bytesStored = 0L;
            avgConnectionTime = 0.0;
            maxConnectionTime = 0L;
            minConnectionTime = Long.MAX_VALUE;
            cacheHits = 0L;
            cacheMisses = 0L;
            readCount = 0L;
            writeCount = 0L;
            p2pCount = 0L;
            storeCount = 0L;
            restoreCount = 0L;
        }

        synchronized IHistogramData getHourlyConnectionTime() {
            DcacheTimeHourly entry = new DcacheTimeHourly();
            entry.setCount(moverCount);
            entry.setDate(timestamp);
            entry.setMaximum(maxConnectionTime);
            if (minConnectionTime == Long.MAX_VALUE) {
                entry.setMinimum(0L);
            } else {
                entry.setMinimum(minConnectionTime);
            }
            entry.setAverage(avgConnectionTime);
            return entry;
        }

        synchronized IHistogramData getHourlyP2ps() {
            PoolToPoolTransfersHourly entry = new PoolToPoolTransfersHourly();
            entry.setCount(p2pCount);
            entry.setDate(timestamp);
            entry.setTransferred(bytesP2p);
            return entry;
        }

        synchronized IHistogramData getHourlyReads() {
            DcacheReadsHourly entry = new DcacheReadsHourly();
            entry.setCount(readCount);
            entry.setDate(timestamp);
            entry.setTransferred(bytesRead);
            return entry;
        }

        synchronized IHistogramData getHourlyRestores() {
            HSMReadsHourly entry = new HSMReadsHourly();
            entry.setCount(restoreCount);
            entry.setDate(timestamp);
            entry.setSize(bytesRestored);
            return entry;
        }

        synchronized IHistogramData getHourlyStores() {
            HSMWritesHourly entry = new HSMWritesHourly();
            entry.setCount(storeCount);
            entry.setDate(timestamp);
            entry.setSize(bytesStored);
            return entry;
        }

        synchronized IHistogramData getHourlyWrites() {
            DcacheWritesHourly entry = new DcacheWritesHourly();
            entry.setCount(writeCount);
            entry.setDate(timestamp);
            entry.setTransferred(bytesWritten);
            return entry;
        }

        synchronized IHistogramData getPoolHits() {
            PoolHitsHourly entry = new PoolHitsHourly();
            entry.setCount(cacheHits+cacheMisses);
            entry.setDate(timestamp);
            entry.setCached(cacheHits);
            entry.setNotcached(cacheMisses);
            return entry;
        }

        public String toString() {
            long mintime = minConnectionTime == Long.MAX_VALUE ? 0L
                            : minConnectionTime;

            return "HourlyAggregateData"
                            + "(" + timestamp + ")"
                            + "(rd b " + bytesRead + ")"
                            + "(rd t " + readCount + ")"
                            + "(wr b" + bytesWritten + ")"
                            + "(wr t" + writeCount + ")"
                            + "(p2p b" + bytesP2p + ")"
                            + "(p2p t" + p2pCount + ")"
                            + "(st b " + bytesStored + ")"
                            + "(st t " + storeCount + ")"
                            + "(rst b" + bytesRestored + ")"
                            + "(rst t " + restoreCount + ")"
                            + "(max t " + maxConnectionTime + ")"
                            + "(min t " + mintime + ")"
                            + "(avg t " + avgConnectionTime + ")"
                            + "(tot t " + moverCount + ")"
                            + "(hits " + cacheHits + ")"
                            + "(misses " + cacheMisses + ")";
        }

        synchronized void update(IHistogramData data) {
            if (data instanceof MoverData) {
                update((MoverData)data);
            } else if (data instanceof StorageData) {
                update((StorageData)data);
            } else if (data instanceof PoolHitData) {
                update((PoolHitData)data);
            } else if (data instanceof DcacheReadsHourly) {
                update((DcacheReadsHourly)data);
            } else if (data instanceof DcacheWritesHourly) {
                update((DcacheWritesHourly)data);
            } else if (data instanceof DcacheTimeHourly) {
                update((DcacheTimeHourly)data);
            } else if (data instanceof PoolToPoolTransfersHourly) {
                update((PoolToPoolTransfersHourly)data);
            } else if (data instanceof HSMReadsHourly) {
                update((HSMReadsHourly)data);
            } else if (data instanceof HSMWritesHourly) {
                update((HSMWritesHourly)data);
            } else if (data instanceof HitsHourly) {
                update((HitsHourly)data);
            } else if (data instanceof MissesHourly) {
                update((MissesHourly)data);
            }
        }

        private void update(DcacheReadsHourly data) {
            readCount += data.getCount();
            bytesRead += data.getTransferred();
        }

        private void update(DcacheWritesHourly data) {
            writeCount += data.getCount();
            bytesWritten += data.getTransferred();
        }

        private void update(PoolToPoolTransfersHourly data) {
            p2pCount += data.getCount();
            bytesP2p += data.getTransferred();
        }

        private void update(DcacheTimeHourly data) {
            moverCount += data.getCount();
            maxConnectionTime = Math.max(maxConnectionTime, data.getMaximum());
            minConnectionTime = Math.min(minConnectionTime, data.getMinimum());
            avgConnectionTime += data.getAverage();
        }

        private void update(HSMReadsHourly data) {
            restoreCount += data.getCount();
            bytesRestored += data.getSize();
        }

        private void update(HSMWritesHourly data) {
            storeCount += data.getCount();
            bytesStored += data.getSize();
        }

        private void update(HitsHourly data) {
            cacheHits += data.getCount();
        }

        private void update(MissesHourly data) {
            cacheMisses += data.getCount();
        }

        private void update(MoverData data) {
            if (data.getErrorCode() != 0) {
                return;
            }

            if (data.isP2p()) {
                bytesP2p += data.getTransferSize();
                ++p2pCount;
            } else if (data.getIsNew()) {
                bytesWritten += data.getTransferSize();
                ++writeCount;
            } else {
                bytesRead += data.getTransferSize();
                ++readCount;
            }

            long t = data.getConnectionTime();
            maxConnectionTime = Math.max(maxConnectionTime, t);
            minConnectionTime = Math.min(minConnectionTime, t);

            double currentTotal = moverCount*avgConnectionTime;
            ++moverCount;
            avgConnectionTime = (currentTotal + (double)t)/moverCount;
        }

        private void update(StorageData data) {
            if (data.getErrorCode() != 0) {
                return;
            }

            if (data.getAction().equals("store")) {
                bytesStored += data.getFullSize();
                ++storeCount;
            } else {
                bytesRestored += data.getFullSize();
                ++restoreCount;
            }
        }

        private void update(PoolHitData data) {
            if (data.getErrorCode() != 0) {
                return;
            }

            if (data.getFileCached()) {
                ++cacheHits;
            } else {
                ++cacheMisses;
            }
        }
    }

    /**
     * <p>Circular buffer holding 24 1-hour bins.</p>
     */
    private final Deque<HourlyAggregateData> deque = new ConcurrentLinkedDeque<>();

    /**
     * <p>Needed only to populate the buffer at startup.</p>
     */
    private IBillingInfoAccess access;

    public void initialize() {
        /*
         *  Seed the deque so that it contains the bins for the current
         *  24-hour interval.
         */
        deque.clear();

        long now = System.currentTimeMillis();
        for (int i = 0; i < 24; i++) {
            long binTime = now - TimeUnit.HOURS.toMillis(i);
            deque.addFirst(new HourlyAggregateData(binTime));
        }

        new Thread(this::populateFromViews, "View-initializer").start();
    }

    /**
     * <p>Finds the proper time bin and updates the pertinent aggregate
     *    fields.  If the timestamp is prior to that of the earliest bin,
     *    or later than the upper bound, the data is dropped.</p>
     */
    public void update(IHistogramData newData) {
        Date timestamp = newData.timestamp();
        timestamp = timestamp == null ? new Date() : timestamp;

        HourlyAggregateData bin = getBin(timestamp.getTime());

        if (bin != null) {
            LOGGER.trace("update bin {}, data {}.", bin, newData);
            bin.update(newData);
        }
    }

    /**
     * <p>Serves up histogram data.</p>
     *
     * @param type specifies which hourly data to fetch.
     * @return collection of 24 hourly histograms of the given type.
     */
    public <T extends BaseEntry> Collection<IHistogramData> get(Class<T> type) {
        Collection<IHistogramData> data = Collections.emptyList();
        if (type.equals(DcacheReadsHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getHourlyReads)
                                 .collect(Collectors.toList());
        } else if (type.equals(DcacheWritesHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getHourlyWrites)
                                 .collect(Collectors.toList());
        } else if (type.equals(PoolToPoolTransfersHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getHourlyP2ps)
                                 .collect(Collectors.toList());
        } else if (type.equals(DcacheTimeHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getHourlyConnectionTime)
                                 .collect(Collectors.toList());
        } else if (type.equals(HSMReadsHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getHourlyRestores)
                                 .collect(Collectors.toList());
        } else if (type.equals(HSMWritesHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getHourlyStores)
                                 .collect(Collectors.toList());
        } else if (type.equals(PoolHitsHourly.class)) {
            data = deque.stream().map(HourlyAggregateData::getPoolHits)
                                 .collect(Collectors.toList());
        }

        LOGGER.trace("get request, returning {}.", data);
        return data;
    }

    public void setAccess(IBillingInfoAccess access) {
        this.access = access;
    }

    private HourlyAggregateData getBin(long timestamp) {
        shiftBufferIfLastBinIsStale();

        long earliest = deque.peekFirst().timestamp.getTime();
        long latest = deque.peekLast().timestamp.getTime()
                        + TimeUnit.HOURS.toMillis(1);

        /*
         * If the (live) data is outside the current 24-hour window, we just
         * throw it away.  This is rather unlikely to occur.
         */
        if (timestamp < earliest || timestamp >= latest ) {
            return null;
        }

        Iterator<HourlyAggregateData> i = deque.descendingIterator();

        /*
         * There should always be 24 bins.  If there aren't, the
         * iterator error would be a bug, so we let it propagate here.
         *
         * For the vast majority of time, live incoming values should be added
         * to the last bin (one possible exception would be if the timestamp of
         * the arriving data precedes the last hourly timestamp because we
         * have just shifted the buffer forward).
         *
         * On initialization, we need to search for the correct bin as
         * the view might have gaps.
         */
        HourlyAggregateData next = i.next();

        do {
            if (timestamp >= next.timestamp.getTime()) {
                break;
            }
            next = i.next();
        } while (i.hasNext());

        return next;
    }

    private void populateFromViews() {
        /*
         *  The classes here are mapped to the database views.
         *  These are only accessed once, on startup.
         */
        access.get(DcacheReadsHourly.class).stream().forEach(this::update);
        access.get(DcacheWritesHourly.class).stream().forEach(this::update);
        access.get(PoolToPoolTransfersHourly.class).stream().forEach(this::update);
        access.get(DcacheTimeHourly.class).stream().forEach(this::update);
        access.get(HSMReadsHourly.class).stream().forEach(this::update);
        access.get(HSMWritesHourly.class).stream().forEach(this::update);
        access.get(HitsHourly.class).stream().forEach(this::update);
        access.get(MissesHourly.class).stream().forEach(this::update);
    }

    private void shiftBufferIfLastBinIsStale() {
        long latest = deque.peekLast().timestamp.getTime();

        if (System.currentTimeMillis() - latest > HOUR_IN_MILLIS) {
            deque.removeFirst();
            latest += HOUR_IN_MILLIS;
            deque.addLast(new HourlyAggregateData(latest));
        }
    }
}
