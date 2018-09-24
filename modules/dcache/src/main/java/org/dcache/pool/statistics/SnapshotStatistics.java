/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.statistics;

import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;

import java.io.PrintWriter;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.dcache.util.Strings.describeBandwidth;
import static org.dcache.util.Strings.describeInteger;
import static org.dcache.util.Strings.describeSize;
import static org.dcache.util.TimeUtils.describeDuration;

/**
 * Provides a snapshot of the information maintained within LiveStatistics.
 * This class makes use of Apache Commons Math StatisticalSummary class to hold
 * statistics on certain metrics.
 */
public class SnapshotStatistics
{
    private static final StatisticalSummary NO_RESULTS = new StatisticalSummaryValues(
            Double.NaN, Double.NaN, 0L, Double.NaN, Double.NaN, Double.NaN);

    private final StatisticalSummary _instantaneousBandwidth;
    private final StatisticalSummary _duration;
    private final StatisticalSummary _requestedBytes;
    private final StatisticalSummary _transferredBytes;
    private final StatisticalSummary _concurrency;

    public SnapshotStatistics()
    {
        this(NO_RESULTS, NO_RESULTS, NO_RESULTS, NO_RESULTS, NO_RESULTS);
    }

    public SnapshotStatistics(
            StatisticalSummary instantaneousBandwidth,
            StatisticalSummary duration,
            StatisticalSummary requestedBytes,
            StatisticalSummary transferredBytes,
            StatisticalSummary concurrency)
    {
        _instantaneousBandwidth = instantaneousBandwidth;
        _duration = duration;
        _requestedBytes = requestedBytes;
        _transferredBytes = transferredBytes;
        _concurrency = concurrency;
    }

    /**
     * Statistics about the requested bytes.
     */
    public StatisticalSummary requestedBytes()
    {
        return _requestedBytes;
    }

    /**
     * Statistics about the bytes actually transferred.
     */
    public StatisticalSummary transferredBytes()
    {
        return _transferredBytes;
    }

    /**
     * Statistics for the instantaneous (per IO request) bandwidth.  The
     * instantaneous bandwidth is the number of bytes transferred divided by
     * the time for the IO operation to complete.  Values are in bytes per second.
     * <p>
     * Note that, if IO operations complete in less than the clock granularity
     * then that operation does not contribute to the instanteneous bandwidth
     * statistics, which results in fewer observations
     * ({@link StatisticalSummary#getN}) than for other metrics.
     */
    public StatisticalSummary instantaneousBandwidth()
    {
        return _instantaneousBandwidth;
    }

    /**
     * Statistics about the concurrency, as mesured at the start of each IO
     * request of this type.  A maximum value of 1 indicates that there was no
     * overlapping IO requests of this type.
     */
    public StatisticalSummary concurrency()
    {
        return _concurrency;
    }

    /**
     * Statistics about the time spend processing IO requests of this type.
     * Numerical values are in nanoseconds.
     */
    public StatisticalSummary IOTime()
    {
        return _duration;
    }

    public void getInfo(PrintWriter pw)
    {
        if (_instantaneousBandwidth.getN() > 0) {
            pw.println("Instantaneous bandwidth: " + describeBandwidth(_instantaneousBandwidth));
        }
        if (_duration.getN() > 0) {
            pw.println("IO wait time: " + describeDuration(_duration, NANOSECONDS));
        }
        if (_requestedBytes.getN() > 0) {
            pw.println("IO requested size: " + describeSize(_requestedBytes));
        }
        if (_transferredBytes.getN() > 0) {
            pw.println("IO transferred size: " + describeSize(_transferredBytes));
        }
        if (_concurrency.getN() > 0) {
            pw.println("Concurrency: " + describeInteger(_concurrency));
        }
    }
}
