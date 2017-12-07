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

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import static com.google.common.base.Preconditions.checkArgument;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Storage for the mutable statistics.  These gathered statistics are intended
 * for a single IO direction: either for read operations or for write
 * operations.  The {@code #accept} method updates the statistics based on the
 * observed behaviour of an IO operation (either read or write) and the getter
 * methods provide immutable snapshots of various metrics.
 * <p>
 * This class protects against concurrent updates and obtaining a snapshot of a
 * metric's statistics.  External synchronisation is needed if multiple snapshot
 * statistics are desired without any intermediate updates.
 */
public class LiveStatistics
{
    private final SummaryStatistics _instantaneousBandwidth = new SummaryStatistics();
    private final SummaryStatistics _requestedBytes = new SummaryStatistics();
    private final SummaryStatistics _transferredBytes = new SummaryStatistics();
    private final SummaryStatistics _duration = new SummaryStatistics();
    private final SummaryStatistics _concurrency = new SummaryStatistics();

    /**
     * Provide a snapshot of current state of the monitored statistics.
     */
    public synchronized SnapshotStatistics snapshot()
    {
        return new SnapshotStatistics(
                _instantaneousBandwidth,
                _duration,
                _requestedBytes,
                _transferredBytes,
                _concurrency);
    }

    /**
     * Accept information about an IO operation so it is used to update
     * statistics.
     * @param concurrency the number of in-flight requests, including the
     * reported operation, when the operation was initiated.
     * @param requestedBytes the number of bytes requests in the IO operation
     * @param transferredBytes the number of bytes transferred in the IO operation.
     * @param startedAt the value of System.nanoTime() immediately before
     * starting the IO operation.
     */
    public synchronized void accept(int concurrency, long requestedBytes,
            long transferredBytes, long startedAt)
    {
        checkArgument(concurrency > 0);
        checkArgument(requestedBytes >= 0);
        checkArgument(transferredBytes >= 0);

        long duration = System.nanoTime() - startedAt;

        double instantaneousBandwidth = SECONDS.getDuration().toNanos()
                * (double) transferredBytes / duration;

        _duration.addValue(duration);
        _transferredBytes.addValue(transferredBytes);
        _requestedBytes.addValue(requestedBytes);
        _instantaneousBandwidth.addValue(instantaneousBandwidth);
        _concurrency.addValue(concurrency);
    }
}

