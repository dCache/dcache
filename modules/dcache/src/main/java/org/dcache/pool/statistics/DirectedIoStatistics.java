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

import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.dcache.util.TimeUtils.describe;


/**
 * An immutable snapshot of statistics describing the channel usage in a
 * particular direction: either read- or write operations.
 */
public class DirectedIoStatistics
{
    private final SnapshotStatistics _statistics;
    private final Duration _idle;
    private final Duration _active;
    private final Instant _firstAccess;
    private final Instant _latestAccess;

    public DirectedIoStatistics()
    {
        _idle = Duration.ZERO;
        _active = Duration.ZERO;
        _firstAccess = null;
        _latestAccess = null;
        _statistics = new SnapshotStatistics();
    }

    public DirectedIoStatistics(Duration idle, Duration active,
            Instant firstAccess, Instant latestAccess,
            LiveStatistics statistics)
    {
        _idle = idle;
        _active = active;
        _firstAccess = firstAccess;
        _latestAccess = latestAccess;
        _statistics = statistics.snapshot();
    }

    /**
     * The collection of statistics gathered about IO operations.
     */
    public SnapshotStatistics statistics()
    {
        return _statistics;
    }

    /**
     * Time spent with no IO requests of this type (read or write).
     */
    public Duration idle()
    {
        return _idle;
    }

    /**
     * Time spent with at least one active IO request of this type (read or
     * write).
     */
    public Duration active()
    {
        return _active;
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("First request: " + org.dcache.util.Strings.describe(Optional.ofNullable(_firstAccess)));
        pw.println("Latest request: " + org.dcache.util.Strings.describe(Optional.ofNullable(_latestAccess)));
        pw.println("Active: " + describe(_active).orElse("never active"));
        pw.println("Idle: " + describe(_idle).orElse("never idle"));
        _statistics.getInfo(pw);
    }
}
