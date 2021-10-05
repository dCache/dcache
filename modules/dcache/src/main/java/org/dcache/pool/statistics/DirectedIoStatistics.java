/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017-2020 Deutsches Elektronen-Synchrotron
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

import static org.dcache.util.TimeUtils.describe;

import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;


/**
 * An immutable snapshot of statistics describing the channel usage in a particular direction:
 * either read- or write operations.
 */
public class DirectedIoStatistics {

    private final SnapshotStatistics _statistics;
    private final Duration _idle;
    private final Duration _active;
    private final Instant _firstAccess;
    private final Instant _latestAccess;
    private final Duration _preActivityWait;
    private final Duration _postActivityWait;

    public DirectedIoStatistics(Duration preActivityWait, Duration idle, Duration active,
          Instant firstAccess, Instant latestAccess, Duration postActivityWait,
          LiveStatistics statistics) {
        _preActivityWait = preActivityWait;
        _idle = idle;
        _active = active;
        _firstAccess = firstAccess;
        _latestAccess = latestAccess;
        _statistics = statistics.snapshot();
        _postActivityWait = postActivityWait;
    }

    /**
     * The collection of statistics gathered about IO operations.
     */
    public SnapshotStatistics statistics() {
        return _statistics;
    }

    /**
     * The time spent after the channel was created ("file was opened") waiting for the first IO
     * operation.  This value includes any time that the pool spent waiting for the client to
     * connect.
     * <p>
     * If there has not been any IO operation (yet) then the returned value is the total time since
     * the mover was created.  This value will not change once IO operations have started.
     *
     * @return time spent waiting for the first IO operation.
     */
    public Duration preActivity() {
        return _preActivityWait;
    }

    /**
     * The cumulative time spent while sending data to the client with no in-flight IO requests.  If
     * the mover is single-threaded (only ever one IO operation at a time) then this value includes
     * the time spent sending data over the network.
     * <p>
     * If there has not been any IO operations (yet) then the returned value is {@literal
     * Duration.ZERO}.  If a transfer is on-going then the returned value is the idle time so far,
     * treating the most recent IO operation as if it were the last for this transfer.
     */
    public Duration idle() {
        return _idle;
    }

    /**
     * The cumulative time spent during the transfer with at least one active IO request.  If the
     * mover is single-threaded (only ever one IO operation at a time) then this value shows for how
     * long the transfer was stalled while waiting to read data from the underlying storage.
     * <p>
     * If there has not been any IO operations (yet) then the returned value is {@literal
     * Duration.ZERO}.  If a transfer is on-going then the returned value is the time spent waiting
     * for IO so far.
     */
    public Duration active() {
        return _active;
    }

    /**
     * The time spent after the last IO operation before the channel was closed. The value includes
     * any time the pool spends processing after the last IO operation and before the file was
     * closed.
     * <p>
     * If there has not been any IO operations (yet) then the returned value is {@literal
     * Duration.ZERO}.  If a transfer is on-going then the returned value is the time spend since
     * the last IO operation.
     */
    public Duration postActivity() {
        return _postActivityWait;
    }

    public void getInfo(PrintWriter pw) {
        pw.println("Pre-activity wait: " + describe(_preActivityWait).orElse("none"));
        pw.println("First request: " + org.dcache.util.Strings.describe(
              Optional.ofNullable(_firstAccess)));
        pw.println("Latest request: " + org.dcache.util.Strings.describe(
              Optional.ofNullable(_latestAccess)));
        pw.println("Active: " + describe(_active).orElse("never active"));
        pw.println("Idle: " + describe(_idle).orElse("never idle"));
        pw.println("Post-activity wait: " + describe(_postActivityWait).orElse("none"));
        _statistics.getInfo(pw);
    }
}
