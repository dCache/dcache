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

/**
 * An immutable snapshot of statistics describing the channel usage since it
 * was created.  Statistics of both read and write activity are provided;
 * although, in many cases, a channel is exclusively used in one direction.
 */
public class IoStatistics
{
    private final DirectedIoStatistics reads;
    private final DirectedIoStatistics writes;

    public IoStatistics()
    {
        reads = new DirectedIoStatistics();
        writes = new DirectedIoStatistics();
    }

    public IoStatistics(DirectedIoStatistics reads, DirectedIoStatistics writes)
    {
        this.reads = reads;
        this.writes = writes;
    }

    public DirectedIoStatistics reads()
    {
        return reads;
    }

    public DirectedIoStatistics writes()
    {
        return writes;
    }
}
