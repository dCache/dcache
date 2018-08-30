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

import org.dcache.util.LineIndentingPrintWriter;

import static org.dcache.util.Strings.toThreeSigFig;

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

    public boolean hasReads()
    {
        return reads.statistics().requestedBytes().getN() > 0;
    }

    public boolean hasWrites()
    {
        return writes.statistics().requestedBytes().getN() > 0;
    }

    private static String ratioDescription(long reads, long writes)
    {
        if (reads <= writes) {
            return toThreeSigFig(writes / (double)reads, 1000)
                    + " writes for every read (on average)";
        } else {
            return toThreeSigFig(reads / (double)writes, 1000)
                    + " reads for every write (on average)";
        }
    }

    private static String percent(long n, long total)
    {
        return toThreeSigFig(100 * n / (double)total, 1000) + "%";
    }

    public void getInfo(PrintWriter pw)
    {
        long readCount = reads.statistics().requestedBytes().getN();
        long writeCount = writes.statistics().requestedBytes().getN();

        if (hasReads() && hasWrites()) {
            long totalCount = readCount + writeCount;
            PrintWriter indented = new LineIndentingPrintWriter(pw, "    ");

            pw.println("Request ratio: " + ratioDescription(readCount, writeCount));

            pw.println("Read statistics:");
            indented.println("Requests: " + readCount + " (" + percent(readCount, totalCount) + " of all requests)");
            reads.getInfo(indented);

            pw.println("Write statistics:");
            indented.println("Requests: " + writeCount + " (" + percent(writeCount, totalCount) + " of all requests)");
            writes.getInfo(indented);
        } else if (hasReads()) {
            pw.println("Requests: " + readCount);
            reads.getInfo(pw);
        } else if (hasWrites()) {
            pw.println("Requests: " + writeCount);
            writes.getInfo(pw);
        }
    }
}
