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
package org.dcache.util.histograms;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;

import java.util.ArrayList;

/**
 * <p>Maintains a histogram data set which consists of a fixed number of
 * time value bins; the window is maintained by rotating a circular
 * buffer.</p>
 */
public class TimeseriesHistogram extends HistogramModel {

    public TimeseriesHistogram() {
    }

    public TimeseriesHistogram(TimeseriesHistogram copy) {
        super(copy);
    }

    @Override
    public void configure() {
        Preconditions.checkNotNull(identifier,
                                   "histogram type must be defined.");
        Preconditions.checkNotNull(binCount,
                                   "bin count must be defined.");
        Preconditions.checkArgument(binCount > 1,
                                    "bin count must be > 1.");
        Preconditions.checkNotNull(binUnit,
                                   "bin unit must be defined.");
        Preconditions.checkArgument(binUnit > 0,
                                    "bin unit must be > 0.");
        Preconditions.checkNotNull(highestBin,
                                   "highest bin must be defined.");

        setBinWidth();
        setBinSize();

        setLowestFromHighest();

        boolean emptyData = true;

        if (data == null) {
            data = new ArrayList<>();
        } else {
            data = new ArrayList<>(data);
            emptyData = data.isEmpty();
        }

        if (emptyData) {
            for (int i = 0; i < binCount; ++i) {
                data.add(null);
            }
        }

        String error = "bin count %s does not match array size %s.";
        Preconditions.checkArgument(data.size() == binCount,
                                    String.format(error,
                                                  binCount,
                                                  data.size()));

        if (metadata == null) {
            metadata = new HistogramMetadata(binCount);
            if (!emptyData) {
                long now = System.currentTimeMillis();
                for (int i = 0; i < binCount; ++i) {
                    metadata.updateCountForBin(i, now);
                }
            }
            updateStatistics();
        }
    }

    /**
     * <p>Find from the timestamp where to insert the data (which bin).
     * If the bin index exceeds the last bin, rotate buffer.
     * If the bin index is less than the first bin, discard the update.
     * The operation type determines how to insert the value.</p>
     */
    @Override
    public void update(Double value,
                       UpdateOperation operation,
                       Long timestamp) {
        int binIndex = findTimebinIndex(timestamp);

        if (binIndex < 0) {
            return;
        }

        if (binIndex >= binCount) {
            binIndex = rotateBuffer(binIndex);
        }

        int count = metadata.updateCountForBin(binIndex, timestamp);

        switch (operation) {
            case REPLACE:
                data.set(binIndex, value);
                break;
            case SUM:
                Double d = data.get(binIndex);
                data.set(binIndex, d == null ? value : d + value);
                break;
            case AVERAGE:
                d = data.get(binIndex);
                data.set(binIndex, d == null ? value : (d + value) / count);
                break;
        }

        metadata.updateStatistics(value, System.currentTimeMillis());
    }

    /**
     * <p>Initialize bin metrics using the {@link TimeFrame} construct. </p>
     */
    public void withTimeFrame(TimeFrame timeFrame) {
        timeFrame.configure();
        binCount = timeFrame.getBinCount();

        /**
         * TimeFrame width is computed in seconds.
         * This is the histogram "unit".
         * Width is 1000 for histogram in this case.
         */
        binUnit = timeFrame.getBinWidth();
        binWidth = 1000;
        binUnitLabel = timeFrame.getTimebin().name();

        /**
         * TimeFrame 'high time' is the upper bound defining
         * the highest edge of the last bin.  So the highest bin
         * is one bin size less.
         */
        double binSize = binUnit * binWidth;
        highestBin = (double) timeFrame.getHighTime() - binSize;
        lowestBin = (double) timeFrame.getLowTime();
    }

    private int findTimebinIndex(long timestamp) {
        return (int) FastMath.floor((timestamp - lowestBin) / binSize);
    }

    private int rotateBuffer(int binIndex) {
        int units = binIndex - binCount + 1;

        int len = units >= binCount ? binIndex : units;

        for (int i = 0; i < len; ++i) {
            data.remove(0);
            data.add(null);
        }

        metadata.rotate(units);

        lowestBin += (binSize * units);
        highestBin += (binSize * units);

        return binCount - 1;
    }
}
