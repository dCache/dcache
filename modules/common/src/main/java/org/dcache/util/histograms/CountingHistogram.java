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
 * <p>Maintains a histogram data set which consists of a fixed number
 * of counting bins; the width of the bin is determined by the max value.</p>
 *
 * <p>This kind of histogram cannot be updated, and must be reconstructed
 * when new values are to be computed.</p>
 */
public class CountingHistogram extends HistogramModel {
    private static final String UNSUPPORTED_UPDATE =
                    "update not supported on this histogram type: "
                                    + CountingHistogram.class;

    public CountingHistogram() {
    }

    public CountingHistogram(CountingHistogram copy) {
        super(copy);
    }

    @Override
    public void configure() {
        Preconditions.checkNotNull(data, "no values set.");
        Preconditions.checkNotNull(binCount,
                                   "bin count must be defined.");
        Preconditions.checkArgument(binCount > 1,
                                    "bin count must be > 1.");
        Preconditions.checkNotNull(binUnit,
                                   "bin unit must be defined.");
        Preconditions.checkArgument(binUnit > 0,
                                    "bin unit must be > 0.");

        computeBinSizeFromWidthAndUnit();

        metadata = new HistogramMetadata();

        /*
         *  Data entered here is raw unordered data to be converted to counts.
         *  First update the statistics from the raw data.
         */
        updateStatistics();

        /*
         *  On the basis of the maximum value, compute the bin width.
         *  This is adjusted in order to maintain the given bin count.
         *  Lowest value is always 0.  Bin width can only have integer
         *  values that are multiples of the bin unit.
         */
        Double max = metadata.getMaxValue().orElse(null);
        double maxValueIndex = max == null ? binCount - 1 : FastMath.floor(max / binSize);
        binWidth = (int) FastMath.ceil(maxValueIndex / (binCount - 1));

        /**
         * Re-adjust size on the basis of the adjusted width.
         */
        computeBinSizeFromWidthAndUnit();

        /*
         *  Construct the histogram from the raw data
         */
        long[] histogram = new long[binCount];

        for (Double d : data) {
            ++histogram[(int) FastMath.floor(d / binSize)];
        }

        data = new ArrayList<>();

        for (long d : histogram) {
            data.add((double) d);
        }

        lowestBin = 0.0D;
        setHighestFromLowest();

        int computedCount = (int) binSize == 0 ?
                        binCount :
                        (int) FastMath.ceil((highestBin - lowestBin) / binSize);

        if (computedCount > binCount) {
            throw new RuntimeException("bin count " + binCount
                                                       + " is less than "
                                                       + computedCount
                                                       + ", computed from bin "
                                                       + "width, highest and "
                                                       + "lowest bin values; "
                                                       + "this is a bug.");
        }
    }
}
