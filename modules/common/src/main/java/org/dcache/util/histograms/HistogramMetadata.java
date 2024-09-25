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

import static java.util.Objects.requireNonNull;
import static org.dcache.util.MathUtils.nanToZero;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import org.apache.commons.math3.util.FastMath;

/**
 * <p>Composed into {@link HistogramModel} to provide for statistics
 * and for running average updating.</p>
 *
 * <p>This class now relies on the ability of the JSON parser to
 * handle {@link Optional} values correctly.  This means that all JSON parsing of the {@link
 * HistogramModel} object hierarchy must also handle them.</p>
 */
public final class HistogramMetadata implements Serializable {

    /**
     * <p>Data statistics.</p>
     *
     * <p>These accumulate for the life of the histogram, regardless
     * of whether values are reset or removed from the data buffer. These data are also accessible
     * through public methods, and thus are part of the preservable state of the histogram.</p>
     */
    private long lastUpdateInMillis;
    private long count = 0L;
    private Double maxValue;
    private Double minValue;
    private double sum = 0.0D;
    private double sumOfSquares = 0.0D;

    /**
     * <p>Maintained in case updates involve a running average.</p>
     */
    private int[] binCounts;
    private long[] binTimestamps;
    private int numBins = 0;
    private int start = 0;

    public HistogramMetadata() {
    }

    public HistogramMetadata(int numBins) {
        this.numBins = numBins;
        binCounts = new int[numBins];
        binTimestamps = new long[numBins];

        long now = System.currentTimeMillis();
        for (int i = 0; i < numBins; ++i) {
            binTimestamps[i] = now;
        }
    }

    public HistogramMetadata(HistogramMetadata copy) {
        requireNonNull(copy,
              "Metadata copy object is null.");
        lastUpdateInMillis = copy.lastUpdateInMillis;
        count = copy.count;
        maxValue = copy.maxValue;
        minValue = copy.minValue;
        sum = copy.sum;
        sumOfSquares = copy.sumOfSquares;
        binCounts = Arrays.copyOf(copy.binCounts, copy.binCounts.length);
        binTimestamps = Arrays.copyOf(copy.binTimestamps,
              copy.binTimestamps.length);
        numBins = copy.numBins;
        start = copy.start;
    }

    public int[] getBinCounts() {
        return binCounts;
    }

    public long[] getBinTimestamps() {
        return binTimestamps;
    }

    public long getCount() {
        return count;
    }

    public long getLastUpdateInMillis() {
        return lastUpdateInMillis;
    }

    public Optional<Double> getMaxValue() {
        return Optional.ofNullable(maxValue);
    }

    public Optional<Double> getMinValue() {
        return Optional.ofNullable(minValue);
    }

    public int getNumBins() {
        return numBins;
    }

    public int getStart() {
        return start;
    }

    public double getSum() {
        return sum;
    }

    public double getSumOfSquares() {
        return sumOfSquares;
    }

    /**
     * @param metadata with which to merge the statistics
     * @return this object with counts incremented and max and min reset on the basis of the
     * additional data from the input object.
     */
    public HistogramMetadata mergeStatistics(HistogramMetadata metadata) {
        requireNonNull(metadata, "Cannot merge statistics, "
              + "metadata object was null.");
        count += metadata.count;
        sum += metadata.sum;
        sumOfSquares += metadata.sumOfSquares;
        if (metadata.maxValue != null) {
            maxValue = maxValue == null ? metadata.maxValue :
                  FastMath.max(maxValue, metadata.maxValue);
        }

        if (metadata.minValue != null) {
            minValue = minValue == null ? metadata.minValue :
                  FastMath.min(minValue, metadata.minValue);
        }

        return this;
    }

    public void rotate(int units) {
        units = Math.min(units, numBins);

        long now = System.currentTimeMillis();

        for (int i = start; i < units; ++i) {
            binCounts[i % numBins] = 0;
            binTimestamps[i % numBins] = now;
        }

        start = units == numBins ? 0 : (start + units) % numBins;
    }

    public void setBinCounts(int[] binCounts) {
        this.binCounts = binCounts;
    }

    public void setBinTimestamps(long[] binTimestamps) {
        this.binTimestamps = binTimestamps;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setLastUpdateInMillis(long lastUpdateInMillis) {
        this.lastUpdateInMillis = lastUpdateInMillis;
    }

    public void setMaxValue(Optional<Double> maxValue) {
        this.maxValue = maxValue.orElse(null);
    }

    public void setMinValue(Optional<Double> minValue) {
        this.minValue = minValue.orElse(null);
    }

    public void setNumBins(int numBins) {
        this.numBins = numBins;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void setSumOfSquares(double sumOfSquares) {
        this.sumOfSquares = sumOfSquares;
    }

    public double standardDeviation() {
        if (count == 0L) {
            return 0.0;
        }

        double variance = nanToZero((sumOfSquares / count)
              - FastMath.pow(sum / count, 2));

        return FastMath.sqrt(variance);
    }

    /**
     * @param index     the actual index corresponding to the position in the array of data
     *                  comprising the histogram.
     * @param timestamp associated with the current update.
     * @return the updated count for the index
     */
    public int updateCountForBin(int index, long timestamp) {
        if (index < 0 || numBins <= index) {
            throw new ArrayIndexOutOfBoundsException(
                  "bin " + index + " does not"
                        + " exist");
        }

        index = rotatedIndex(index);

        ++binCounts[index];
        binTimestamps[index] = timestamp;

        return binCounts[index];
    }

    /**
     * @param lastValue used to increment counts and determine the new min and max
     * @param now       timestamp of the update
     * @return this object updated
     */
    public HistogramMetadata updateStatistics(Double lastValue, long now) {
        requireNonNull(lastValue, "Can only update "
              + "using nonnull value.");
        ++count;
        sum += lastValue;
        sumOfSquares += FastMath.pow(lastValue, 2L);
        maxValue = maxValue == null ? lastValue :
              FastMath.max(lastValue, maxValue);
        minValue = minValue == null ? lastValue :
              FastMath.min(lastValue, minValue);
        lastUpdateInMillis = now;
        return this;
    }

    @VisibleForTesting
    int rotatedIndex(int index) {
        return (start + index) % numBins;
    }
}
