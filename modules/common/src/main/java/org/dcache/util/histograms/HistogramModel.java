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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Base (generic) container for histogram data.  Bean which can be used for
 * persistence, e.g., as JSON object.</p>
 *
 * <p>This bean is meant as an aid for the maintenance and updating of
 * data to be returned eventually to client in the form of the simplified {@link Histogram}.</p>
 */
public abstract class HistogramModel implements Serializable {

    private static final String DATA_SIZE_ERROR =
          "Template was not properly configured: "
                + "bin count %s does not match data size %s.";

    protected Integer binCount;     // number of bins
    protected Integer binWidth;     // number of units per bin
    protected Double binUnit;      // measure for values
    protected Double highestBin;   // value for highest bin
    protected Double lowestBin;    // value for lowest bin
    protected String identifier;
    protected String binUnitLabel;
    protected String dataUnitLabel;

    protected HistogramMetadata metadata;
    protected List<Double> data;

    /*
     * For convenience
     */
    protected double binSize;

    protected HistogramModel() {
    }

    protected HistogramModel(HistogramModel copy) {
        requireNonNull(copy);

        binCount = copy.binCount;
        binWidth = copy.binWidth;
        binUnit = copy.binUnit;
        highestBin = copy.highestBin;
        lowestBin = copy.lowestBin;
        identifier = copy.identifier;
        binUnitLabel = copy.binUnitLabel;
        dataUnitLabel = copy.dataUnitLabel;
        metadata = copy.metadata;
        binSize = copy.binSize;

        if (copy.data != null) {
            data = new ArrayList<>(copy.data);
        }

    }

    public Integer getBinCount() {
        return binCount;
    }

    public double getBinSize() {
        return binSize;
    }

    public Double getBinUnit() {
        return binUnit;
    }

    public String getBinUnitLabel() {
        return binUnitLabel;
    }

    public Integer getBinWidth() {
        return binWidth;
    }

    public List<Double> getData() {
        return data;
    }

    public String getDataUnitLabel() {
        return dataUnitLabel;
    }

    public Double getHighestBin() {
        return highestBin;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Double getLowestBin() {
        return lowestBin;
    }

    public HistogramMetadata getMetadata() {
        return metadata;
    }

    public void setBinCount(Integer binCount) {
        this.binCount = binCount;
    }

    public void setBinSize(double binSize) {
        this.binSize = binSize;
    }

    public void setBinUnit(Double binUnit) {
        this.binUnit = binUnit;
    }

    public void setBinUnitLabel(String binUnitLabel) {
        this.binUnitLabel = binUnitLabel;
    }

    public void setBinWidth(Integer binWidth) {
        this.binWidth = binWidth;
    }

    public void setData(List<Double> data) {
        this.data = data;
    }

    public void setDataUnitLabel(String dataUnitLabel) {
        this.dataUnitLabel = dataUnitLabel;
    }

    public void setHighestBin(Double highestBin) {
        this.highestBin = highestBin;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setLowestBin(Double lowestBin) {
        this.lowestBin = lowestBin;
    }

    public void setMetadata(HistogramMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * <p>Conversion of model eliminating metadata.  Data is converted to
     * 2D tuples.</p>
     */
    public Histogram toHistogram() {
        Histogram histogram = new Histogram();
        histogram.setIdentifier(requireNonNull(identifier,
              "No histogram identifier provided."));
        requireNonNull(binUnit,
              "No histogram binUnit provided.");
        requireNonNull(binWidth,
              "No histogram binWidth provided.");
        histogram.setBinSize(binUnit * binWidth);
        histogram.setBinUnit(requireNonNull(binUnitLabel,
              "No histogram bin label provided."));
        histogram.setDataUnit(requireNonNull(dataUnitLabel,
              "No histogram data label provided."));
        histogram.setLowestBin(requireNonNull(lowestBin,
              "Lowest histogram bin not provided."));

        int len = requireNonNull(binCount,
              "No histogram bin count provided.");

        requireNonNull(data, "Histogram data list missing.");
        Preconditions.checkArgument(data.size() == len,
              String.format(DATA_SIZE_ERROR,
                    binCount,
                    data.size()));

        Double[][] values = new Double[binCount][];
        for (int i = 0; i < binCount; ++i) {
            values[i] = new Double[]{lowestBin + (i * binSize), data.get(i)};
        }

        histogram.setValues(values);

        return histogram;
    }

    /**
     * <p>May be overridden to adjust or compute the histogram settings.</p>
     *
     * <p>Should be idempotent in the presence of precomputed values.</p>
     */
    public abstract void configure();

    protected void computeBinSizeFromWidthAndUnit() {
        if (binWidth == null) {
            binWidth = 1;
        }

        binSize = binWidth == 0 ?
              binUnit :
              binUnit * binWidth;
    }

    protected double getHighestFromLowest() {
        return lowestBin + (binCount - 1) * binSize;
    }

    protected double getLowestFromHighest() {
        return highestBin - (binCount - 1) * binSize;
    }

    protected void setHighestFromLowest() {
        if (highestBin == null) {
            highestBin = getHighestFromLowest();
        }
    }

    protected void setLowestFromHighest() {
        if (lowestBin == null) {
            lowestBin = getLowestFromHighest();
        }
    }

    protected void updateStatistics() {
        if (metadata == null) {
            return;
        }

        long now = System.currentTimeMillis();
        for (Double d : data) {
            if (d != null) {
                metadata.updateStatistics(d, now);
            }
        }
    }
}
