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

import java.io.Serializable;

/**
 * <p>Thin histogram abstraction.  This contains the minimal identifying
 *    data and metadata.  It is not necessarily more lightweight than
 *    the {@link HistogramModel}, since all the coordinate values
 *    are computed here (whereas the abscissa values for the model
 *    are implicit).  It merely exposes less (redundant) metadata
 *    and is easier to translate into a plot.</p>
 */
public final class Histogram implements Serializable {
    private static final String DATA_SIZE_ERROR =
        "Template was not properly configured: "
                        + "bin count %s does not match data size %s.";

    private static final String MISSING_DATA_ERROR =
        "Template was not properly configured: missing %s.";

    private String identifier;
    private String binUnit;
    private String dataUnit;
    private double binSize;
    private double lowestBin;

    private Double[][] values;

    public double getBinSize() {
        return binSize;
    }

    public String getBinUnit() {
        return binUnit;
    }

    public String getDataUnit() {
        return dataUnit;
    }

    public String getIdentifier() {
        return identifier;
    }

    public double getLowestBin() {
        return lowestBin;
    }

    public Double[][] getValues() {
        return values;
    }

    public void setBinSize(double binSize) {
        this.binSize = binSize;
    }

    public void setBinUnit(String binUnit) {
        this.binUnit = binUnit;
    }

    public void setDataUnit(String dataUnit) {
        this.dataUnit = dataUnit;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setLowestBin(double lowestBin) {
        this.lowestBin = lowestBin;
    }

    public void setValues(Double[][] values) {
        this.values = values;
    }
}
