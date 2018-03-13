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
package org.dcache.restful.providers.billing;

import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import org.dcache.util.histograms.TimeFrame;
import org.dcache.util.histograms.TimeFrame.BinType;
import org.dcache.util.histograms.TimeFrame.Type;
import org.dcache.vehicles.billing.BillingDataRequestMessage.SeriesDataType;
import org.dcache.vehicles.billing.BillingDataRequestMessage.SeriesType;

@ApiModel(description = "Defines a possible grid entry based on four enumerations:  "
                                + "type, series type, series data type and bin type.")
public final class BillingDataGridEntry {

    @ApiModelProperty(value = "Type of time series",
                    allowableValues = "READ, WRITE, P2P, STORE, RESTORE, CONNECTION, CACHED")
    private SeriesType type;

    @ApiModelProperty(value = "Type of data represented",
                    allowableValues = "BYTES, COUNT, AVGSECS, MAXSECS, MINSECS, HITS, MISSES")
    private SeriesDataType dataType;

    @ApiModelProperty(value = "Type of histogram bin (unit of the time frame)",
                    allowableValues = "HOUR, DAY, WEEK, MONTH")
    private BinType binType;

    @ApiModelProperty(value = "Extent of the time frame",
                    allowableValues = "DAY, WEEK, MONTH, YEAR")
    private Type range;

    public BillingDataGridEntry() {
    }

    public BillingDataGridEntry(String toParse) {
        Preconditions.checkNotNull(toParse,
                                   "String value cannot be null.");

        String[] parts = toParse.split("_");

        Preconditions.checkArgument(parts.length == 4,
                                    "String value must have 4 parts.");

        type = SeriesType.valueOf(parts[0].toUpperCase());
        dataType = SeriesDataType.valueOf(parts[1].toUpperCase());
        binType = BinType.valueOf(parts[2].toUpperCase());
        range = Type.valueOf(parts[3].toUpperCase());
    }

    public BillingDataGridEntry(SeriesType type,
                                SeriesDataType dataType,
                                TimeFrame timeFrame) {
        Preconditions.checkNotNull(type, "Series type cannot be null.");
        Preconditions.checkNotNull(dataType,
                                   "Series data type cannot be null.");
        Preconditions.checkNotNull(timeFrame,
                                   "TimeFrame cannot be null.");
        Preconditions.checkNotNull(timeFrame.getTimebin(),
                                   "Bin type  cannot be null.");
        Preconditions.checkNotNull(timeFrame.getTimeframe(),
                                   "Range type cannot be null.");
        this.type = type;
        this.dataType = dataType;
        this.binType = timeFrame.getTimebin();
        this.range = timeFrame.getTimeframe();
    }

    public BinType getBinType() {
        return binType;
    }

    public SeriesDataType getDataType() {
        return dataType;
    }

    public Type getRange() {
        return range;
    }

    public SeriesType getType() {
        return type;
    }

    public void setBinType(BinType binType) {
        this.binType = binType;
    }

    public void setDataType(SeriesDataType dataType) {
        this.dataType = dataType;
    }

    public void setRange(Type range) {
        this.range = range;
    }

    public void setType(SeriesType type) {
        this.type = type;
    }

    public String toString() {
        return type + "_" + dataType + "_" + binType + "_" + range;
    }
}
