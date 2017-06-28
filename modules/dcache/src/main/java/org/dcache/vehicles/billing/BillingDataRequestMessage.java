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
package org.dcache.vehicles.billing;

import diskCacheV111.vehicles.Message;
import org.dcache.util.histograms.TimeFrame;
import org.dcache.util.histograms.TimeseriesHistogram;

/**
 * <p>The purpose of this message is to facilitate interaction
 * in support of RESTful interfaces.</p>
 *
 * <p>Unlike previous APIs, which were somewhat more coupled with the
 * actual plotting requirements (JAIDA), there are no presuppositions
 * here as to what will be done with the histogram data.  Plotting will now
 * be completely a client-side prerogative.</p>
 *
 * <p>This class is part of an API which will eventually replace the
 * previous, JAIDA-based one.</p>
 *
 * <p>The type specifications are mapped to the underlying DAO classes in the
 * {@link org.dcache.services.billing.cells.receivers.BillingDataRequestReceiver}.</p>
 */
public final class BillingDataRequestMessage extends Message {
    public enum SeriesType {
        READ, WRITE, P2P, STORE, RESTORE, CONNECTION, CACHED
    }

    public enum SeriesDataType {
        BYTES, COUNT, AVGSECS, MAXSECS, MINSECS, HITS, MISSES
    }

    /**
     * <p>Specifies the type of billing data: for a specific kind of transfers,
     * for an aggregated connection time, or for disk cache statistics.</p>
     */
    private SeriesType type;

    /**
     * <p>Specifies the subtype of data: for transfers, whether the amount of
     * data transferred or the number of transfers; for connections, whether
     * average, minimum or maximum values; and for disk cache,
     * whether hits or misses.</p>
     */
    private SeriesDataType dataType;

    /**
     * <p>Specifies the time series to which this data corresponds.</p>
     */
    private TimeFrame timeFrame;

    /**
     * <p>The histogram constructed from the series, bin and timeFrame.</p>
     */
    private TimeseriesHistogram histogram;

    public SeriesDataType getDataType() {
        return dataType;
    }

    public TimeseriesHistogram getHistogram() {
        return histogram;
    }

    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    public SeriesType getType() {
        return type;
    }

    public void setDataType(
                    SeriesDataType dataType) {
        this.dataType = dataType;
    }

    public void setHistogram(TimeseriesHistogram histogram) {
        this.histogram = histogram;
    }

    public void setTimeFrame(TimeFrame timeFrame) {
        this.timeFrame = timeFrame;
    }

    public void setType(SeriesType type) {
        this.type = type;
    }
}
