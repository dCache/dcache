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
package org.dcache.services.billing.histograms;

/**
 * Property definitions for label, color and scale of histograms. <br>
 * <br>
 *
 * The constants correspond to the names of the properties which are
 * defined in <code>plotting-impl.properties</code>.
 *
 * @author arossi
 */
public class TimeFrameHistogramProperties {
    /**
     * Labels for axes according to plot type and histogram time-binning.
     */
    public static final String LABEL_X_AXIS = "label.x.axis";
    public static final String LABEL_Y_AXIS_GBYTES_HR = "label.y.axis.gbytes.hourly";
    public static final String LABEL_Y_AXIS_GBYTES_DY = "label.y.axis.gbytes.daily";
    public static final String LABEL_Y_AXIS_TRANSF_HR = "label.y.axis.transfers.hourly";
    public static final String LABEL_Y_AXIS_TRANSF_DY = "label.y.axis.transfers.daily";
    public static final String LABEL_Y_AXIS_CONNECT = "label.y.axis.connection.time";
    public static final String LABEL_Y_AXIS_HITS_HR = "label.y.axis.hits.hourly";
    public static final String LABEL_Y_AXIS_HITS_DY = "label.y.axis.hits.daily";

    /**
     * Histogram labels for plot legend.
     */
    public static final String LABEL_DC_RD = "label.dcache.reads";
    public static final String LABEL_DC_WR = "label.dcache.writes";
    public static final String LABEL_HSM_RD = "label.hsm.reads";
    public static final String LABEL_HSM_WR = "label.hsm.writes";
    public static final String LABEL_DC_P2P = "label.dcache.p2p";
    public static final String LABEL_MIN = "label.minimum";
    public static final String LABEL_MAX = "label.maximum";
    public static final String LABEL_AVG = "label.average";
    public static final String LABEL_CACHED = "label.cached";
    public static final String LABEL_NCACHED = "label.ncached";

    /**
     * Histogram colors.
     */
    public static final String COLOR_DC = "color.dcache";
    public static final String COLOR_HSM = "color.hsm";
    public static final String COLOR_MAX = "color.max";
    public static final String COLOR_MIN = "color.min";
    public static final String COLOR_AVG = "color.avg";
    public static final String COLOR_CACHED = "color.cached";
    public static final String COLOR_NCACHED = "color.ncached";

    private TimeFrameHistogramProperties() {
    }
}
