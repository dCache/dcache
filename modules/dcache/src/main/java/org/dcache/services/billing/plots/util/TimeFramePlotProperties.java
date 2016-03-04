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
package org.dcache.services.billing.plots.util;

/**
 * Property definitions for plot type, axis, curve thickness and image type of
 * plots. <br><br>
 *
 * The constants correspond to the names of the properties which are defined in
 * <code>plotting-impl.properties</code>.
 *
 * @author arossi
 */
public final class TimeFramePlotProperties {

    public enum PlotType {
        BYTES_READ,
        BYTES_WRITTEN,
        BYTES_P2P,
        TRANSFERS_READ,
        TRANSFERS_WRITTEN,
        TRANSFERS_P2P,
        CONNECTION_TIME,
        CACHE_HITS
    }

    public enum TimeFrame {
        DAILY, WEEKLY, MONTHLY, YEARLY
    }

    public static final String POPUP_WIDTH = "plot.width";
    public static final String POPUP_HEIGHT = "plot.height";

    /**
     * Types, titles and time extensions for the plot s.
     */
    public static final String PLOT_TYPE = "plot.type.";
    public static final String PLOT_TITLE = "plot.title.";
    public static final String TIME_EXT = "time.ext.";
    public static final String TIME_DESC = "time.description.";

    /**
     * Interval between plot generations.
     */
    public static final String REFRESH_THRESHOLD = "refresh.threshold";
    public static final String REFRESH_THRESHOLD_UNIT = "refresh.threshold.unit";
    public static final String PLOT_TITLE_COLOR = "plot.title.color";
    public static final String PLOT_TITLE_SIZE = "plot.title.size";

    /**
     * Number of rows and columns for the plot grid; e.g., (2,2) would define a
     * plot divided into quadrants, each displying its own histograms. (1,1)
     * means the plot has only a single histogram area.
     */
    public static final String PLOT_GRID_ROWS = "plot.grid.rows";
    public static final String PLOT_GRID_COLS = "plot.grid.cols";

    public static final String PLOT_WIDTH = "plotWidth";
    public static final String PLOT_HEIGHT = "plotHeight";
    public static final String CURVE_THICKNESS = "curve.thickness";
    public static final String OUTLINE_THICKNESS = "outline.thickness";
    public static final String OPACITY = "opacity";
    public static final String MARKER_SHAPE = "marker.shape";
    public static final String MARKER_SIZE = "marker.size";
    public static final String X_AXIS_TYPE = "x.axis.type";
    public static final String X_AXIS_SIZE = "x.axis.size";
    public static final String X_AXIS_LABEL_COLOR = "x.axis.label.color";
    public static final String X_AXIS_TICK_SIZE = "x.axis.tick.size";
    public static final String X_AXIS_TICK_LABEL_COLOR = "x.axis.tick.label.color";
    public static final String Y_AXIS_SIZE = "y.axis.size";
    public static final String Y_AXIS_LABEL_COLOR = "y.axis.label.color";
    public static final String Y_AXIS_TICK_SIZE = "y.axis.tick.size";
    public static final String Y_AXIS_TICK_LABEL_COLOR = "y.axis.tick.label.color";
    public static final String Y_AXIS_ALLOW_ZERO_SUPPRESSION = "y.axis.allow.zero.suppression";

    /**
     * Where the plot image files are written
     */
    public static final String EXPORT_SUBDIR = "export.subdir";

    /**
     * e.g., gif, png, etc.
     */
    public static final String EXPORT_TYPE = "export.imagetype";

    /**
     * e.g., .gif, .png, etc.
     */
    public static final String EXPORT_EXTENSION = "export.extension";

    private TimeFramePlotProperties() {
    }
}
