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
package org.dcache.webadmin.model.dataaccess.util.rrd4j;

import org.rrd4j.graph.RrdGraphConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Placeholder class which controls properties for the round-robin database and
 * for the plots.
 *
 * @author arossi
 */
public class RrdSettings {
    public static final String FILE_SUFFIX = "_queue";
    public static final String RRD_SUFFIX = ".rrd";
    public static final String ALL_POOLS = "all";
    public static final String GROUP_DIR = "group_";

    public static int getRrdGraphConstant(TimeUnit unit) {
        if (TimeUnit.DAYS.equals(unit)) {
            return RrdGraphConstants.DAY;
        }
        if (TimeUnit.HOURS.equals(unit)) {
            return RrdGraphConstants.HOUR;
        }
        if (TimeUnit.MINUTES.equals(unit)) {
            return RrdGraphConstants.MINUTE;
        }
        if (TimeUnit.SECONDS.equals(unit)) {
            return RrdGraphConstants.SECOND;
        }
        return 0;
    }

    public static File getImagePath(String type, File dir, String name) {
        return new File(dir, name + FILE_SUFFIX + "." + type);
    }

    /*
     * Package visibility for easy access by the collector; setters are for
     * Spring injection,
     */
    String baseDirectory;
    String imgType;
    int stepSize = 5;
    int spanSize = 15;
    TimeUnit stepUnit = TimeUnit.MINUTES;
    TimeUnit spanUnit = TimeUnit.DAYS;
    int rightMarginInSteps = 3;
    String yLabel = "Threads";
    int version = 2;
    int imgWidth = 1200;
    int imgHeight = 800;
    double heartbeatFactor = 1.5;

    /*
     * RrdGraphConstants
     */
    int minorUnit = RrdGraphConstants.DAY;
    int minorUnitCount = 1;
    int majorUnit = RrdGraphConstants.DAY;
    int majorUnitCount = 1;
    int labelUnit = RrdGraphConstants.DAY;
    int labelUnitCount = 1;
    int labelSpan = 1;

    String simpleDateFormat = "E MMM dd YYYY HH:mm";

    /*
     * Derived values
     */
    long stepInMillis;
    long spanInMillis;
    long stepInSeconds;
    long spanInSeconds;
    int numSteps;

    long rightMarginInSeconds;

    public void initialize() {
        stepInMillis = stepUnit.toMillis(stepSize);
        spanInMillis = spanUnit.toMillis(spanSize);
        stepInSeconds = TimeUnit.MILLISECONDS.toSeconds(stepInMillis);
        spanInSeconds = TimeUnit.MILLISECONDS.toSeconds(spanInMillis);
        numSteps = (int) (spanInSeconds / stepInSeconds);
        rightMarginInSeconds = rightMarginInSteps * stepInSeconds;
        adjustToPixels();
    }

    public void setBaseDirectory(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    public void setHeartbeatFactor(String heartbeatFactor) {
        this.heartbeatFactor = Double.valueOf(heartbeatFactor);
    }

    public void setImgHeight(int imgHeight) {
        this.imgHeight = imgHeight;
    }

    public void setImgType(String imgType) {
        this.imgType = imgType;
    }

    public void setImgWidth(int imgWidth) {
        this.imgWidth = imgWidth;
    }

    public void setLabelSpan(int labelSpan) {
        this.labelSpan = labelSpan;
    }

    public void setLabelUnit(TimeUnit labelUnit) {
        this.labelUnit = getRrdGraphConstant(labelUnit);
    }

    public void setLabelUnitCount(int labelUnitCount) {
        this.labelUnitCount = labelUnitCount;
    }

    public void setMajorUnit(TimeUnit majorUnit) {
        this.majorUnit = getRrdGraphConstant(majorUnit);
    }

    public void setMajorUnitCount(int majorUnitCount) {
        this.majorUnitCount = majorUnitCount;
    }

    public void setMinorUnit(TimeUnit minorUnit) {
        this.minorUnit = getRrdGraphConstant(minorUnit);
    }

    public void setMinorUnitCount(int minorUnitCount) {
        this.minorUnitCount = minorUnitCount;
    }

    public void setRightMarginInSteps(int rightMarginInSteps) {
        this.rightMarginInSteps = rightMarginInSteps;
    }

    public void setSimpleDateFormat(String simpleDateFormat) {
        this.simpleDateFormat = simpleDateFormat;
    }

    public void setSpanSize(int spanSize) {
        this.spanSize = spanSize;
    }

    public void setSpanUnit(TimeUnit spanUnit) {
        this.spanUnit = spanUnit;
    }

    public void setStepSize(int stepSize) {
        this.stepSize = stepSize;
    }

    public void setStepUnit(TimeUnit stepUnit) {
        this.stepUnit = stepUnit;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setYLabel(String yLabel) {
        this.yLabel = yLabel;
    }

    public String toString() {
        String lb = System.getProperty("line.separator");
        return new StringBuilder()
        .append("RrdSettings:").append(lb)
        .append("-------------------------------------").append(lb)
        .append("baseDirectory").append("=").append(baseDirectory).append(lb)
        .append("imgType").append("=").append(baseDirectory).append(lb)
        .append("stepSize").append("=").append(stepSize).append(lb)
        .append("spanSize").append("=").append(spanSize).append(lb)
        .append("stepUnit").append("=").append(stepUnit).append(lb)
        .append("spanUnit").append("=").append(spanUnit).append(lb)
        .append("rightMarginInSteps").append("=").append(rightMarginInSteps).append(lb)
        .append("yLabel").append("=").append(yLabel).append(lb)
        .append("version").append("=").append(version).append(lb)
        .append("imgWidth").append("=").append(imgWidth).append(lb)
        .append("imgHeight").append("=").append(imgHeight).append(lb)
        .append("minorUnit").append("=").append(minorUnit).append(lb)
        .append("minorUnitCount").append("=").append(minorUnitCount).append(lb)
        .append("majorUnit").append("=").append(majorUnit).append(lb)
        .append("majorUnitCount").append("=").append(majorUnitCount).append(lb)
        .append("labelUnit").append("=").append(labelUnit).append(lb)
        .append("labelUnitCount").append("=").append(labelUnitCount).append(lb)
        .append("labelSpan").append("=").append(labelSpan).append(lb)
        .append("simpleDateFormat").append("=").append(simpleDateFormat).append(lb)
        .append("stepInMillis").append("=").append(stepInMillis).append(lb)
        .append("spanInMillis").append("=").append(spanInMillis).append(lb)
        .append("stepInSeconds").append("=").append(stepInSeconds).append(lb)
        .append("spanInSeconds").append("=").append(spanInSeconds).append(lb)
        .append("numSteps").append("=").append(numSteps).append(lb)
        .append("rightMarginInSeconds").append("=").append(rightMarginInSeconds).append(lb)
        .append("heartbeatFactor").append("=").append(heartbeatFactor).append(lb)
        .append("______________________________________").append(lb).toString();
    }

    /**
     * Checks that there are enough pixels to accommodate the steps.
     * Normalizes width/steps so that there are at least 2 pixels
     * per step and the image width is an integral multiple of total steps.
     */
    private void adjustToPixels() {
        double totalSteps = (double)(numSteps+rightMarginInSteps);
        double stepRatio = ((double)imgWidth)/totalSteps;
        double normalRatio = Math.max(2.0, Math.floor(stepRatio));
        double delta = normalRatio/stepRatio;
        int newWidth = (int)(imgWidth*delta);
        if (newWidth != imgWidth) {
            Logger logger = LoggerFactory.getLogger(this.getClass());
            logger.warn("Rrd pool queue plots: "
                            + "number of time steps ({}); "
                            + "stepRatio ({}); " + "normalizedRatio ({}); "
                            + "delta ({}); "
                            + "original width in pixels ({}); "
                            + "normalized width: ({}).",
                            totalSteps, stepRatio, normalRatio,
                            delta, imgWidth, newWidth);
            imgWidth = newWidth;
        }
    }
}
