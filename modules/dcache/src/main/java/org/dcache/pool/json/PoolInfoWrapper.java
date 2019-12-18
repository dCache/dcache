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
package org.dcache.pool.json;

import java.io.Serializable;

import org.dcache.util.histograms.Histogram;
import org.dcache.util.histograms.HistogramModel;
import org.dcache.util.histograms.TimeseriesHistogram;

/**
 * <p>Meant for service-side caching.</p>
 *
 * <p>For the timeseries data, {@link HistogramModel} rather than
 * {@link Histogram} is used in order to enable updating and regeneration.</p>
 */
public class PoolInfoWrapper implements Serializable {
    /**
     * <p>Full pool name or pool group name.</p>
     */
    private String key;

    /**
     * <p>Full pool diagnostic data ... see {@link PoolData} </p>
     * <p>The composed SweeperData contains the counts histogram.</p>
     */
    private PoolData info = new PoolData();

    /**
     * <p>Derived from sweeper's file lifetime data.</p>
     */
    private TimeseriesHistogram fileLiftimeMax;
    private TimeseriesHistogram fileLiftimeAvg;
    private TimeseriesHistogram fileLiftimeMin;
    private TimeseriesHistogram fileLiftimeStddev;

    /**
     * <p>Derived from pool cost queue data.</p>
     */
    private TimeseriesHistogram activeMovers;
    private TimeseriesHistogram queuedMovers;
    private TimeseriesHistogram activeP2P;
    private TimeseriesHistogram queuedP2P;
    private TimeseriesHistogram activeP2PClient;
    private TimeseriesHistogram queuedP2PClient;
    private TimeseriesHistogram activeFlush;
    private TimeseriesHistogram queuedFlush;
    private TimeseriesHistogram activeStage;
    private TimeseriesHistogram queuedStage;

    public TimeseriesHistogram getActiveFlush() {
        return activeFlush;
    }

    public TimeseriesHistogram getActiveMovers() {
        return activeMovers;
    }

    public TimeseriesHistogram getActiveP2P() {
        return activeP2P;
    }

    public TimeseriesHistogram getActiveP2PClient() {
        return activeP2PClient;
    }

    public TimeseriesHistogram getActiveStage() {
        return activeStage;
    }

    public TimeseriesHistogram getFileLiftimeAvg() {
        return fileLiftimeAvg;
    }

    public TimeseriesHistogram getFileLiftimeMax() {
        return fileLiftimeMax;
    }

    public TimeseriesHistogram getFileLiftimeMin() {
        return fileLiftimeMin;
    }

    public TimeseriesHistogram getFileLiftimeStddev() {
        return fileLiftimeStddev;
    }

    public PoolData getInfo() {
        return info;
    }

    public String getKey() {
        return key;
    }

    public TimeseriesHistogram getQueuedFlush() {
        return queuedFlush;
    }

    public TimeseriesHistogram getQueuedMovers() {
        return queuedMovers;
    }

    public TimeseriesHistogram getQueuedP2P() {
        return queuedP2P;
    }

    public TimeseriesHistogram getQueuedP2PClient() {
        return queuedP2PClient;
    }

    public TimeseriesHistogram getQueuedStage() {
        return queuedStage;
    }

    public void setActiveFlush(TimeseriesHistogram activeFlush) {
        this.activeFlush = activeFlush;
    }

    public void setActiveMovers(
                    TimeseriesHistogram activeMovers) {
        this.activeMovers = activeMovers;
    }

    public void setActiveP2P(TimeseriesHistogram activeP2P) {
        this.activeP2P = activeP2P;
    }

    public void setActiveP2PClient(
                    TimeseriesHistogram activeP2PClient) {
        this.activeP2PClient = activeP2PClient;
    }

    public void setActiveStage(TimeseriesHistogram activeStage) {
        this.activeStage = activeStage;
    }

    public void setFileLiftimeAvg(
                    TimeseriesHistogram fileLiftimeAvg) {
        this.fileLiftimeAvg = fileLiftimeAvg;
    }

    public void setFileLiftimeMax(
                    TimeseriesHistogram fileLiftimeMax) {
        this.fileLiftimeMax = fileLiftimeMax;
    }

    public void setFileLiftimeMin(
                    TimeseriesHistogram fileLiftimeMin) {
        this.fileLiftimeMin = fileLiftimeMin;
    }

    public void setFileLiftimeStddev(
                    TimeseriesHistogram fileLiftimeStddev) {
        this.fileLiftimeStddev = fileLiftimeStddev;
    }

    public void setInfo(PoolData info) {
        this.info = info;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setQueuedFlush(TimeseriesHistogram queuedFlush) {
        this.queuedFlush = queuedFlush;
    }

    public void setQueuedMovers(
                    TimeseriesHistogram queuedMovers) {
        this.queuedMovers = queuedMovers;
    }

    public void setQueuedP2P(TimeseriesHistogram queuedP2P) {
        this.queuedP2P = queuedP2P;
    }

    public void setQueuedP2PClient(
                    TimeseriesHistogram queuedP2PClient) {
        this.queuedP2PClient = queuedP2PClient;
    }

    public void setQueuedStage(TimeseriesHistogram queuedStage) {
        this.queuedStage = queuedStage;
    }
}
