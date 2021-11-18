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
package org.dcache.qos.services.scanner.data;

import static org.dcache.qos.services.scanner.data.ScanOperation.ScanLabel.FINISHED;
import static org.dcache.qos.services.scanner.data.ScanOperation.ScanLabel.STARTED;

import diskCacheV111.util.CacheException;
import java.util.UUID;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.services.scanner.util.SystemScanTask;
import org.dcache.qos.util.ExceptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation which periodically scans the namespace inodes table (ignoring location/pool).
 */
public final class SystemScanOperation extends ScanOperation<SystemScanTask> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemScanOperation.class);

    private static final String TO_STRING = "%s | %s (from %s to %s) %s (completed: %s / %s : %s%%)"
          + "(%s: %s)(%s: %s) (failed %s) %s";

    final String id;
    final long from;
    final long to;
    final boolean nearline;

    long lastUpdate;
    long lastScan;
    long[] minMaxIndices;

    SystemScanTask task;
    CacheException exception;
    ScanLabel scanLabel;

    private long runningTotal;
    private long completed;
    private long failed;
    private boolean canceled;

    SystemScanOperation(long from, long to, boolean nearline) {
        id = UUID.randomUUID().toString();
        this.from = from;
        this.to = to;
        this.nearline = nearline;

        lastUpdate = System.currentTimeMillis();
        lastScan = lastUpdate;
        runningTotal = 0;
        completed = 0;
        failed = 0;
        canceled = false;
        scanLabel = STARTED;
    }

    public String toString() {
        return String.format(TO_STRING,
              id,
              nearline ? "NEARLINE" : "ONLINE",
              from,
              to,
              canceled ? "CANCELED" : (scanLabel == FINISHED ? "DONE" : "RUNNING"),
              completed,
              runningTotal == 0 && completed > 0 ? "?" : runningTotal,
              getFormattedPercentDone(),
              scanLabel == FINISHED ? "finished" : "updated",
              FileQoSUpdate.getFormattedDateFromMillis(lastUpdate),
              "started",
              FileQoSUpdate.getFormattedDateFromMillis(lastScan),
              failed,
              exception == null ? getFailedMessage() :
                    new ExceptionMessage(exception));
    }

    public void cancel() {
        this.canceled = true;
        scanLabel = FINISHED;
    }

    protected void incrementCompleted(boolean failed) {
        LOGGER.trace("entering incrementCompleted, failed {}, "
                    + "runningTotal {}, completed = {}.",
              failed, runningTotal, completed);
        ++completed;
        if (failed) {
            ++this.failed;
        }
        lastUpdate = System.currentTimeMillis();
        LOGGER.trace("leaving incrementCompleted, failed {}, "
                    + "runningTotal {}, completed = {}.",
              failed, runningTotal, completed);
    }

    void incrementCurrent(long current) {
        this.runningTotal += current;
        lastUpdate = System.currentTimeMillis();
    }

    protected boolean isComplete() {
        boolean isComplete = runningTotal > 0 && runningTotal == completed;
        LOGGER.trace("isComplete ? {} (runningTotal {}, completed = {}).",
              isComplete, runningTotal, completed);
        return isComplete;
    }

    boolean isFinal() {
        return to >= minMaxIndices[1];
    }

    boolean isNearline() {
        return nearline;
    }

    protected String getFormattedPercentDone() {
        String percent = runningTotal == 0 ?
              "?" :
              (runningTotal == completed ? "100" :
                    String.format("%.1f", 100 * (double) completed
                          / (double) runningTotal));
        if ("100.0".equals(percent)) {
            return "99.9";
        }
        return percent;
    }
}