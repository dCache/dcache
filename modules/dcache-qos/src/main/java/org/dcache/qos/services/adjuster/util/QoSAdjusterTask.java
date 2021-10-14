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
package org.dcache.qos.services.adjuster.util;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.adjuster.adjusters.CopyAdjuster;
import org.dcache.qos.services.adjuster.adjusters.QoSAdjuster;
import org.dcache.qos.services.adjuster.adjusters.QoSAdjusterFactory;
import org.dcache.qos.util.ErrorAwareTask;
import org.dcache.qos.util.ExceptionMessage;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.vehicles.FileAttributes;

/**
 * Responsible for running the adjuster.
 */
public final class QoSAdjusterTask extends ErrorAwareTask implements Cancellable {

    private static final String TO_STRING = "%s (%s %s)(src %s, tgt %s, retried %s)";
    private static final String TO_HISTORY_STRING = "%s (%s %s)(src %s, tgt %s, retried %s, secs %s) %s";

    private final PnfsId pnfsId;
    private final QoSAction type;
    private final int retry;
    private final QoSAdjusterFactory factory;
    private final FileAttributes attributes;
    private final PoolManagerPoolInformation targetInfo;
    private final String source;
    private final String poolGroup;

    private String target;
    private QoSAdjuster adjuster;
    private Future future;
    private Exception exception;

    /*
     *  Monitoring statistics.
     */
    private long startTime;
    private long endTime;

    enum Status {
        INITIALIZED, RUNNING, CANCELLED, DONE
    }

    private Status status;

    public QoSAdjusterTask(QoSAdjustmentRequest request, QoSAdjusterFactory factory) {
        this.pnfsId = request.getPnfsId();
        this.type = request.getAction();
        this.retry = 0;
        this.factory = factory;
        this.attributes = request.getAttributes();
        this.targetInfo = request.getTargetInfo();
        this.source = request.getSource();
        this.target = request.getTarget();
        this.poolGroup = request.getPoolGroup();
        this.status = Status.INITIALIZED;
    }

    public QoSAdjusterTask(QoSAdjusterTask task, int retry) {
        this.pnfsId = task.pnfsId;
        this.type = task.type;
        ;
        this.retry = retry;
        this.factory = task.factory;
        this.attributes = task.attributes;
        this.targetInfo = task.targetInfo;
        this.source = task.source;
        this.target = task.target;
        this.poolGroup = task.poolGroup;
        this.status = task.status;
    }

    @Override
    public void run() {
        synchronized (this) {
            status = Status.RUNNING;
            exception = null;
            adjuster = factory.newBuilder().of(type).build();
            startTime = System.currentTimeMillis();
        }

        switch (type) {
            case VOID:
                taskTerminated(Optional.empty(), null);
                break;
            default:
                if (isCancelled()) {
                    break;
                }
                adjuster.adjustQoS(this);
                break;
        }
    }

    @Override
    public void cancel(String explanation) {
        cancel();

        if (adjuster != null) {
            adjuster.cancel(explanation);
        }

        if (future != null) {
            future.cancel(true);
        }

        taskTerminated(Optional.empty(), null);
    }

    public Exception getException() {
        return exception;
    }

    public synchronized long getStartTime() {
        return startTime;
    }

    public synchronized long getEndTime() {
        return endTime;
    }

    public synchronized Status getStatus() {
        return status;
    }

    public synchronized String getStatusName() {
        return status.name();
    }

    public Integer getTypeValue() {
        if (type == null) {
            return null;
        }
        return type.ordinal();
    }

    public QoSAction getAction() {
        return type;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public int getRetry() {
        return retry;
    }

    public FileAttributes getAttributes() {
        return attributes;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getPoolGroup() {
        return poolGroup;
    }

    public PoolManagerPoolInformation getTargetInfo() {
        return targetInfo;
    }

    public synchronized boolean isCancelled() {
        return status == Status.CANCELLED;
    }

    public synchronized boolean isRunning() {
        return status == Status.RUNNING;
    }

    public synchronized boolean isDone() {
        return status == Status.DONE || status == Status.CANCELLED;
    }

    public void relayMessage(PoolMigrationCopyFinishedMessage message) {
        if (!message.getPnfsId().equals(pnfsId)) {
            return;
        }

        if (!(adjuster instanceof CopyAdjuster)) {
            String msg = String.format(
                  "migration copy finished message arrived for %s, but there is "
                        + "no corresponding migration task.", pnfsId);
            throw new IllegalStateException(msg);
        }

        ((CopyAdjuster) adjuster).relayMessage(message);
    }

    public synchronized void setFuture(Future future) {
        this.future = future;
    }

    public synchronized void taskTerminated(Optional<String> target, Exception exception) {
        if (target.isPresent()) {
            this.target = target.get();
        }
        if (!isDone()) {
            status = Status.DONE;
        }
        this.exception = exception;
        endTime = System.currentTimeMillis();
    }

    public String toString() {
        return String.format(TO_STRING,
              startTime == 0L ? "" : FileQoSUpdate.getFormattedDateFromMillis(startTime),
              pnfsId, type.name(),
              source == null ? "none" : source, target == null ? "none" : target,
              retry);
    }

    public String toHistoryString() {
        return String.format(TO_HISTORY_STRING,
              startTime == 0L ? "" : FileQoSUpdate.getFormattedDateFromMillis(startTime),
              pnfsId, type.name(),
              source == null ? "none" : source, target == null ? "none" : target,
              retry,
              TimeUnit.MILLISECONDS.toSeconds(endTime - startTime),
              exception == null ? "" : new ExceptionMessage(exception));
    }

    private synchronized void cancel() {
        this.status = Status.CANCELLED;
    }
}
