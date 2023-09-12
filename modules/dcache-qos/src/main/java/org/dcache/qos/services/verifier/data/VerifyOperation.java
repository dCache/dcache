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
package org.dcache.qos.services.verifier.data;

import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSAction.WAIT_FOR_STAGE;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.ABORTED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.DONE;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.FAILED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.READY;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.UNINITIALIZED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.WAITING;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.listeners.QoSAdjustmentListener;
import org.dcache.qos.util.ExceptionMessage;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;

/**
 * Object stored in the operation store.
 * <p/>
 * As a fresh instance of this object is dehydrated by the handler and the map as needed, no
 * synchronization is necessary.
 * <p/>
 * Has natural ordering defined on lastUpdate.
 */
public final class VerifyOperation implements Comparable<VerifyOperation> {

    private static final String TO_STRING = "%s (%s %s)(%s %s)(parent %s, src %s, tgt %s, retried %s) %s";
    private static final String TO_ARCHIVE_STRING = "%s (%s %s)(last adjustment: %s)(parent %s, retried %s) %s";

    private final PnfsId pnfsId;

    private Long arrived;
    private Long lastUpdate;

    private QoSMessageType messageType;
    private VerifyOperationState state;
    private QoSAction previousAction;
    private QoSAction action;

    private String poolGroup;
    private String storageUnit;
    private String parent;
    private String source;
    private String target;

    private int retried;
    /**
     * Estimated number of adjustments left
     */
    private int needed;

    private Collection<String> tried;
    private CacheException exception;

    public VerifyOperation(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
        state = UNINITIALIZED;
        previousAction = VOID;
        action = VOID;
    }

    @Override
    public int compareTo(VerifyOperation operation) {
        if (operation == null || operation.lastUpdate == null) {
            return -1;
        }
        if (lastUpdate == null) {
            return 1;
        }
        return lastUpdate.compareTo(operation.lastUpdate);
    }

    public void abortOperation() {
        state = ABORTED;
        lastUpdate = System.currentTimeMillis();
        source = null;
        target = null;
    }

    public void addSourceToTriedLocations() {
        if (source == null) {
            return;
        }

        if (tried == null) {
            tried = new HashSet<>();
        }

        tried.add(source);
    }

    public void addTargetToTriedLocations() {
        if (target == null) {
            return;
        }

        if (tried == null) {
            tried = new HashSet<>();
        }

        tried.add(target);
    }

    public QoSAction getAction() {
        return action;
    }

    public long getArrived() {
        return arrived;
    }

    public CacheException getException() {
        return exception;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public QoSMessageType getMessageType() {
        return messageType;
    }

    public int getNeededAdjustments() {
        return needed;
    }

    public String getParent() {
        return parent;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public String getPoolGroup() {
        return poolGroup;
    }

    public QoSAction getPreviousAction() {
        return previousAction;
    }

    public String getPrincipalPool() {
        if (parent != null) {
            return parent;
        }

        if (source != null) {
            return source;
        }

        if (target != null) {
            return target;
        }

        return null;
    }

    public int getRetried() {
        return retried;
    }

    public String getSource() {
        return source;
    }

    public VerifyOperationState getState() {
        return state;
    }

    public String getStorageUnit() {
        return storageUnit;
    }

    public String getTarget() {
        return target;
    }

    public Set<String> getTried() {
        if (tried == null) {
            return Set.of();
        }
        return Set.copyOf(tried);
    }

    public void incrementRetried() {
        ++retried;
    }

    public boolean isBackground() {
        return parent != null;
    }

    public boolean isInTerminalState() {
        switch (state) {
            case DONE:
            case CANCELED:
            case FAILED:
            case ABORTED:
                return true;
            default:
                return false;
        }
    }

    public void requestAdjustment(QoSAdjustmentRequest request,
          QoSAdjustmentListener adjustmentListener)
          throws QoSException {
        if (isInTerminalState()) {
            return;
        }

        QoSAction action = request.getAction();
        previousAction = this.action;
        this.action = action;
        adjustmentListener.fileQoSAdjustmentRequested(request);
        if (action == WAIT_FOR_STAGE) {
            setState(WAITING);
        }
    }

    /**
     * The operation has more work to do, or has failed and is eligible for retry.  In the latter
     * case, it is prioritized by resetting is last updated time to its arrival time.
     *
     * @param retry because the operation failed with a non-fatal exception.
     */
    public void resetOperation(boolean retry) {
        state = READY;
        exception = null;

        if (retry) {
            lastUpdate = arrived;
        } else {
            tried = null;
            if (needed < 2) {
                lastUpdate = arrived;
            } else {
                lastUpdate = System.currentTimeMillis();
            }
        }
    }

    public void resetSourceAndTarget() {
        retried = 0;
        source = null;
        target = null;
    }

    public void setAction(QoSAction action) {
        this.action = action;
    }

    public void setArrived(long arrived) {
        this.arrived = arrived;
    }

    public void setException(CacheException exception) {
        this.exception = exception;
    }

    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public void setMessageType(QoSMessageType messageType) {
        this.messageType = messageType;
    }

    public void setNeeded(int needed) {
        /*
         *  Once needed has been determined to be greater than 1,
         *  each successive verification will decrement this count.
         *  The original count needs to be preserved, however,
         *  so that the task does not inappropriately get moved
         *  to the head of the queue.
         */
        this.needed = Math.max(this.needed, needed);
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public void setPoolGroup(String poolGroup) {
        this.poolGroup = poolGroup;
    }

    public void setPreviousAction(QoSAction previousAction) {
        this.previousAction = previousAction;
    }

    public void setRetried(int retried) {
        this.retried = retried;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setState(VerifyOperationState state) {
        this.state = state;
    }

    public void setStorageUnit(String storageUnit) {
        this.storageUnit = storageUnit;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setTried(Set<String> tried) {
        this.tried = tried;
    }

    @Override
    public String toString() {
        return String.format(TO_STRING,
              FileQoSUpdate.getFormattedDateFromMillis(lastUpdate),
              pnfsId, messageType, previousAction,
              state, parent == null ? "none" : parent, source, target, retried,
              exception == null ? "" : new ExceptionMessage(exception));
    }

    public String toArchiveString() {
        return String.format(TO_ARCHIVE_STRING,
              FileQoSUpdate.getFormattedDateFromMillis(lastUpdate),
              pnfsId, messageType, previousAction, parent == null ? "none" : parent, retried,
              exception == null ? "" : new ExceptionMessage(exception));
    }

    public boolean updateOperation(CacheException error) {
        if (isInTerminalState()) {
            return false;
        }

        if (error != null) {
            exception = error;
            state = FAILED;
        } else {
            state = DONE;
            retried = 0;
        }

        lastUpdate = System.currentTimeMillis();
        previousAction = action;
        return true;
    }

    public void voidOperation() {
        if (!isInTerminalState()) {
            state = DONE;
        }
        retried = 0;
        source = null;
        target = null;
        tried = null;
        lastUpdate = System.currentTimeMillis();
        previousAction = action;
        action = VOID;
    }
}
