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
package org.dcache.resilience.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellPath;
import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.FileOperationHandler.Type;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.vehicles.resilience.ForceSystemStickyBitMessage;

/**
 * <p>Object stored in the {@link FileOperationMap}.</p>
 *
 * <p>Since this table may grow very large, two strategies have been
 *      adopted to try to reduce the memory footprint of each instance:</p>
 *
 * <ol>
 *  <li>Enums are replaced by int values and conversion methods.</li>
 *  <li>Only int indices referencing the {@link PoolInfoMap} are stored.</li>
 * </ol>
 *
 * <p>The latter choice is to minimize variable allocation to 4-byte primitives
 *      rather than 8-byte object references. This will have some impact on
 *      performance, but the trade-off is suggested by our decision to use only
 *      a simple persistence model for rollback/recovery purposes and otherwise
 *      rely on values stored in memory.</p>
 *
 * <p>A note on synchronization.  There are only three points of real
 *      contention possible on an operation object.</p>
 *
 * <ol>
 *  <li>Between various threads on the entry point operations
 *      {@link FileOperationHandler#handleLocationUpdate(FileUpdate)}
 *      {@link FileOperationHandler#handleScannedLocation(FileUpdate, Integer)}</li>
 *  <li>The task thread or arriving completion message thread setting
 *      state and the consumer reading state.</li>
 *  <li>The admin thread and the consumer setting state/operation on cancel.</li>
 * </ol>
 *
 * <p>In the first case, we need only synchronize on the opCount;
 *      in the second case, on state.  The third case again involves these
 *      two attributes, so they again must by synchronized.
 *      In all other cases, modification of the object attributes belongs
 *      solely to either the task or the consumer at distinct times, ie.
 *      at implicitly serialized instants.</p>
 *
 * <p>For the purposes of efficiency, we allow purely read-only access
 *      (such as through the admin interface or the checkpointing operation)
 *      to be unsynchronized.</p>
 */
public final class FileOperation {
    /*
     * Stored state. Instead of using enum, to leave less of a memory footprint.
     * As above.
     */
    public static final int REPLICA   = 0;
    public static final int OUTPUT    = 1;
    public static final int CUSTODIAL = 2;

    /**
     * Stored state. Instead of using enum, to leave less of a memory footprint.
     * The map storing operation markers is expected to be very large.
     * Order is significant.
     */
    static final int WAITING  = 0;     // NEXT TASK READY TO RUN
    static final int RUNNING  = 1;     // TASK SUBMITTED TO THE EXECUTOR
    static final int DONE     = 2;     // CURRENT TASK SUCCESSFULLY COMPLETED
    static final int CANCELED = 3;     // CURRENT TASK WAS TERMINATED BY USER
    static final int FAILED   = 4;     // CURRENT TASK FAILED WITH EXCEPTION
    static final int VOID     = 5;     // NO FURTHER WORK NEEDS TO BE DONE
    static final int ABORTED  = 6;     // CANNOT DO ANYTHING FURTHER
    static final int UNINITIALIZED = 7;

    private static final String TO_STRING =
                    "%s (%s %s)(%s %s)(parent %s, count %s, retried %s)";
    private static final String TO_HISTORY_STRING =
                    "%s (%s %s)(%s %s)(parent %s, retried %s) %s";
    private static final String FORMAT_STR = "E MMM dd HH:mm:ss zzz yyyy";

    /*
     * Hidden marker for null, used with int->Integer autoboxing.
     */
    private static final int NIL = -19;

    /**
     *  Seems formatting is not thread-safe, so we create a new format object
     *  at each call.
     */
    static String getFormattedDateFromMillis(long millis) {
        DateFormat format = new SimpleDateFormat(FORMAT_STR);
        format.setLenient(false);
        return format.format(new Date(millis));
    }

    private final PnfsId      pnfsId;
    private final long        size;
    private long              lastUpdate;
    private int               retentionPolicy;
    private int               selectionAction;
    private int               poolGroup;
    private int               storageUnit;
    private int               parent;
    private int               source;
    private int               target;
    private int               lastType;
    private int               state;
    private int               opCount;
    private int               retried;
    private boolean           checkSticky;

    private Collection<Integer> tried;
    private ResilientFileTask task;
    private CacheException    exception;

    FileOperation(PnfsId pnfsId, int pgroup, Integer sunit, int action,
                  int opCount, long size) {
        this(pnfsId, opCount, size);
        selectionAction = action;
        poolGroup = pgroup;
        storageUnit = setNilForNull(sunit);
        state = UNINITIALIZED;
    }

    FileOperation(PnfsId pnfsId, int opCount, long size) {
        this.pnfsId = pnfsId;
        this.opCount = opCount;
        retried = 0;
        this.size = size;
        state = UNINITIALIZED;
        storageUnit = NIL;
        parent = NIL;
        source = NIL;
        target = NIL;
        lastType = NIL;
        lastUpdate = System.currentTimeMillis();
    }

    @VisibleForTesting
    public FileOperation(FileOperation operation) {
        this(operation.pnfsId, operation.poolGroup, operation.storageUnit,
                        operation.selectionAction, operation.opCount,
                        operation.size);
        lastUpdate = operation.lastUpdate;
        parent = operation.parent;
        exception = operation.exception;
        retentionPolicy = operation.retentionPolicy;
        retried = operation.retried;
        source = operation.source;
        state = operation.state;
        target = operation.target;
        task = operation.task;
        if (operation.tried != null) {
            tried.addAll(operation.tried);
        }
    }

    /**
     * <p>This method fires and forgets a message to the source pool
     *    to set the system-owned sticky bit.</p>
     *
     * <p>While this may most of the time be a redundant operation, the
     *    rationale behind it is to prevent having to check the sticky
     *    bit during a pool scan to see whether the copy is a replica or
     *    merely a temporarily cached one (via an ad hoc p2p transfer).
     *    By forcing the system sticky bit, we ensure that resilient
     *    pools will never have simply cached copies.</p>
     *
     * <p>Checking/verifying requires a response, which means either a
     *    blocking call or yet another queue of messages to handle.
     *    Since this operation most of the time will not be crucial,
     *    an asynchronous best effort is the most efficient solution.</p>
     *
     * <p>Should the message/command fail for some reason, it will be
     *    recorded on the pool side.</p>
     */
    public void ensureSticky(PoolInfoMap poolInfoMap, CellStub pools) {
        if (!checkSticky) {
            return;
        }

        String pool = poolInfoMap.getPool(getNullForNil(source));

        pools.send(new CellPath(pool),
                   new ForceSystemStickyBitMessage(pool, pnfsId));
    }

    public CacheException getException() {
        return exception;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public synchronized int getOpCount() {
        return opCount;
    }

    public Integer getParent() {
        return getNullForNil(parent);
    }

    public String getPrincipalPool(PoolInfoMap map) {
        if (parent != NIL) {
            return map.getPool(parent);
        }

        if (source != NIL) {
            return map.getPool(source);
        }

        if (target != NIL) {
            return map.getPool(target);
        }

        return "UNDEFINED";
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public int getPoolGroup() {
        return poolGroup;
    }

    public int getRetentionPolicy() {
        return retentionPolicy;
    }

    public int getSelectionAction() {
        return selectionAction;
    }

    public long getSize() { return size; }

    public Integer getSource() {
        return getNullForNil(source);
    }

    public synchronized int getState() {
        return state;
    }

    public String getStateName() {
        switch (state) {
            case WAITING:
                return "WAITING";
            case RUNNING:
                return "RUNNING";
            case DONE:
                return "DONE";
            case CANCELED:
                return "CANCELED";
            case FAILED:
                return "FAILED";
            case VOID:
                return "VOID";
            case ABORTED:
                return "ABORTED";
            case UNINITIALIZED:
                return "UNINITIALIZED";
        }

        throw new IllegalArgumentException("No such state: " + state);
    }

    public Integer getStorageUnit() {
        return getNullForNil(storageUnit);
    }

    public Integer getTarget() {
        return getNullForNil(target);
    }

    public ResilientFileTask getTask() {
        return task;
    }

    public Set<Integer> getTried() {
        if (tried == null) {
            return Collections.EMPTY_SET;
        }
        return ImmutableSet.copyOf(tried);
    }

    public synchronized void incrementCount() {
        lastUpdate = System.currentTimeMillis();
        ++opCount;
    }

    public boolean isBackground() {
        return parent != NIL;
    }

    public synchronized void relay(PoolMigrationCopyFinishedMessage message) {
        if (task != null) {
            task.relayMessage(message);
        }
    }

    @VisibleForTesting
    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public synchronized void setOpCount(int opCount) {
        this.opCount = opCount;
    }

    public void setSource(Integer source) {
        this.source = setNilForNull(source);
    }

    public void setTarget(Integer target) {
        this.target = setNilForNull(target);
    }

    public synchronized void submit() {
        if (task != null) {
            task.submit();
        }
    }

    public String toString() {
        return String.format(TO_STRING, getFormattedDateFromMillis(lastUpdate),
                             pnfsId, getRetentionPolicyName(), lastTypeName(),
                             getStateName(), parent == NIL ? "none" : parent,
                             opCount, retried);
    }

    public String toHistoryString() {
        return String.format(TO_HISTORY_STRING, getFormattedDateFromMillis(lastUpdate),
                        pnfsId, getRetentionPolicyName(), lastTypeName(),
                        getStateName(), parent == NIL ? "none" : parent,
                        retried, exception == null ? "" : new ExceptionMessage(exception));
    }

    void abortOperation() {
        synchronized( this) {
            updateState(ABORTED);
            opCount = 0;
        }

        lastUpdate = System.currentTimeMillis();
        source = NIL;
        target = NIL;
    }

    void addSourceToTriedLocations() {
        if (source == NIL) {
            return;
        }

        if (tried == null) {
            tried = new HashSet<>();
        }
        tried.add(source);

        /*
         *  Sticky-bit verification only needs to be done
         *  on a new source location.  If a different source
         *  is to be attempted, the verification can be
         *  bypassed.
         */
        checkSticky = false;
    }

    void addTargetToTriedLocations() {
        if (target == NIL) {
            return;
        }

        if (tried == null) {
            tried = new HashSet<>();
        }
        tried.add(target);
    }

    boolean cancelCurrent() {
        synchronized( this) {
            if (isInTerminalState()) {
                return false;
            }

            updateState(CANCELED);
            --opCount;
        }

        lastUpdate = System.currentTimeMillis();
        if (task != null) {
            task.cancel();
        }
        retried = 0;

        return true;
    }

    String getRetentionPolicyName() {
        switch (retentionPolicy) {
            case OUTPUT:
                return "OUTPUT";
            case CUSTODIAL:
                return "CUSTODIAL";
            case REPLICA:
            default:
                return "REPLICA";
        }
    }

    int getRetried() {
        return retried;
    }

    Type getType() {
        if (task == null) {
            return Type.VOID;
        }
        return task.getType();
    }

    void incrementRetried() {
        ++retried;
    }

    void resetOperation() {
        synchronized (this) {
            updateState(WAITING);
        }
        task = null;
        exception = null;
        lastUpdate = System.currentTimeMillis();
    }

    void resetSourceAndTarget() {
        retried = 0;
        source = NIL;
        target = NIL;
    }

    void setVerifySticky(boolean checkSticky) {
        this.checkSticky = checkSticky;
    }

    void setLastType() {
        if (task != null) {
            lastType = task.getTypeValue();
        }
    }

    void setParentOrSource(Integer pool, boolean isParent) {
        if (isParent) {
            parent = setNilForNull(pool);
        } else {
            source = setNilForNull(pool);
        }
    }

    void setRetentionPolicy(String policy) {
        switch (policy) {
            case "REPLICA":
                retentionPolicy = REPLICA;
                break;
            case "OUTPUT":
                retentionPolicy = OUTPUT;
                break;
            case "CUSTODIAL":
                retentionPolicy = CUSTODIAL;
                break;
            default:
                throw new IllegalArgumentException("No such policy: " + policy);
        }
    }

    synchronized void setState(int state) {
        updateState(state);
    }

    @VisibleForTesting
    void setState(String state) {
        switch (state) {
            case "WAITING":     updateState(WAITING);   break;
            case "RUNNING":     updateState(RUNNING);   break;
            case "DONE":        updateState(DONE);      break;
            case "CANCELED":    updateState(CANCELED);  break;
            case "FAILED":      updateState(FAILED);    break;
            case "VOID":        updateState(VOID);      break;
            case "ABORTED":     updateState(ABORTED);   break;
            case "UNINITIALIZED":
                throw new IllegalArgumentException("Cannot set "
                                + "operation to UNINITIALIZED.");
            default:
                throw new IllegalArgumentException("No such state: " + state);
        }
    }

    void setTask(ResilientFileTask task) {
        this.task = task;
    }

    /**
     * <p>When another operation for this file/pnfsid is to be
     *    queued, we simply overwrite the appropriate fields on
     *    this one.</p>
     */
    void updateOperation(FileOperation operation) {
        if (operation.storageUnit != NIL) {
            storageUnit = operation.storageUnit;
        }

        if (operation.checkSticky) {
            checkSticky = true;
        }

        /*
         *  The incoming count can be safely added to this one;
         *  if the count exceeds the necessary number of operations
         *  to achieve what this operation must, the count will
         *  be zeroed out anyway when the correct replica state
         *  is reached.
         */
        opCount += operation.opCount;
    }

    boolean updateOperation(CacheException error) {
        synchronized (this) {
            if (isInTerminalState()) {
                return false;
            }

            if (error != null) {
                exception = error;
                updateState(FAILED);
            } else {
                updateState(DONE);
                --opCount;
                retried = 0;
            }
        }

        lastUpdate = System.currentTimeMillis();
        return true;
    }

    boolean voidOperation() {
        synchronized(this) {
            if (isInTerminalState()) {
                return false;
            }
            updateState(VOID);
            opCount = 0;
        }
        retried = 0;
        source = NIL;
        target = NIL;
        tried = null;
        lastUpdate = System.currentTimeMillis();
        return true;
    }

    private Integer getNullForNil(int value) {
        return value == NIL ? null : value;
    }

    private boolean isInTerminalState() {
        switch (state) {
            case UNINITIALIZED:
            case WAITING:
            case RUNNING:
                return false;
            default:
                return true;
        }
    }

    private synchronized String lastTypeName() {
        return lastType == NIL ? "" : Type.values()[lastType].toString();
    }

    private int setNilForNull(Integer value) {
        return value == null ? NIL : value;
    }

    private void updateState(int state) {
        if (this.state == state) {
            return;
        }

        this.state = state;
    }
}
