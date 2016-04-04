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

import diskCacheV111.util.CacheException;
import org.dcache.resilience.util.PoolScanTask;
import org.dcache.resilience.util.PoolSelectionUnitDecorator.SelectionAction;
import org.dcache.resilience.util.ExceptionMessage;

/**
 * <p>Object stored in the {@link PoolOperationMap}.</p>
 */
final class PoolOperation {
    private static final String TO_STRING = "(completed: %s / %s : %s%%) – "
                    + "(updated: %s)(scanned: %s)(prev %s)(curr %s)(%s) %s";

    enum State {
        IDLE,       // NEXT OPERATION READY TO RUN
        WAITING,    // OPERATION SUBMITTED TO THE EXECUTOR
        RUNNING,    // OPERATION IS ACTIVE
        CANCELED,   // OPERATION WAS TERMINATED BY USER
        FAILED,     // OPERATION FAILED WITH EXCEPTION
        INACTIVE,   // RECEIVED AN IGNORE STATUS
        EXCLUDED    // POOL EXCLUDED FROM OPERATIONS
    }

    enum NextAction {
        NOP,            // REMAIN IN CURRENT QUEUE
        UP_TO_DOWN,     // PROMOTE TO WAITING
        DOWN_TO_UP,     // CANCEL ANY CURRENT DOWN AND PROMOTE TO WAITING
        DEACTIVATE      // SET DISABLED ON STATE
    }

    boolean forceScan;     // Overrides non-handling of restarts
    long lastUpdate;
    long lastScan;
    Integer group;         // Only set when the psuAction != NONE
    Integer unit;          // Only set when the psuAction == MODIFY
    State state;
    SelectionAction psuAction;
    PoolStatusForResilience lastStatus;
    PoolStatusForResilience currStatus;
    PoolScanTask task;
    CacheException exception;

    private int children;
    private int completed;

    PoolOperation() {
        forceScan = false;
        group = null;
        unit = null;
        state = State.IDLE;
        psuAction = SelectionAction.NONE;
        lastUpdate = System.currentTimeMillis();
        lastScan = lastUpdate;
        lastStatus = PoolStatusForResilience.UNINITIALIZED;
        currStatus = PoolStatusForResilience.UNINITIALIZED;
        children = 0;
        completed = 0;
    }

    public String toString() {
        return String.format(TO_STRING,
                        completed,
                        children == 0 && completed > 0 ? "?" : children,
                        getFormattedPercentDone(),
                        PnfsOperation.getFormattedDateFromMillis(lastUpdate),
                        PnfsOperation.getFormattedDateFromMillis(lastScan),
                        lastStatus, currStatus, state,
                        exception == null ? "" : new ExceptionMessage(exception));
    }

    /**
     * <p>Provides a transition table for determining what to do when
     *      a successive status change notification is received.</p>
     *
     * <p>Note that the UP_IGNORE and DOWN_IGNORE states are not stored.</p>
     */
    synchronized NextAction getNextAction(PoolStatusForResilience incoming) {
        switch(incoming) {
            case DOWN_IGNORE:
                currStatus = PoolStatusForResilience.DOWN;
                return state == State.EXCLUDED ? NextAction.NOP :
                                NextAction.DEACTIVATE;
            case UP_IGNORE:
                currStatus = currStatus == PoolStatusForResilience.DOWN ?
                                PoolStatusForResilience.READ_ONLY : currStatus;
                return state == State.EXCLUDED ? NextAction.NOP :
                                NextAction.DEACTIVATE;
            default:
                if (state == State.EXCLUDED) {
                    currStatus = incoming;
                    return NextAction.NOP;
                }
        }

        lastStatus = currStatus;
        currStatus = incoming;

        switch(lastStatus) {
            case DOWN:
                switch (currStatus) {
                    case READ_ONLY:
                    case ENABLED:
                        return NextAction.DOWN_TO_UP;
                    case DOWN:
                        return state == State.INACTIVE ? NextAction.UP_TO_DOWN :
                                        NextAction.NOP;
                }
            case READ_ONLY:
            case ENABLED:
                switch (currStatus) {
                    case DOWN:
                        return NextAction.UP_TO_DOWN;
                    case READ_ONLY:
                    case ENABLED:
                        return state == State.INACTIVE ? NextAction.DOWN_TO_UP :
                                        NextAction.NOP;
                }
                /*
                 *  The transition from UNINITIALIZED to another
                 *  state occurs when the resilience service comes on line,
                 *  whether simultaneously with pool initialization or
                 *  not.  The probability of pools going down at initialization
                 *  is low, but it is possible that resilience could go down
                 *  and then restart to find a number of pools down.  We
                 *  therefore have to consider the uninitialized-to-down
                 *  transition as actionable.  On the other hand, it is
                 *  less crucial to handle restarts from this initial state,
                 *  and preferable not to, since the majority of pools will
                 *  most of the time initialize to a viable readable status,
                 *  and handling this transition will unnecessarily provoke
                 *  an immediate system-wide scan.
                 */
            case UNINITIALIZED:
                switch (currStatus) {
                    case DOWN:
                        return NextAction.UP_TO_DOWN;
                    case READ_ONLY:
                    case ENABLED:
                        return NextAction.NOP;
                }
            default:
                return NextAction.NOP;
        }
    }

    synchronized void incrementCompleted() {
        if (state == State.RUNNING) {
            ++completed;
        }
    }

    synchronized boolean isComplete() {
        return children > 0 && children == completed;
    }

    synchronized void resetChildren() {
        children = 0;
        completed = 0;
    }

    synchronized void setChildren(int children) {
        if (state == State.RUNNING) {
            this.children = children;
        }
    }

    private String getFormattedPercentDone() {
        String percent = children == 0 ?
                        "?" :
                        (children == completed ? "100" :
                        String.format("%.1f", 100 * (double) completed
                                        / (double) children));
        if ("100.0".equals(percent)) {
            return "99.9";
        }
        return percent;
    }
}