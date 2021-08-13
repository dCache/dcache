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

import diskCacheV111.util.CacheException;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.services.scanner.util.PoolScanTask;
import org.dcache.qos.util.ExceptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.qos.services.scanner.data.PoolScanOperation.State.WAITING;
import static org.dcache.qos.services.scanner.data.ScanOperation.ScanLabel.FINISHED;
import static org.dcache.qos.services.scanner.data.ScanOperation.ScanLabel.STARTED;

/**
 *  Object stored in the pool operation map.
 */
public final class PoolScanOperation extends ScanOperation<PoolScanTask> {
    private static final Logger LOGGER    = LoggerFactory.getLogger(PoolScanOperation.class);

    private static final String TO_STRING = "(completed: %s / %s : %s%%) – "
                    + "(updated: %s)(%s: %s)(prev %s)(curr %s)(%s) %s";

    enum State {
        IDLE,       /* NEXT OPERATION READY TO RUN               */
        WAITING,    /* OPERATION SUBMITTED, AWAITING EXECUTION   */
        RUNNING,    /* OPERATION IS ACTIVE                       */
        CANCELED,   /* OPERATION WAS TERMINATED BY USER          */
        FAILED,     /* OPERATION FAILED WITH EXCEPTION           */
        EXCLUDED    /* POOL EXCLUDED FROM OPERATIONS             */
    }

    enum NextAction {
        NOP,            /* REMAIN IN CURRENT QUEUE                         */
        UP_TO_DOWN,     /* PROMOTE TO WAITING                              */
        DOWN_TO_UP      /* CANCEL ANY CURRENT DOWN AND PROMOTE TO WAITING  */
    }

    final long initializationGracePeriod;

    boolean                 forceScan;  /* Overrides non-handling of restarts */
    String                  group;      /* Only set when the psuAction != NONE */
    String                  unit;       /* Set when unit has changed, or scan
                                           is periodic or initiated by command */
    State                   state;
    PoolQoSStatus           lastStatus;
    PoolQoSStatus           currStatus;
    CacheException          exception;

    private long children;

    PoolScanOperation(long initializationGracePeriod) {
        this.initializationGracePeriod = initializationGracePeriod;
        forceScan = false;
        group = null;
        unit = null;
        state = State.IDLE;
        lastUpdate = System.currentTimeMillis();
        lastScan = lastUpdate;
        scanLabel = FINISHED;
        lastStatus = PoolQoSStatus.UNINITIALIZED;
        currStatus = PoolQoSStatus.UNINITIALIZED;
        children = 0L;
        completed = 0L;
        failed = 0L;
    }

    public String toString() {
        scanLabel = state == State.RUNNING || state == WAITING ? STARTED : FINISHED;
        return String.format(TO_STRING,
                             completed,
                             children == 0 && completed > 0 ? "?" : children,
                             getFormattedPercentDone(),
                             FileQoSUpdate.getFormattedDateFromMillis(lastUpdate),
                             scanLabel.label(),
                             FileQoSUpdate.getFormattedDateFromMillis(lastScan),
                             lastStatus, currStatus, state,
                             exception == null ? getFailedMessage() :
                                             new ExceptionMessage(exception));
    }

    public synchronized boolean isExcluded() {
        return state == State.EXCLUDED;
    }

    /**
     *  Provides a transition table for determining what to do when
     *  a successive status change notification is received.
     */
    synchronized NextAction getNextAction(PoolQoSStatus incoming) {
        if (state == State.EXCLUDED) {
            currStatus = incoming;
            return NextAction.NOP;
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
                        return NextAction.NOP;
                }
            case READ_ONLY:
            case ENABLED:
                switch (currStatus) {
                    case DOWN:
                        return NextAction.UP_TO_DOWN;
                    case READ_ONLY:
                    case ENABLED:
                        return NextAction.NOP;
                }
            case UNINITIALIZED:
                /*
                 *  The transition from UNINITIALIZED to another
                 *  state occurs when the scanner comes on line,
                 *  whether simultaneously with pool initialization or
                 *  not.  The probability of pools going down at initialization
                 *  is low, but it is possible that the scanner could go down
                 *  and then restart to find a number of pools down.  We
                 *  therefore have to consider the uninitialized-to-down
                 *  transition as actionable.  We do so after initialized has not
                 *  gone to another state for more than the grace period.
                 *
                 *  On the other hand, it is less crucial to handle restarts
                 *  from this initial state, and preferable not to, since the
                 *  majority of pools will most of the time initialize to a
                 *  viable readable status, and handling this transition will
                 *  unnecessarily provoke an immediate system-wide scan.
                 */
                if (currStatus == PoolQoSStatus.DOWN) {
                    if (exceedsGracePeriod()) {
                        return NextAction.UP_TO_DOWN;
                    }
                    currStatus = lastStatus;
                }
                /*
                 *  Fall through to default
                 */
            default:
                return NextAction.NOP;
        }
    }

    protected synchronized void incrementCompleted(boolean failed) {
        LOGGER.trace("entering incrementCompleted, state {}, failed {}, "
                                     + "children {}, completed = {}.",
                     state, failed, children, completed );
        if (state == State.RUNNING) {
            ++completed;
            if (failed) {
                ++this.failed;
            }
            lastUpdate = System.currentTimeMillis();
        }
        LOGGER.trace("leaving incrementCompleted, state {}, failed {}, "
                                     + "children {}, completed = {}.",
                     state, failed, children, completed );
    }

    protected synchronized boolean isComplete() {
        boolean isComplete = children > 0 && children == completed;
        LOGGER.trace("isComplete {}, children {}, completed = {}.",
                     isComplete, children, completed );
        return isComplete;
    }

    synchronized long failedChildren() {
        return failed;
    }

    synchronized long getCompleted() { return completed; }

    synchronized void resetChildren() {
        children = 0L;
        completed = 0L;
    }

    synchronized void resetFailed() {
        failed = 0;
    }

    synchronized void setChildren(long children) {
        if (state == State.RUNNING) {
            this.children = children;
        }
    }

    private boolean exceedsGracePeriod() {
        return System.currentTimeMillis() - lastUpdate >= initializationGracePeriod;
    }

    protected String getFormattedPercentDone() {
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