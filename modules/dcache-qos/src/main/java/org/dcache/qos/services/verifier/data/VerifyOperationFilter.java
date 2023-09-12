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

import static org.dcache.qos.util.InitializerAwareCommand.getTimestamp;

import diskCacheV111.util.PnfsId;
import java.util.Arrays;
import java.util.function.Predicate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao.VerifyOperationCriterion;

/**
 * Abstracts user command filter, and provides methods for conversion into dao criteria and update.
 */
public class VerifyOperationFilter {
    private PnfsId[] ids;
    private QoSAction[] action;
    private QoSMessageType[] msgType;
    private VerifyOperationState[] state;
    private String storageUnit;
    private String poolGroup;
    private String parent;
    private String source;
    private String target;
    private Integer retried;
    private String arrivedBefore;
    private String arrivedAfter;
    private String lastUpdateBefore;
    private String lastUpdateAfter;
    private boolean reverse;

    public VerifyOperationCriterion toCriterion(VerifyOperationDao dao) {
        VerifyOperationCriterion criterion = dao.where();

        if (ids != null) {
            criterion.pnfsIds(ids);
        }

        criterion.messageType(msgType);

        if (arrivedBefore != null) {
            criterion.arrivedBefore(getTimestamp(arrivedBefore));
        }

        if (arrivedAfter != null) {
            criterion.arrivedAfter(getTimestamp(arrivedAfter));
        }

        if (storageUnit != null) {
            criterion.unit(storageUnit);
        }

        if (poolGroup != null) {
            criterion.group(poolGroup);
        }

        if (parent != null) {
            criterion.parent(parent);
        }

        if (source != null) {
            criterion.source(source);
        }

        if (target != null) {
            criterion.target(target);
        }

        if (reverse) {
            criterion.reverse(reverse);
        }

        return criterion;
    }

    /**
     * Filter components are treated as parts of an AND statement.
     */
    public Predicate toPredicate() {
        Predicate<VerifyOperation> matchesIds = operation -> ids == null
              || Arrays.stream(ids).anyMatch(id -> operation.getPnfsId().equals(id));

        Predicate<VerifyOperation> matchesParent = operation -> parent == null
              || parent.equals(operation.getParent());

        Predicate<VerifyOperation> matchesState = operation -> state == null
              || Arrays.stream(state).anyMatch(state -> operation.getState().equals(state));

        Predicate<VerifyOperation> matchesAction = operation -> action == null
              || Arrays.stream(action).anyMatch(action -> operation.getAction().equals(action));

        Predicate<VerifyOperation> matchesMsgType = operation -> msgType == null
              || Arrays.stream(msgType).anyMatch(type -> operation.getMessageType().equals(type));

        Predicate<VerifyOperation> matchesSource = operation -> source == null
              || source.equals(operation.getSource());

        Predicate<VerifyOperation> matchesTarget = operation -> target == null
              || target.equals(operation.getTarget());

        Predicate<VerifyOperation> matchesPoolGroup = operation -> poolGroup == null
              || poolGroup.equals(operation.getPoolGroup());

        Predicate<VerifyOperation> matchesStorageUnit = operation -> storageUnit == null
              || storageUnit.equals(operation.getStorageUnit());

        Predicate<VerifyOperation> matchesArrivedBefore = operation -> arrivedBefore == null
              || getTimestamp(arrivedBefore) <= operation.getArrived();

        Predicate<VerifyOperation> matchesArrivedAfter = operation -> arrivedAfter == null
              || getTimestamp(arrivedAfter) >= operation.getArrived();

        Predicate<VerifyOperation> matchesBefore = operation -> lastUpdateBefore == null
              || getTimestamp(lastUpdateBefore) <= operation.getLastUpdate();

        Predicate<VerifyOperation> matchesAfter = operation -> lastUpdateAfter == null
              || getTimestamp(lastUpdateAfter) >= operation.getLastUpdate();

        Predicate<VerifyOperation> matchesRetried = operation -> retried == null
              || operation.getRetried() < retried;

        return matchesIds.and(matchesParent)
              .and(matchesState)
              .and(matchesAction)
              .and(matchesMsgType)
              .and(matchesSource)
              .and(matchesTarget)
              .and(matchesPoolGroup)
              .and(matchesStorageUnit)
              .and(matchesBefore)
              .and(matchesArrivedBefore)
              .and(matchesArrivedAfter)
              .and(matchesAfter)
              .and(matchesRetried);
    }

    public void setPnfsIds(PnfsId... ids) {
        this.ids = ids;
    }

    public void setAction(QoSAction[] action) {
        this.action = action;
    }

    public void setMsgType(QoSMessageType[] msgType) {
        this.msgType = msgType;
    }

    public void setState(VerifyOperationState[] state) {
        this.state = state;
    }

    public void setStorageUnit(String storageUnit) {
        this.storageUnit = storageUnit;
    }

    public void setPoolGroup(String poolGroup) {
        this.poolGroup = poolGroup;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setRetried(Integer retried) {
        this.retried = retried;
    }

    public void setArrivedBefore(String arrivedBefore) {
        this.arrivedBefore = arrivedBefore;
    }

    public void setArrivedAfter(String arrivedAfter) {
        this.arrivedAfter = arrivedAfter;
    }

    public void setLastUpdateBefore(String lastUpdateBefore) {
        this.lastUpdateBefore = lastUpdateBefore;
    }

    public void setLastUpdateAfter(String lastUpdateAfter) {
        this.lastUpdateAfter = lastUpdateAfter;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }
}
