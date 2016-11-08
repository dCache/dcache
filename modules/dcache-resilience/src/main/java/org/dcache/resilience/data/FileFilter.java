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

import com.google.common.base.Splitter;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>Simple implementation of matcher.</p>
 */
public final class FileFilter implements FileMatcher {
    private Set<String> state;
    private Set<String> pnfsids;
    private String      retentionPolicy;
    private String      storageUnit;
    private String      parent;
    private String      source;
    private String      target;
    private Long        lastUpdateBefore;
    private Long        lastUpdateAfter;
    private Integer     opCount;
    private boolean     forceRemoval = false;

    private static boolean matchesPool(String toMatch,
                                       Integer operationValue,
                                       PoolInfoMap map) {
        if (toMatch == null) {
            return true;
        }

        if (toMatch.isEmpty()) {
            return operationValue == null;
        }

        Integer filterValue = map.getPoolIndex(toMatch);
        if (filterValue == null) {
            return false;
        }

        return filterValue.equals(operationValue);
    }

    @Override
    public boolean isForceRemoval() {
        return forceRemoval;
    }

    @Override
    public boolean isSimplePnfsMatch() {
        return pnfsids == null ? false : pnfsids.size() == 1;
    }

    public boolean isUndefined() {
        return (null == pnfsids || pnfsids.isEmpty()) &&
                        null == state &&
                        null == parent &&
                        null == source &&
                        null == target &&
                        null == retentionPolicy &&
                        null == storageUnit &&
                        null == opCount &&
                        null == lastUpdateBefore &&
                        null == lastUpdateAfter;
    }

    /**
     * <p>Filter components are treated as parts of an AND statement.</p>
     */
    @Override
    public boolean matches(FileOperation operation, PoolInfoMap map) {
        if (state != null && !state.contains(operation.getStateName())) {
            return false;
        }

        if (pnfsids != null && !pnfsids.contains(
                        operation.getPnfsId().toString())) {
            return false;
        }

        if (retentionPolicy != null && !retentionPolicy.equals(
                        operation.getRetentionPolicyName())) {
            return false;
        }

        if (storageUnit != null &&
                        !storageUnit.equals(map.getUnit(operation.getStorageUnit()))) {
            return false;
        }

        if (opCount != null && operation.getOpCount() != opCount) {
            return false;
        }

        Long lastUpdate = operation.getLastUpdate();

        if (lastUpdateBefore != null && lastUpdateBefore <= lastUpdate) {
            return false;
        }

        if (lastUpdateAfter != null && lastUpdateAfter >= lastUpdate) {
            return false;
        }

        if (!matchesPool(parent, operation.getParent(), map)) {
            return false;
        }

        if (!matchesPool(source, operation.getSource(), map)) {
            return false;
        }

        return matchesPool(target, operation.getTarget(), map);
    }

    public void setForceRemoval(boolean forceRemoval) {
        this.forceRemoval = forceRemoval;
    }

    public void setLastUpdateAfter(Long lastUpdateAfter) {
        this.lastUpdateAfter = lastUpdateAfter;
    }

    public void setLastUpdateBefore(Long lastUpdateBefore) {
        this.lastUpdateBefore = lastUpdateBefore;
    }

    public void setOpCount(Integer opCount) {
        this.opCount = opCount;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public void setPnfsIds(String pnfsIds) {
        if (pnfsIds != null) {
            this.pnfsids = new HashSet<>(Splitter.on(",").splitToList(pnfsIds));
        }
    }

    public void setRetentionPolicy(String retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setState(Set<String> states) {
            this.state = states;
    }

    public void setStorageUnit(String storageUnit) {
        this.storageUnit = storageUnit;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}
