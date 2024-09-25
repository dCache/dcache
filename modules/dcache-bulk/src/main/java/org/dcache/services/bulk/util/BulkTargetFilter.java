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
package org.dcache.services.bulk.util;

import java.util.Set;
import org.dcache.services.bulk.util.BulkRequestTarget.State;

/**
 * Provides filtering for target queries.  Converts collections into arrays for easy use by JDBC.
 */
public final class BulkTargetFilter {

    private final Set<String> rids;
    private final Set<String> pnfsIds;
    private final Set<String> activities;
    private final Set<State> states;
    private final Set<String> types;
    private final Set<Integer> pid;
    private final Long offset;

    /**
     * General purpose filter (used with, e.g., admin interface calls).
     *
     * @param requestIds of request targets should belong to.
     * @param offset     beginning id of targets to fetch.
     * @param pid        node type of target.
     * @param pnfsIds    pnfsIds to match.
     * @param activities to match.
     * @param states     of targets.
     * @param types      file type of targets.
     */
    public BulkTargetFilter(Set<String> requestIds, Long offset, Set<Integer> pid,
          Set<String> pnfsIds, Set<String> activities, Set<String> types,
          Set<State> states) {
        this.rids = requestIds;
        this.offset = offset;
        this.pid = pid;
        this.pnfsIds = pnfsIds;
        this.activities = activities;
        this.states = states;
        this.types = types;
    }

    public String[] getRids() {
        return getArray(rids);
    }

    public Long getOffset() {
        return offset;
    }

    public Integer[] getPids() {
        return pid == null ? null : pid.toArray(Integer[]::new);
    }

    public String[] getPnfsIds() {
        return getArray(pnfsIds);
    }

    public String[] getActivities() {
        return getArray(activities);
    }

    public State[] getStates() {
        return states == null ? null : states.toArray(State[]::new);
    }

    public String[] getTypes() {
        return getArray(types);
    }

    private String[] getArray(Set<String> set) {
        return set == null ? null : set.toArray(String[]::new);
    }
}
