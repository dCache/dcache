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

package org.dcache.chimera.quota;

import diskCacheV111.util.RetentionPolicy;

import org.dcache.util.ByteSizeParser;

import static org.dcache.util.ByteUnits.isoPrefix;
import static org.dcache.util.ByteUnits.isoSymbol;

public class Quota {

    private static final ByteSizeParser SIZE_PARSER = ByteSizeParser
            .using(isoPrefix(), isoSymbol())
            .requiring(l -> l >= 0, "Size must be non-negative.")
            .build();

    int id;
    private final long usedCustodialSpace;
    private final long usedReplicaSpace;
    private final long usedOutputSpace;

    private Long custodialSpaceLimit;
    private Long replicaSpaceLimit;
    private Long outputSpaceLimit;

    public Quota(int id,
                 long usedCustodialSpace,
                 Long custodialSpaceLimit,
                 long usedOutputSpace,
                 Long outputSpaceLimit,
                 long usedReplicaSpace,
                 Long replicaSpaceLimit) {
        this.id = id;
        this.usedCustodialSpace = usedCustodialSpace;
        this.usedReplicaSpace = usedReplicaSpace;
        this.usedOutputSpace = usedOutputSpace;
        this.custodialSpaceLimit = custodialSpaceLimit;
        this.outputSpaceLimit = outputSpaceLimit;
        this.replicaSpaceLimit = replicaSpaceLimit;
    }

    /**
     * An unique id associated with Quota (UID or GID)
     * @return int id
     */
    public int getId()
    {
        return id;
    }

    /**
     * Used CUSTODIAL space
     * @return used CUSTODIAL space
     */
    public long getUsedCustodialSpace()
    {
        return usedCustodialSpace;
    }

    /**
     * Used REPLICA space
     * @return used REPLICA space
     */
    public long getUsedReplicaSpace()
    {
        return usedReplicaSpace;
    }

    /**
     * Used OUTPUT space
     * @return used OUTPUT space
     */
    public long getUsedOutputSpace()
    {
        return usedOutputSpace;
    }

    /**
     * CUSTODIAL space limit, null is not set
     * @return Long CUSTODIAL space limit,
     */
    public Long getCustodialSpaceLimit()
    {
        return custodialSpaceLimit;
    }

    /**
     * REPLICA space limit, null is not set
     * @return Long REPLICA space limit,
     */
    public Long getReplicaSpaceLimit() {
        return replicaSpaceLimit;
    }

    /**
     * OUTPUT space limit, null is not set
     * @return Long OUTPUT space limit,
     */
    public Long getOutputSpaceLimit() {
        return outputSpaceLimit;
    }

    /**
     * set CUSTODIAL space limit, passing null means
     * set to "no limit" (no quota)
     */
    public void setCustodialSpaceLimit(Long custodialSpaceLimit)
    {
        this.custodialSpaceLimit = custodialSpaceLimit;
    }

    /**
     * set REPLICA space limit, passing null means
     * set to "no limit" (no quota)
     */
    public void setReplicaSpaceLimit(Long replicaSpaceLimit)
    {
        this.replicaSpaceLimit = replicaSpaceLimit;
    }

    /**
     * set OUTPUT space limit, passing null means
     * set to "no limit" (no quota)
     */
    public void setOutputSpaceLimit(Long outputSpaceLimit)
    {
        this.outputSpaceLimit = outputSpaceLimit;
    }

    /**
     * Check quota for a given RetentionPolicy
     * @param retentionPolicy
     * @return boolean true (under quota) false (over quota)
     */
    public boolean check(RetentionPolicy retentionPolicy) {
        if (retentionPolicy == RetentionPolicy.CUSTODIAL &&
                custodialSpaceLimit != null &&
                custodialSpaceLimit < usedCustodialSpace) {
            return false;
        }
        if (retentionPolicy == RetentionPolicy.REPLICA &&
                replicaSpaceLimit != null &&
                replicaSpaceLimit < usedReplicaSpace) {
            return false;
        }
        if (retentionPolicy == RetentionPolicy.OUTPUT &&
                outputSpaceLimit != null &&
                outputSpaceLimit < usedOutputSpace) {
            return false;
        }
        return true;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("id=").append(id).append(",");
        sb.append("custodial=").append(usedCustodialSpace).append("/").
                append(custodialSpaceLimit == null ? "NONE" : custodialSpaceLimit).append(",");
        sb.append("output=").append(usedOutputSpace).append("/").
                append(outputSpaceLimit == null ? "NONE" : outputSpaceLimit).append(",");
        sb.append("replica=").append(usedReplicaSpace).append("/").
                append(replicaSpaceLimit == null ? "NONE" : replicaSpaceLimit);
        return sb.toString();
    }

    /**
     * A method to set Quota numbers based on string input
     * @param q Quota
     * @param custodial String
     * @param output String
     * @param replica String
     */
    public static void furnishQuota(Quota q,
                                    String custodial,
                                    String output,
                                    String replica) {
        if (custodial != null) {
            Long custodialLimit = custodial.equalsIgnoreCase("null") ?
                null : SIZE_PARSER.parse(custodial);
            q.setCustodialSpaceLimit(custodialLimit);
        }
        if (output != null) {
            Long outputLimit = output.equalsIgnoreCase("null") ?
                null : SIZE_PARSER.parse(output);
            q.setOutputSpaceLimit(outputLimit);
        }
        if (replica != null) {
            Long replicaLimit = replica.equalsIgnoreCase("null") ?
                null : SIZE_PARSER.parse(replica);
            q.setReplicaSpaceLimit(replicaLimit);
        }
    }

}
