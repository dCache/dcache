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
package org.dcache.restful.providers.pool;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

import diskCacheV111.repository.CacheRepositoryEntryInfo;

import org.dcache.pool.json.PoolData;
import org.dcache.util.histograms.Histogram;

/**
 * <p>Some of the fields may be <code>null</code>, depending on the
 * type of request this is used to respond to.</p>
 */
@ApiModel(description = "Container for all metadata requests pertaining to a pool.")
public class PoolInfo implements Serializable {
    private static final long serialVersionUID = 5758816176471906326L;

    @ApiModelProperty("Multi-part element carrying pool configuration and state data.")
    private PoolData poolData;

    @ApiModelProperty("If the request includes data for a specific file, "
                    + "this is the parsed repository info corresponding to the"
                    + " encoded listing string.")
    private CacheRepositoryEntryInfo pnfsidInfo;

    @ApiModelProperty("If the request includes data for a specific file, "
                    + "this is the encoded string that appears when the admin "
                    + "command 'rep ls' is issued on it.")
    private String repositoryListing;

    @ApiModelProperty("Histogram data for the mover "
                    + "activity statistics on the pool.")
    private Histogram[] queueStat;

    @ApiModelProperty("Histogram data for the file lifetime statistics"
                    + " on the pool.")
    private Histogram[] fileStat;

    public Histogram[] getFileStat() {
        return fileStat;
    }

    public CacheRepositoryEntryInfo getPnfsidInfo() {
        return pnfsidInfo;
    }

    public PoolData getPoolData() {
        return poolData;
    }

    public Histogram[] getQueueStat() {
        return queueStat;
    }

    public String getRepositoryListing() {
        return repositoryListing;
    }

    public void setFileStat(Histogram[] fileStat) {
        this.fileStat = fileStat;
    }

    public void setPnfsidInfo(
                    CacheRepositoryEntryInfo pnfsidInfo) {
        this.pnfsidInfo = pnfsidInfo;
    }

    public void setPoolData(PoolData poolData) {
        this.poolData = poolData;
    }

    public void setQueueStat(Histogram[] queueStat) {
        this.queueStat = queueStat;
    }

    public void setRepositoryListing(String repositoryListing) {
        this.repositoryListing = repositoryListing;
    }
}
