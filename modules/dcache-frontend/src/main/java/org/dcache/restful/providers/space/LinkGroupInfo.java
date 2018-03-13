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
package org.dcache.restful.providers.space;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.services.space.LinkGroup;

@ApiModel(description = "Container for metadata pertaining to a link group.")
public class LinkGroupInfo implements Serializable {
    private static final long serialVersionUID = -8947531878064847599L;

    @ApiModelProperty("The identifier of the group.")
    private long         id;

    @ApiModelProperty("The name of the group.")
    private String       name;

    @ApiModelProperty("Unreserved space in the group.")
    private long         availableSpace;

    @ApiModelProperty("Files with ONLINE access latency can be stored.")
    private boolean      onlineAllowed;

    @ApiModelProperty("Files with NEARLINE access latency can be stored.")
    private boolean      nearlineAllowed;

    @ApiModelProperty("Files with REPLICA retention policy can be stored.")
    private boolean      replicaAllowed;

    @ApiModelProperty("Files with OUTPUT retention policy can be stored.")
    private boolean      outputAllowed;

    @ApiModelProperty("Files with CUSTODIAL retention policy can be stored.")
    private boolean      custodialAllowed;

    @ApiModelProperty("List of VOs which can access this link group.")
    private List<VOInfo> vos;

    @ApiModelProperty("Last time the link group was updated.")
    private long         updateTime;

    @ApiModelProperty("Reserved space in the group.")
    private long         reservedSpace;

    public LinkGroupInfo() {
    }

    public LinkGroupInfo(LinkGroup group) {
        id = group.getId();
        name = group.getName();
        availableSpace = group.getAvailableSpace();
        onlineAllowed = group.isOnlineAllowed();
        nearlineAllowed = group.isNearlineAllowed();
        replicaAllowed = group.isReplicaAllowed();
        outputAllowed = group.isOutputAllowed();
        custodialAllowed = group.isCustodialAllowed();
        updateTime = group.getUpdateTime();
        reservedSpace = group.getReservedSpace();

        diskCacheV111.util.VOInfo[] voInfo = group.getVOs();

        if (voInfo != null) {
            vos = Stream.of(voInfo)
                        .map(VOInfo::new)
                        .collect(Collectors.toList());
        }
    }

    public long getAvailableSpace() {
        return availableSpace;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getReservedSpace() {
        return reservedSpace;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public List<VOInfo> getVos() {
        return vos;
    }

    public boolean isCustodialAllowed() {
        return custodialAllowed;
    }

    public boolean isNearlineAllowed() {
        return nearlineAllowed;
    }

    public boolean isOnlineAllowed() {
        return onlineAllowed;
    }

    public boolean isOutputAllowed() {
        return outputAllowed;
    }

    public boolean isReplicaAllowed() {
        return replicaAllowed;
    }

    public void setAvailableSpace(long availableSpace) {
        this.availableSpace = availableSpace;
    }

    public void setCustodialAllowed(boolean custodialAllowed) {
        this.custodialAllowed = custodialAllowed;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNearlineAllowed(boolean nearlineAllowed) {
        this.nearlineAllowed = nearlineAllowed;
    }

    public void setOnlineAllowed(boolean onlineAllowed) {
        this.onlineAllowed = onlineAllowed;
    }

    public void setOutputAllowed(boolean outputAllowed) {
        this.outputAllowed = outputAllowed;
    }

    public void setReplicaAllowed(boolean replicaAllowed) {
        this.replicaAllowed = replicaAllowed;
    }

    public void setReservedSpace(long reservedSpace) {
        this.reservedSpace = reservedSpace;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public void setVos(List<VOInfo> vos) {
        this.vos = vos;
    }
}
