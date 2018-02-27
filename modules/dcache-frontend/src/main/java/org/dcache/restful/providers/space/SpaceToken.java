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

import java.io.Serializable;

import diskCacheV111.services.space.Space;

@ApiModel("Container for metadata pertaining to a space token.")
public class SpaceToken implements Serializable {
    private static final long serialVersionUID = -3470575589163352322L;

    private long   id;
    private String voGroup;
    private String voRole;
    private String retentionPolicy;
    private String accessLatency;
    private long   linkGroupId;
    private long   sizeInBytes;
    private long   usedSizeInBytes;
    private long   allocatedSpaceInBytes;
    private long   creationTime;
    private Long   expirationTime;
    private String description;
    private String state;

    public SpaceToken() {
    }

    public SpaceToken(Space space) {
        id = space.getId();
        voGroup = space.getVoGroup();
        voRole = space.getVoRole();
        retentionPolicy = space.getRetentionPolicy().toString();
        accessLatency = space.getAccessLatency().toString();
        linkGroupId = space.getLinkGroupId();
        sizeInBytes = space.getSizeInBytes();
        usedSizeInBytes = space.getUsedSizeInBytes();
        allocatedSpaceInBytes = space.getAllocatedSpaceInBytes();
        creationTime = space.getCreationTime();
        expirationTime = space.getExpirationTime();
        description = space.getDescription();
        state = space.getState().name();
    }

    public String getAccessLatency() {
        return accessLatency;
    }

    public long getAllocatedSpaceInBytes() {
        return allocatedSpaceInBytes;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getDescription() {
        return description;
    }

    public Long getExpirationTime() {
        return expirationTime;
    }

    public long getId() {
        return id;
    }

    public long getLinkGroupId() {
        return linkGroupId;
    }

    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public String getState() {
        return state;
    }

    public long getUsedSizeInBytes() {
        return usedSizeInBytes;
    }

    public String getVoGroup() {
        return voGroup;
    }

    public String getVoRole() {
        return voRole;
    }

    public void setAccessLatency(String accessLatency) {
        this.accessLatency = accessLatency;
    }

    public void setAllocatedSpaceInBytes(long allocatedSpaceInBytes) {
        this.allocatedSpaceInBytes = allocatedSpaceInBytes;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setExpirationTime(Long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setLinkGroupId(long linkGroupId) {
        this.linkGroupId = linkGroupId;
    }

    public void setRetentionPolicy(String retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setUsedSizeInBytes(long usedSizeInBytes) {
        this.usedSizeInBytes = usedSizeInBytes;
    }

    public void setVoGroup(String voGroup) {
        this.voGroup = voGroup;
    }

    public void setVoRole(String voRole) {
        this.voRole = voRole;
    }
}
