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
package org.dcache.restful.providers.restores;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import org.dcache.util.InvalidatableItem;

@ApiModel(description = "Container for metadata pertaining to a file stage from tape.")
public class RestoreInfo implements Comparable<RestoreInfo>, InvalidatableItem, Serializable {

    @ApiModelProperty("Identifies the transfer.")
    private String key;

    @ApiModelProperty("PnfsId of staged file.")
    private PnfsId pnfsId;

    @ApiModelProperty("Path of staged file.")
    private String path;

    @ApiModelProperty("Owner of the staged file.")
    private String owner;

    @ApiModelProperty("Owner group of the staged file.")
    private String ownerGroup;

    @ApiModelProperty("Net identifier of the staging host.")
    private String subnet;

    @ApiModelProperty("Pool selected for the stage.")
    private String poolCandidate;

    @ApiModelProperty("Staging began at this timestamp, in unix-time.")
    private Long started;

    @ApiModelProperty("Number of clients waiting for this file.")
    private Integer clients;

    @ApiModelProperty("Number of times staging failed and was retried.")
    private Integer retries;

    @ApiModelProperty("Current state of the request.")
    private String status;

    @ApiModelProperty("dCache error code.")
    private Integer error;

    @ApiModelProperty("Description of error, if any.")
    private String errorMessage;

    @ApiModelProperty("Validity of current request.")
    private boolean valid;

    public RestoreInfo() {
        valid = true;
    }

    public RestoreInfo(RestoreHandlerInfo info) {
        key = info.getName();
        int atIndex = key.indexOf('@');
        this.pnfsId = new PnfsId(key.substring(0, atIndex));
        this.subnet = key.substring(atIndex);
        poolCandidate = info.getPool();
        started = info.getStartTime();
        clients = info.getClientCount();
        retries = info.getRetryCount();
        status = info.getStatus();
        error = info.getErrorCode();
        errorMessage = info.getErrorMessage();
        valid = true;
    }

    @Override
    public int compareTo(RestoreInfo o) {
        return key.compareTo(o.key);
    }

    public Integer getClients() {
        return clients;
    }

    public Integer getError() {
        return error;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getKey() {
        return key;
    }

    public String getOwner() {
        return owner;
    }

    public String getOwnerGroup() {
        return ownerGroup;
    }

    public String getPath() {
        return path;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public String getPoolCandidate() {
        return poolCandidate;
    }

    public Integer getRetries() {
        return retries;
    }

    public Long getStarted() {
        return started;
    }

    public String getStatus() {
        return status;
    }

    public String getSubnet() {
        return subnet;
    }

    @Override
    public void invalidate() {
        valid = false;
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    public void setClients(Integer clients) {
        this.clients = clients;
    }

    public void setError(Integer error) {
        this.error = error;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setOwnerGroup(String ownerGroup) {
        this.ownerGroup = ownerGroup;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setPnfsId(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }

    public void setPoolCandidate(String poolCandidate) {
        this.poolCandidate = poolCandidate;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public void setStarted(Long started) {
        this.started = started;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setSubnet(String subnet) {
        this.subnet = subnet;
    }
}
