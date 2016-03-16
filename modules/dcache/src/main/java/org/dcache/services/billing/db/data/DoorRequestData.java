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
package org.dcache.services.billing.db.data;

import java.util.Map;
import java.util.Objects;

import diskCacheV111.vehicles.DoorRequestInfoMessage;

import org.dcache.auth.Subjects;

import static com.google.common.base.Strings.nullToEmpty;

/**
 * @author arossi
 */
public final class DoorRequestData extends PnfsConnectInfo {
    private static final long serialVersionUID = 4921127459667094459L;

    public static final String QUEUED_TIME = "queuedTime";

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + owner + "," + mappedUID + "," + mappedGID + ","
                        + "," + client + "," + transaction + "," + pnfsID
                        + "," + connectionTime + "," + queuedTime + ","
                        + errorCode + "," + errorMessage + "," + path + ","
                        + nullToEmpty(fqan) + ")";
    }

    private String owner;
    private Integer mappedUID;
    private Integer mappedGID;
    private String client;
    private String path;
    private Long queuedTime;
    private String fqan;

    public DoorRequestData() {
        queuedTime = 0L;
    }

    public DoorRequestData(DoorRequestInfoMessage info) {
        super(info, info.getTransactionDuration());
        queuedTime = info.getTimeQueued();
        owner = info.getOwner();
        mappedUID = info.getUid();
        mappedGID = info.getGid();
        client = info.getClient();
        path = info.getBillingPath();
        fqan = Objects.toString(Subjects.getPrimaryFqan(info.getSubject()),null);
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Integer getMappedUID() {
        return mappedUID;
    }

    public void setMappedUID(Integer mappedUID) {
        this.mappedUID = mappedUID;
    }

    public Integer getMappedGID() {
        return mappedGID;
    }

    public void setMappedGID(Integer mappedGID) {
        this.mappedGID = mappedGID;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public Long getQueuedTime() {
        return queuedTime;
    }

    public void setQueuedTime(Long queuedTime) {
        this.queuedTime = queuedTime;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFqan(String fqan) {
        this.fqan = fqan;
    }

    public String getFqan() {
        return fqan;
    }


    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(QUEUED_TIME, queuedTime.doubleValue());
        return dataMap;
    }
}
