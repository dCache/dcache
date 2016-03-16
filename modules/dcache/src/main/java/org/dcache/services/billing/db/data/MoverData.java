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

import javax.security.auth.Subject;

import java.util.Map;
import java.util.Objects;

import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.auth.Subjects;

import static com.google.common.base.Strings.nullToEmpty;

/**
 * @author arossi
 */
public final class MoverData extends PnfsStorageInfo {

    private static final long serialVersionUID = 2538217391486197578L;

    public static final String TRANSFER_SIZE = "transferSize";

    private static final String DEFAULT_PROTOCOL = "<unknown>";

    public String toString() {
        return "(" + dateString() + "," + cellName + "," + action + ","
                        + transaction + "," + pnfsID + "," + fullSize + ","
                        + transferSize + "," + storageClass + "," + isNew
                        + "," + client + "," + connectionTime + ","
                        + errorCode + "," + errorMessage + "," + protocol
                        + "," + initiator + "," + p2p + ","
                        + owner + "," + mappedUID + "," + mappedGID +","
                        + nullToEmpty(fqan) + ")";
    }

    private Long transferSize;
    private Boolean isNew;
    private String client;
    private String protocol;
    private String initiator;
    private Boolean p2p;
    private String owner;
    private Integer mappedUID;
    private Integer mappedGID;
    private String fqan;

    public MoverData() {
        transferSize = 0L;
        isNew = false;
        protocol = DEFAULT_PROTOCOL;
        client = DEFAULT_PROTOCOL;
        p2p = false;
    }

    public MoverData(MoverInfoMessage info) {
        super(info, info.getConnectionTime(), info.getFileSize());
        transferSize = info.getDataTransferred();
        isNew = info.isFileCreated();

        if (info.getProtocolInfo() instanceof IpProtocolInfo) {
            protocol = info.getProtocolInfo()
                            .getVersionString();
            client = ((IpProtocolInfo) info.getProtocolInfo()).
                    getSocketAddress().getAddress().getHostAddress();
        } else {
            protocol = DEFAULT_PROTOCOL;
            client = DEFAULT_PROTOCOL;
        }

        StorageInfo sinfo = info.getStorageInfo();
        if (sinfo != null) {
            storageClass = sinfo.getStorageClass()
                            + "@"
                            + sinfo.getHsm();
        }

        initiator = info.getInitiator();
        p2p = info.isP2P();

        Subject subject = info.getSubject();
        owner = Subjects.getDn(subject);
        if (owner == null) {
            owner = Subjects.getUserName(subject);
        }
        long[] gids = Subjects.getGids(subject);
        mappedGID = (gids.length > 0) ? (int) gids[0] : -1;
        long[] uids = Subjects.getUids(subject);
        mappedUID = (uids.length > 0) ? (int) uids[0] : -1;
        fqan = Objects.toString(Subjects.getPrimaryFqan(info.getSubject()),null);
    }

    public Long getTransferSize() {
        return transferSize;
    }

    public void setTransferSize(Long transferSize) {
        this.transferSize = transferSize;
    }

    public Boolean getIsNew() {
        return isNew;
    }

    public void setIsNew(Boolean isNew) {
        this.isNew = isNew;
    }

    public Boolean isP2p() {
        return p2p;
    }

    public void setP2p(Boolean p2p) {
        this.p2p = p2p;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getInitiator() {
        return initiator;
    }

    public void setInitiator(String initiator) {
        this.initiator = initiator;
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

    public void setFqan(String fqan) {
        this.fqan = fqan;
    }

    public String getFqan() {
        return fqan;
    }

    @Override
    public Map<String, Double> data() {
        Map<String, Double> dataMap = super.data();
        dataMap.put(TRANSFER_SIZE, transferSize.doubleValue());
        return dataMap;
    }
}
