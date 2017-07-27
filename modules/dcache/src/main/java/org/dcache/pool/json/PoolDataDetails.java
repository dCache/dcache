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
package org.dcache.pool.json;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;

import diskCacheV111.pools.json.PoolCostData;
import diskCacheV111.pools.json.PoolQueueData;

import dmg.cells.nucleus.CellInfo;

/**
 * <p>Corresponds to the information delivered
 * from the {@link org.dcache.pool.classic.PoolV4} using
 * {@link dmg.cells.nucleus.CellInfoProvider#getInfo(PrintWriter)} and
 * {@link dmg.cells.nucleus.CellInfoProvider#getCellInfo(CellInfo)}.</p>
 */
public class PoolDataDetails implements Serializable {
    private static final long serialVersionUID = -3630909338100368554L;

    public enum OnOff {
        ON, OFF
    }

    public enum Lsf {
        NONE, VOLATILE, PRECIOUS
    }

    public enum Duplicates {
        NONE, IGNORED, REFRESHED
    }

    public enum P2PMode {
        CACHED, PRECIOUS
    }

    private String      label;
    private InetAddress[] inetAddresses;
    private String      baseDir;
    private String      poolVersion;

    @Deprecated // Remove reportRemovals after 4.2 is branched.
    private OnOff       reportRemovals;
    private boolean     isRemovalReported;

    private String      poolMode;
    private Integer     poolStatusCode;
    private String      poolStatusMessage;

    @Deprecated // Remove cleanPreciousFiles after 4.2 is branched.
    private OnOff       cleanPreciousFiles;
    private boolean     isPreciousFileCleaned;

    @Deprecated // Remove suppressHsmLoad after 4.2 is branched.
    private OnOff       suppressHsmLoad;
    private boolean     isHsmLoadSuppressed;

    private Integer     pingHeartbeatInSecs;
    private Double      breakEven;
    private Lsf         largeFileStore;
    private Duplicates  duplicateRequests;
    private P2PMode     p2pFileMode;
    private Integer     hybridInventory;

    private int                 errorCode;
    private String              errorMessage;
    private Map<String, String> tagMap;
    private PoolCostData        costData;

    public String getBaseDir() {
        return baseDir;
    }

    public Double getBreakEven() {
        return breakEven;
    }

    public boolean isPreciousFileCleaned() {
        return isPreciousFileCleaned;
    }

    public PoolCostData getCostData() {
        return costData;
    }

    public Duplicates getDuplicateRequests() {
        return duplicateRequests;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Integer getHybridInventory() {
        return hybridInventory;
    }

    public InetAddress[] getInetAddresses() {
        return inetAddresses;
    }

    public String getLabel() {
        return label;
    }

    public Lsf getLargeFileStore() {
        return largeFileStore;
    }

    public P2PMode getP2pFileMode() {
        return p2pFileMode;
    }

    public Integer getPingHeartbeatInSecs() {
        return pingHeartbeatInSecs;
    }

    public String getPoolMode() {
        return poolMode;
    }

    public Integer getPoolStatusCode() {
        return poolStatusCode;
    }

    public String getPoolStatusMessage() {
        return poolStatusMessage;
    }

    public String getPoolVersion() {
        return poolVersion;
    }

    public boolean isRemovalReported() {
        return isRemovalReported;
    }

    public boolean isHsmLoadSuppressed() {
        return isHsmLoadSuppressed;
    }

    public Map<String, String> getTagMap() {
        return tagMap;
    }

    private String asOnOff(boolean value)
    {
        return value ? "ON" : "OFF";
    }

    public void print(PrintWriter pw) {
        pw.println("Base directory    : " + baseDir);
        pw.println("Version           : " + poolVersion);
        pw.println("Report remove     : " + asOnOff(isRemovalReported));
        pw.println("Pool Mode         : " + poolMode);
        if (poolStatusCode != null) {
            pw.println("Detail            : [" + poolStatusCode + "] "
                                       + poolStatusMessage);
        }
        pw.println("Clean prec. files : " + asOnOff(isPreciousFileCleaned));
        pw.println("Hsm Load Suppr.   : " + asOnOff(isHsmLoadSuppressed));
        pw.println("Ping Heartbeat    : " + pingHeartbeatInSecs + " seconds");
        pw.println("Breakeven         : " + breakEven);
        pw.println("LargeFileStore    : " + largeFileStore);
        pw.println("DuplicateRequests : " + duplicateRequests);
        pw.println("P2P File Mode     : " + p2pFileMode);

        if (hybridInventory != null) {
            pw.println("Inventory         : " + hybridInventory);
        }

        if (costData != null) {
            Map<String, PoolQueueData> movers = costData.getExtendedMoverHash();
            if (movers != null) {
                movers.values().stream()
                      .forEach((q) -> pw.println(
                                      "Mover Queue (" + q.getName() + ") "
                                                      + q.getActive()
                                                      + "(" + q.getMaxActive()
                                                      + ")/" + q.getQueued()));
            }
        }
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public void setBreakEven(Double breakEven) {
        this.breakEven = breakEven;
    }

    public void setPreciousFileCleaned(boolean isCleaned) {
        isPreciousFileCleaned = isCleaned;
    }

    public void setCostData(PoolCostData costData) {
        this.costData = costData;
    }

    public void setDuplicateRequests(
                    Duplicates duplicateRequests) {
        this.duplicateRequests = duplicateRequests;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setHybridInventory(Integer hybridInventory) {
        this.hybridInventory = hybridInventory;
    }

    public void setInetAddresses(InetAddress[] inetAddresses) {
        this.inetAddresses = inetAddresses;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setLargeFileStore(
                    Lsf largeFileStore) {
        this.largeFileStore = largeFileStore;
    }

    public void setP2pFileMode(
                    P2PMode p2pFileMode) {
        this.p2pFileMode = p2pFileMode;
    }

    public void setPingHeartbeatInSecs(Integer pingHeartbeatInSecs) {
        this.pingHeartbeatInSecs = pingHeartbeatInSecs;
    }

    public void setPoolMode(String poolMode) {
        this.poolMode = poolMode;
    }

    public void setPoolStatusCode(Integer poolStatusCode) {
        this.poolStatusCode = poolStatusCode;
    }

    public void setPoolStatusMessage(String poolStatusMessage) {
        this.poolStatusMessage = poolStatusMessage;
    }

    public void setPoolVersion(String poolVersion) {
        this.poolVersion = poolVersion;
    }

    public void setRemovalReported(boolean isReported) {
        isRemovalReported = isReported;
    }

    public void setHsmLoadSuppressed(boolean isSuppressed) {
        isHsmLoadSuppressed = isSuppressed;
    }

    public void setTagMap(Map<String, String> tagMap) {
        this.tagMap = tagMap;
    }

    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException
    {
        aInputStream.defaultReadObject();
        if (reportRemovals != null) {
            isRemovalReported = reportRemovals == OnOff.ON;
        }
        if (cleanPreciousFiles != null) {
            isPreciousFileCleaned = cleanPreciousFiles == OnOff.ON;
        }
        if (suppressHsmLoad != null) {
            isHsmLoadSuppressed = suppressHsmLoad == OnOff.ON;
        }
    }
}
