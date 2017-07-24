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
package org.dcache.pool.nearline.json;

import java.io.PrintWriter;
import java.io.Serializable;

/**
 * <p>Corresponds to the information delivered
 * from the {@link org.dcache.pool.nearline.NearlineStorageHandler} using
 * {@link dmg.cells.nucleus.CellInfoProvider#getInfo(PrintWriter)}.</p>
 */
public class StorageHandlerData implements Serializable {
    private static final long serialVersionUID = -8358860515553038804L;
    private String label;
    private Long    restoreTimeoutInSeconds;
    private Long    storeTimeoutInSeconds;
    private Long    removeTimeoutInSeconds;
    private Integer activeStores;
    private Integer queuedStores;
    private Integer activeRestores;
    private Integer queuedRestores;
    private Integer activeRemoves;
    private Integer queuedRemoves;

    public Integer getActiveRemoves() {
        return activeRemoves;
    }

    public Integer getActiveRestores() {
        return activeRestores;
    }

    public Integer getActiveStores() {
        return activeStores;
    }

    public String getLabel() {
        return label;
    }

    public Integer getQueuedRemoves() {
        return queuedRemoves;
    }

    public Integer getQueuedRestores() {
        return queuedRestores;
    }

    public Integer getQueuedStores() {
        return queuedStores;
    }

    public Long getRemoveTimeoutInSeconds() {
        return removeTimeoutInSeconds;
    }

    public Long getRestoreTimeoutInSeconds() {
        return restoreTimeoutInSeconds;
    }

    public Long getStoreTimeoutInSeconds() {
        return storeTimeoutInSeconds;
    }

    public void print(PrintWriter pw) {
        pw.append(" Restore Timeout  : ").print(restoreTimeoutInSeconds);
        pw.println(" seconds");
        pw.append("   Store Timeout  : ").print(storeTimeoutInSeconds);
        pw.println(" seconds");
        pw.append("  Remove Timeout  : ").print(removeTimeoutInSeconds);
        pw.println(" seconds");
        pw.println("  Job Queues (active/queued)");
        pw.append("    to store   ").print(activeStores);
        pw.append("/").print(queuedStores);
        pw.println();
        pw.append("    from store ").print(activeRestores);
        pw.append("/").print(queuedRestores);
        pw.println();
        pw.append("    delete     " + "").print(activeRemoves);
        pw.append("/").print(queuedRemoves);
        pw.println();
    }

    public void setActiveRemoves(Integer activeRemoves) {
        this.activeRemoves = activeRemoves;
    }

    public void setActiveRestores(Integer activeRestores) {
        this.activeRestores = activeRestores;
    }

    public void setActiveStores(Integer activeStores) {
        this.activeStores = activeStores;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setQueuedRemoves(Integer queuedRemoves) {
        this.queuedRemoves = queuedRemoves;
    }

    public void setQueuedRestores(Integer queuedRestores) {
        this.queuedRestores = queuedRestores;
    }

    public void setQueuedStores(Integer queuedStores) {
        this.queuedStores = queuedStores;
    }

    public void setRemoveTimeoutInSeconds(Long removeTimeoutInSeconds) {
        this.removeTimeoutInSeconds = removeTimeoutInSeconds;
    }

    public void setRestoreTimeoutInSeconds(Long restoreTimeoutInSeconds) {
        this.restoreTimeoutInSeconds = restoreTimeoutInSeconds;
    }

    public void setStoreTimeoutInSeconds(Long storeTimeoutInSeconds) {
        this.storeTimeoutInSeconds = storeTimeoutInSeconds;
    }
}
