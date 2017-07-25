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
package org.dcache.cells.json;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>Corresponds to the information delivered
 * via the {@link dmg.cells.nucleus.CellInfo} object.</p>
 */
public class CellData implements Serializable {
    private static final long serialVersionUID = 1861008223543700303L;
    private String  label;
    private String  cellName;
    private String  cellType;
    private String  cellClass;
    private String  domainName;
    private Date    creationTime;
    private Integer state;
    private Integer eventQueueSize;
    private Integer threadCount;
    private Long    expectedQueueTime;
    private String  version;
    private String  release;
    private String  revision;
    private long    roundTripTime;

    public String toString() {
        return "(" + cellName + "@" + domainName + ")("
                        + creationTime + ")("
                        + version + ")(state "
                        + state + ")(event queue "
                        + eventQueueSize + ")(threads "
                        + threadCount + ")(ping (ms) "
                        + roundTripTime + ")";
    }

    public String getCellClass() {
        return cellClass;
    }

    public String getCellName() {
        return cellName;
    }

    public String getCellType() {
        return cellType;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public String getDomainName() {
        return domainName;
    }

    public Integer getEventQueueSize() {
        return eventQueueSize;
    }

    public Long getExpectedQueueTime() {
        return expectedQueueTime;
    }

    public String getLabel() {
        return label;
    }

    public String getRelease() {
        return release;
    }

    public String getRevision() {
        return revision;
    }

    public long getRoundTripTime() {
        return roundTripTime;
    }

    public Integer getState() {
        return state;
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public String getVersion() {
        return version;
    }

    public void setCellClass(String cellClass) {
        this.cellClass = cellClass;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public void setCellType(String cellType) {
        this.cellType = cellType;
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public void setEventQueueSize(Integer eventQueueSize) {
        this.eventQueueSize = eventQueueSize;
    }

    public void setExpectedQueueTime(Long expectedQueueTime) {
        this.expectedQueueTime = expectedQueueTime;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public void setRoundTripTime(long roundTripTime) {
        this.roundTripTime = roundTripTime;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }

    public void setVersion(String version) {
        this.version = version;
    }

}
