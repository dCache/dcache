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
package org.dcache.pool.movers.json;

import java.io.Serializable;

/**
 * <p>Information derived from the
 * {@link org.dcache.pool.classic.MoverRequestScheduler}.</p>
 */
public class MoverData implements Serializable {
    private static final long serialVersionUID = 5130958721608671842L;
    private String pnfsId;
    private String queue;
    private String mode;
    private String door;
    private String  storageClass;
    private String  state;
    private Long    bytes;
    private Long    timeInSeconds;
    private Long    startTime;
    private Long    submitTime;
    private Long    lastModified;
    private Integer moverId;

    public Long getBytes() {
        return bytes;
    }

    public String getDoor() {
        return door;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public String getMode() {
        return mode;
    }

    public Integer getMoverId() {
        return moverId;
    }

    public String getPnfsId() {
        return pnfsId;
    }

    public String getQueue() {
        return queue;
    }

    public Long getStartTime() {
        return startTime;
    }

    public String getState() {
        return state;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public Long getSubmitTime() {
        return submitTime;
    }

    public Long getTimeInSeconds() {
        return timeInSeconds;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public void setDoor(String door) {
        this.door = door;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setMoverId(Integer moverId) {
        this.moverId = moverId;
    }

    public void setPnfsId(String pnfsId) {
        this.pnfsId = pnfsId;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public void setSubmitTime(Long submitTime) {
        this.submitTime = submitTime;
    }

    public void setTimeInSeconds(Long timeInSeconds) {
        this.timeInSeconds = timeInSeconds;
    }
}
