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

import com.google.common.collect.ComparisonChain;
import diskCacheV111.util.PnfsId;
import java.io.Serializable;

/**
 * <p>Information derived from the
 * {@link org.dcache.pool.nearline.NearlineStorageHandler}.</p>
 */
public class NearlineData implements Comparable<NearlineData>, Serializable {

    private static final long serialVersionUID = -3519944641370737320L;
    private String type;
    private String state;
    private String uuid;
    private PnfsId pnfsId;
    private String storageClass;
    private String uri;
    private long created;
    private long activated;
    private long running;
    private long totalElapsed;

    @Override
    public int compareTo(NearlineData o) {
        return ComparisonChain.start()
              .compare(created, o.created)
              .compare(activated, o.activated)
              .compare(state, o.state)
              .compare(storageClass, o.storageClass)
              .compare(pnfsId, o.pnfsId)
              .result();
    }

    public long getActivated() {
        return activated;
    }

    public long getCreated() {
        return created;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public long getRunning() {
        return running;
    }

    public String getState() {
        return state;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public long getTotalElapsed() {
        return totalElapsed;
    }

    public String getType() {
        return type;
    }

    public String getUri() {
        return uri;
    }

    public String getUuid() {
        return uuid;
    }

    public void setActivated(long activated) {
        this.activated = activated;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public void setPnfsId(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }

    public void setRunning(long running) {
        this.running = running;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public void setTotalElapsed(long totalElapsed) {
        this.totalElapsed = totalElapsed;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}
