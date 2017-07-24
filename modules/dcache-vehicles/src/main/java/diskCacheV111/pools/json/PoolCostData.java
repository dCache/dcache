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
package diskCacheV111.pools.json;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;

/**
 * <p>Bean analogous to {@link PoolCostInfo}.</p>
 */
public class PoolCostData implements Serializable {
    private static final long serialVersionUID = 4425027701522401625L;
    private String label;
    private PoolQueueData              mover;
    private PoolQueueData              store;
    private PoolQueueData              restore;
    private PoolQueueData              p2p;
    private PoolQueueData              p2pClient;
    private Map<String, PoolQueueData> extendedMoverHash;
    private String                     defaultQueueName;
    private PoolSpaceData              space;
    private double                     moverCostFactor;

    public PoolCostData() {
    }

    public PoolCostData(PoolCostInfo info) {
        label = "Pool Cost Info";
        moverCostFactor = info.getMoverCostFactor();
        defaultQueueName = info.getDefaultQueueName();
        store = new PoolQueueData(info.getStoreQueue());
        restore = new PoolQueueData(info.getRestoreQueue());
        p2p = new PoolQueueData(info.getP2pQueue());
        p2pClient = new PoolQueueData(info.getP2pClientQueue());
        mover = new PoolQueueData(info.getMoverQueue());
        extendedMoverHash = new HashMap<>();
        info.getExtendedMoverHash()
            .entrySet()
            .stream()
            .forEach((e) -> extendedMoverHash.put(e.getKey(),
                                                  new PoolQueueData(
                                                                  e.getValue())));
        space = new PoolSpaceData(info.getSpaceInfo());
    }

    public String getDefaultQueueName() {
        return defaultQueueName;
    }

    public Map<String, PoolQueueData> getExtendedMoverHash() {
        return extendedMoverHash;
    }

    public String getLabel() {
        return label;
    }

    public PoolQueueData getMover() {
        return mover;
    }

    public double getMoverCostFactor() {
        return moverCostFactor;
    }

    public PoolQueueData getP2p() {
        return p2p;
    }

    public PoolQueueData getP2pClient() {
        return p2pClient;
    }

    public PoolQueueData getRestore() {
        return restore;
    }

    public PoolSpaceData getSpace() {
        return space;
    }

    public PoolQueueData getStore() {
        return store;
    }

    public void setDefaultQueueName(String defaultQueueName) {
        this.defaultQueueName = defaultQueueName;
    }

    public void setExtendedMoverHash(
                    Map<String, PoolQueueData> extendedMoverHash) {
        this.extendedMoverHash = extendedMoverHash;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setMover(PoolQueueData mover) {
        this.mover = mover;
    }

    public void setMoverCostFactor(double moverCostFactor) {
        this.moverCostFactor = moverCostFactor;
    }

    public void setP2p(PoolQueueData p2p) {
        this.p2p = p2p;
    }

    public void setP2pClient(PoolQueueData p2pClient) {
        this.p2pClient = p2pClient;
    }

    public void setRestore(PoolQueueData restore) {
        this.restore = restore;
    }

    public void setSpace(PoolSpaceData space) {
        this.space = space;
    }

    public void setStore(PoolQueueData store) {
        this.store = store;
    }
}
