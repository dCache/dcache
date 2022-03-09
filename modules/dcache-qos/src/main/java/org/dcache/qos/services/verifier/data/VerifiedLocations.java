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
package org.dcache.qos.services.verifier.data;

import diskCacheV111.util.PnfsId;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import org.dcache.vehicles.qos.ReplicaStatusMessage;

/**
 * The following table illustrates the classification of a file with each type of replica location.
 * <p/>
 * <table>
 *  <thead>
 *    <th>
 *        <td style="text-align: center;">Cached+Sticky</td>
 *        <td style="text-align: center;">Precious+Sticky</td>
 *        <td style="text-align: center;">Precious</td>
 *        <td style="text-align: center;">Cached</td>
 *        <td style="text-align: center;">Broken</td>
 *        <td style="text-align: center;">Removed</td>
 *        <td style="text-align: center;">OFFLINE</td>
 *        <td style="text-align: center;">EXCLUDED</td>
 *    </th>
 *  </thead>
 *  <tbody>
 *    <tr>
 *            <td style="text-align: left;">CURRENT DISK LOCATIONS</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;">B2</td>
 *            <td style="text-align: center;">C</td>
 *            <td style="text-align: center;">D</td>
 *            <td style="text-align: center;">[E]</td>
 *            <td style="text-align: center;">F</td>
 *            <td style="text-align: center;">G</td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">VERIFIED</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;">B2</td>
 *            <td style="text-align: center;">C</td>
 *            <td style="text-align: center;">D</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">G</td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">BROKEN</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">D</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">OCCUPIED</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;">B2</td>
 *            <td style="text-align: center;">C</td>
 *            <td style="text-align: center;">D</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">F</td>
 *            <td style="text-align: center;">G</td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">READABLE</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;">B2</td>
 *            <td style="text-align: center;">C</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">[E]</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">G</td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">VIABLE</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;">B2</td>
 *            <td style="text-align: center;">C</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">G</td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">PERSISTENT</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">G</td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">PERSISTENT W/O EXCLUDED</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;">B1</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">CACHED</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;">C</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *    </tr>
 *    <tr>
 *            <td style="text-align: left;">REMOVABLE</td>
 *            <td style="text-align: center;">A</td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *            <td style="text-align: center;"></td>
 *    </tr>
 *  </tbody>
 * </table>
 * p/>
 * Note that "REMOVABLE" here means from the standpoint of being able to reduce the number of
 * persistent replicas, not in the sense of the file not having a sticky flag; i.e., it tells
 * the engine whether the replica can be cached or not.  In effect, only cached+sticky files
 * belong to this class.
 */
public final class VerifiedLocations implements Serializable {

    private static final long serialVersionUID = -987104125231438126L;

    private final PnfsId pnfsId;

    /*
     *  Locations as reported in the namespace.
     */
    private Collection<String> currentDiskLocations;
    private Collection<String> currentTapeLocations;

    /*
     *  Disk locations verified from the pool repository.
     */
    private Set<String> broken;
    private Set<String> readable;
    private Set<String> exist;
    private Set<String> viable;
    private Set<String> persistent;
    private Set<String> members;
    private Set<String> occupied;
    private Set<String> cached;
    private Set<String> excluded;
    private Set<String> precious;

    /*
     *  Set by verifier from the pool information.
     */
    private Set<String> hsm;

    /*
     *  For each location, a verification message is provided by the verifier.
     */
    private Collection<ReplicaStatusMessage> replicaStatus;

    public VerifiedLocations(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public Collection<String> getCurrentDiskLocations() {
        return currentDiskLocations;
    }

    public void setCurrentDiskLocations(Collection<String> currentDiskLocations) {
        this.currentDiskLocations = currentDiskLocations;
    }

    public Collection<String> getCurrentTapeLocations() {
        return currentTapeLocations;
    }

    public void setCurrentTapeLocations(Collection<String> currentTapeLocations) {
        this.currentTapeLocations = currentTapeLocations;
    }

    public Set<String> getBroken() {
        return broken;
    }

    public void setBroken(Set<String> broken) {
        this.broken = broken;
    }

    public Set<String> getReadable() {
        return readable;
    }

    public void setReadable(Set<String> readable) {
        this.readable = readable;
    }

    public Set<String> getExist() {
        return exist;
    }

    public void setExist(Set<String> exist) {
        this.exist = exist;
    }

    public Set<String> getViable() {
        return viable;
    }

    public void setViable(Set<String> viable) {
        this.viable = viable;
    }

    public Set<String> getPersistent() {
        return persistent;
    }

    public void setPersistent(Set<String> persistent) {
        this.persistent = persistent;
    }

    public Set<String> getPrecious() {
        return precious;
    }

    public void setPrecious(Set<String> precious) {
        this.precious = precious;
    }

    public Set<String> getMembers() {
        return members;
    }

    public void setMembers(Set<String> members) {
        this.members = members;
    }

    public Set<String> getOccupied() {
        return occupied;
    }

    public void setOccupied(Set<String> occupied) {
        this.occupied = occupied;
    }

    public Set<String> getCached() {
        return cached;
    }

    public void setCached(Set<String> cached) {
        this.cached = cached;
    }

    public Set<String> getExcluded() {
        return excluded;
    }

    public void setExcluded(Set<String> excluded) {
        this.excluded = excluded;
    }

    public Set<String> getHsm() {
        return hsm;
    }

    public void setHsm(Set<String> hsm) {
        this.hsm = hsm;
    }

    public Collection<ReplicaStatusMessage> getReplicaStatus() {
        return replicaStatus;
    }

    public void setReplicaStatus(Collection<ReplicaStatusMessage> replicaStatus) {
        this.replicaStatus = replicaStatus;
    }
}
