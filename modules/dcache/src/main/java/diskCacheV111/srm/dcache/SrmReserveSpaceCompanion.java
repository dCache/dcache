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

/*
 * StageAndPinCompanion.java
 *
 * Created on January 2, 2003, 2:08 PM
 */
package diskCacheV111.srm.dcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.services.space.NoFreeSpaceException;
import diskCacheV111.services.space.SpaceException;
import diskCacheV111.services.space.message.Reserve;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.ThreadManagerMessageCallback;
import org.dcache.srm.SrmReserveSpaceCallbacks;

/**
 *
 * @author  timur
 */
/**
 * this class does all the dcache specific work needed for staging and pinning a
 * file represented by a path. It notifies the caller about each next stage
 * of the process via a StageAndPinCompanionCallbacks interface.
 * Boolean functions of the callback interface need to return true in order for
 * the process to continue
 */
public final class SrmReserveSpaceCompanion
    extends AbstractMessageCallback<Reserve>
{
    private final static Logger _log =
            LoggerFactory.getLogger(SrmReserveSpaceCompanion.class);
    private final long sizeInBytes;
    private final long spaceReservationLifetime;
    private final String retentionPolicy;
    private final String accessLatency;
    private final String description;
    private final SrmReserveSpaceCallbacks callbacks;

    /** Creates a new instance of StageAndPinCompanion */
    private SrmReserveSpaceCompanion(
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicy,
            String accessLatency,
            String description,
            SrmReserveSpaceCallbacks callbacks) {
        this.sizeInBytes = sizeInBytes;
        this.spaceReservationLifetime = spaceReservationLifetime;
        this.retentionPolicy = retentionPolicy;
        this.accessLatency = accessLatency;
        this.description = description;
        this.callbacks = callbacks;
    }

    @Override
    public void failure(int rc, Object error) {
        _log.error("Space Reservation Failed rc:" + rc + " error:" + error);
        if (error instanceof NoFreeSpaceException) {
            NoFreeSpaceException nfse = (NoFreeSpaceException) error;
            callbacks.NoFreeSpace(nfse.getMessage());
            return;
        }
        if (error instanceof SpaceException) {
            SpaceException se = (SpaceException) error;
            callbacks.ReserveSpaceFailed(se.getMessage());
            return;

        }
        callbacks.ReserveSpaceFailed("Space Reservation Failed rc:" + rc +
                " error:" + error);
    }

    @Override
    public void noroute(CellPath path) {
        _log.error("No Route to SrmSpaceManager");
        callbacks.ReserveSpaceFailed("No Route to SrmSpaceManager");
    }

    @Override
    public void success(Reserve reservationResponse) {
        _log.trace("success");
        callbacks.SpaceReserved(
                Long.toString(reservationResponse.getSpaceToken()),
                reservationResponse.getSizeInBytes());
    }

    @Override
    public void timeout(CellPath path) {
        _log.error("Timeout waiting for answer from SrmSpaceManager");
        callbacks.ReserveSpaceFailed("Timeout waiting for answer from " +
                "SrmSpaceManager");
    }

    @Override
    public String toString() {

        return this.getClass().getName() + " [size:" + sizeInBytes + " " +
                accessLatency + " " + retentionPolicy +
                " lifetime:" + spaceReservationLifetime + "]";
    }

    public static void reserveSpace(
            Subject subject,
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicyString,
            String accessLatencyString,
            String description,
            SrmReserveSpaceCallbacks callbacks,
            CellStub spaceManagerStub) {
        _log.trace(" SrmReserveSpaceCompanion.reserveSpace({} for {} bytes, access lat.={} retention pol.={} lifetime={})",
                subject.getPrincipals(), sizeInBytes, accessLatencyString, retentionPolicyString, spaceReservationLifetime);



        SrmReserveSpaceCompanion companion = new SrmReserveSpaceCompanion(
                sizeInBytes,
                spaceReservationLifetime,
                retentionPolicyString,
                accessLatencyString,
                description,
                callbacks);
        AccessLatency accessLatency = null;
        RetentionPolicy retentionPolicy = null;
        if (accessLatencyString != null) {
            try {
                accessLatency =
                        AccessLatency.getAccessLatency(accessLatencyString);
            } catch (IllegalArgumentException iae) {
                callbacks.ReserveSpaceFailed(iae);
                return;
            }
        }
        if (retentionPolicyString != null) {
            try {

                retentionPolicy =
                        RetentionPolicy.getRetentionPolicy(retentionPolicyString);
            } catch (IllegalArgumentException iae) {
                callbacks.ReserveSpaceFailed(iae);
                return;
            }
        }

        Reserve reserve =
                new Reserve(
                sizeInBytes,
                retentionPolicy,
                accessLatency,
                spaceReservationLifetime,
                description);
        reserve.setSubject(subject);
        spaceManagerStub.send(reserve, Reserve.class,
                new ThreadManagerMessageCallback(companion));
    }
}

