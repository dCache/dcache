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

package diskCacheV111.srm.dcache;

import com.google.common.util.concurrent.MoreExecutors;
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
import org.dcache.srm.SrmReserveSpaceCallback;

public final class SrmReserveSpaceCompanion
        extends AbstractMessageCallback<Reserve>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmReserveSpaceCompanion.class);

    private final SrmReserveSpaceCallback callback;

    private SrmReserveSpaceCompanion(SrmReserveSpaceCallback callback)
    {
        this.callback = callback;
    }

    @Override
    public void failure(int rc, Object error)
    {
        LOGGER.error("Space Reservation Failed rc:" + rc + " error:" + error);
        if (error instanceof NoFreeSpaceException) {
            NoFreeSpaceException nfse = (NoFreeSpaceException) error;
            callback.noFreeSpace(nfse.getMessage());
        } else if (error instanceof SpaceException) {
            SpaceException se = (SpaceException) error;
            callback.failed(se.getMessage());
        } else {
            callback.failed("Space reservation failed:" + error);
        }
    }

    @Override
    public void noroute(CellPath path)
    {
        LOGGER.error("No Route to SrmSpaceManager");
        callback.internalError("Space manager unavailable");
    }

    @Override
    public void success(Reserve reservationResponse)
    {
        callback.success(
                Long.toString(reservationResponse.getSpaceToken()),
                reservationResponse.getSizeInBytes());
    }

    @Override
    public void timeout(String error)
    {
        LOGGER.error(error);
        callback.internalError("Space manager timeout");
    }

    public static void reserveSpace(
            Subject subject,
            long sizeInBytes,
            long spaceReservationLifetime,
            String retentionPolicyString,
            String accessLatencyString,
            String description,
            SrmReserveSpaceCallback callback,
            CellStub spaceManagerStub)
    {
        LOGGER.trace(" SrmReserveSpaceCompanion.reserveSpace({} for {} bytes, access lat.={} retention pol.={} lifetime={})",
                subject.getPrincipals(), sizeInBytes, accessLatencyString, retentionPolicyString, spaceReservationLifetime);

        SrmReserveSpaceCompanion companion = new SrmReserveSpaceCompanion(callback);
        AccessLatency accessLatency = null;
        RetentionPolicy retentionPolicy = null;
        if (accessLatencyString != null) {
            try {
                accessLatency =
                        AccessLatency.getAccessLatency(accessLatencyString);
            } catch (IllegalArgumentException iae) {
                callback.failed("Invalid access latency");
                return;
            }
        }
        if (retentionPolicyString != null) {
            try {
                retentionPolicy =
                        RetentionPolicy.getRetentionPolicy(retentionPolicyString);
            } catch (IllegalArgumentException iae) {
                callback.failed("Invalid retention policy");
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
        CellStub.addCallback(spaceManagerStub.send(reserve), companion, MoreExecutors.sameThreadExecutor());
    }
}

