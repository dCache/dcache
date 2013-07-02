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
import diskCacheV111.services.space.SpaceAuthorizationException;
import diskCacheV111.services.space.SpaceExpiredException;
import diskCacheV111.services.space.SpaceReleasedException;
import diskCacheV111.services.space.message.Use;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.ThreadManagerMessageCallback;
import org.dcache.srm.SrmUseSpaceCallbacks;

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
public final class SrmMarkSpaceAsBeingUsedCompanion
    extends AbstractMessageCallback<Use>
{
    private final static Logger _log =
            LoggerFactory.getLogger(SrmMarkSpaceAsBeingUsedCompanion.class);
    private final long spaceToken;
    private final String pnfPath;
    private final long sizeInBytes;
    private final long markLifetime;
    private final SrmUseSpaceCallbacks callbacks;
    private final boolean overwrite;

    /** Creates a new instance of StageAndPinCompanion */
    private SrmMarkSpaceAsBeingUsedCompanion(
            long spaceToken,
            String pnfPath,
            long sizeInBytes,
            long markLifetime,
            boolean overwrite,
            SrmUseSpaceCallbacks callbacks) {
        this.spaceToken = spaceToken;
        this.pnfPath = pnfPath;
        this.sizeInBytes = sizeInBytes;
        this.markLifetime = markLifetime;
        this.callbacks = callbacks;
        this.overwrite = overwrite;
    }

    @Override
    public void failure(int rc, Object error) {
        _log.error("Marking Space as Being Used Failed rc:" + rc + " error:" +
                error);
        if (error == null) {
            callbacks.SrmUseSpaceFailed(
                    "Marking Space as Being Used failed");
        } else if (error instanceof NoFreeSpaceException) {
            callbacks.SrmNoFreeSpace(
                    ((NoFreeSpaceException) error).getMessage());
        } else if (error instanceof SpaceExpiredException) {
            callbacks.SrmExpired(
                    ((SpaceExpiredException) error).getMessage());
        } else if (error instanceof SpaceAuthorizationException) {
            callbacks.SrmNotAuthorized(
                    ((SpaceAuthorizationException) error).getMessage());
        } else if (error instanceof SpaceReleasedException) {
            callbacks.SrmReleased(
                    ((SpaceReleasedException) error).getMessage());
        } else if (error instanceof Exception) {
            callbacks.SrmUseSpaceFailed(
                    "Marking Space as Being Used failed =>" +
                    ((Exception) error).getMessage());
        } else {
            callbacks.SrmUseSpaceFailed(
                    "Marking Space as Being Used failed =>" + error);
        }
    }

    public void noroute() {
        _log.error("No Route to to SrmSpaceManager");
        callbacks.SrmUseSpaceFailed("No Route to SrmSpaceManager");
    }

    @Override
    public void success(Use message) {
        _log.trace("success");
        callbacks.SpaceUsed();
    }

    public void timeout() {
        _log.error("Timeout waiting for answer from SrmSpaceManager");
        callbacks.SrmUseSpaceFailed("Timeout waiting for answer from " +
                "SrmSpaceManager");
    }

    @Override
    public String toString() {
        return this.getClass().getName() + " token:" + spaceToken +
                " size: " + sizeInBytes +
                " path: " + pnfPath +
                " lifetime: " + markLifetime;
    }

    public static void markSpace(
            Subject subject,
            long spaceToken,
            String pnfsPath,
            long sizeInBytes,
            long markLifetime,
            boolean overwrite,
            SrmUseSpaceCallbacks callbacks,
            CellStub spaceManagerStub) {
        _log.trace("SrmMarkSpaceAsBeingUsedCompanion.markSpace({} spaceToken={} pnfsPath={} of {} bytes, mark lifetime={})",
                subject.getPrincipals(), spaceToken, pnfsPath, sizeInBytes, markLifetime);



        SrmMarkSpaceAsBeingUsedCompanion companion =
                new SrmMarkSpaceAsBeingUsedCompanion(
                spaceToken,
                pnfsPath,
                sizeInBytes,
                markLifetime,
                overwrite,
                callbacks);
        Use use = new Use(
                spaceToken,
                pnfsPath,
                null,
                sizeInBytes,
                markLifetime,
                overwrite);
        use.setSubject(subject);
        spaceManagerStub.send(use, Use.class,
                new ThreadManagerMessageCallback(companion));
    }
}

