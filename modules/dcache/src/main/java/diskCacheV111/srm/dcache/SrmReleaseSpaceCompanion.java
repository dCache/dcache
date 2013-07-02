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

import diskCacheV111.services.space.message.Release;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.ThreadManagerMessageCallback;
import org.dcache.srm.SrmReleaseSpaceCallbacks;

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
public final class SrmReleaseSpaceCompanion
    extends AbstractMessageCallback<Release>
{

    private final static Logger _log = LoggerFactory.getLogger(SrmReleaseSpaceCompanion.class);
    private final long spaceToken;
    private final Long spaceToReleaseInBytes;
    private final SrmReleaseSpaceCallbacks callbacks;

    /** Creates a new instance of StageAndPinCompanion */
    private SrmReleaseSpaceCompanion(
            long spaceToken,
            Long spaceToReleaseInBytes,
            SrmReleaseSpaceCallbacks callbacks) {
        this.spaceToken = spaceToken;
        this.spaceToReleaseInBytes = spaceToReleaseInBytes;
        this.callbacks = callbacks;
    }

    @Override
    public void failure(int rc, Object error) {
        _log.error("Space Release Failed rc" + rc + " error:" + error);
        callbacks.ReleaseSpaceFailed("Space Release Failed rc" + rc +
                " error:" + error);
    }

    public void noroute() {
        _log.error("No Route to SrmSpaceManager");
        callbacks.ReleaseSpaceFailed("No Route to SrmSpaceManager");
    }

    @Override
    public void success(Release releaseResponse) {
        _log.trace("success");
        callbacks.SpaceReleased(
                Long.toString(releaseResponse.getSpaceToken()),
                releaseResponse.getRemainingSizeInBytes());
    }

    public void timeout() {
        _log.error("Timeout waiting for answer from SrmSpaceManager");
        callbacks.ReleaseSpaceFailed("Timeout waiting for answer from " +
                "SrmSpaceManager");
    }

    @Override
    public String toString() {

        return getClass().getName() + " token:" + spaceToken +
                " releasesize:" + spaceToReleaseInBytes;
    }

    public static void releaseSpace(
            Subject subject,
            long spaceToken,
            Long spaceToReleaseInBytes,
            SrmReleaseSpaceCallbacks callbacks,
            CellStub spaceManagerStub) {
        _log.trace("SrmReleaseSpaceCompanion.releaseSpace({}, token {}, spaceToReleaseInBytes {})",
                subject.getPrincipals(),  spaceToken, spaceToReleaseInBytes);

        SrmReleaseSpaceCompanion companion = new SrmReleaseSpaceCompanion(
                spaceToken,
                spaceToReleaseInBytes,
                callbacks);
        Release release =
                new Release(spaceToken,
                spaceToReleaseInBytes);
        release.setSubject(subject);
        spaceManagerStub.send(release, Release.class,
                new ThreadManagerMessageCallback(companion));
    }
}

