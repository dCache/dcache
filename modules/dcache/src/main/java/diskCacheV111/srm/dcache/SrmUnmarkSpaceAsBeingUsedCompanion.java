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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.services.space.message.CancelUse;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;

import org.dcache.cells.CellStub;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;

/**
 *
 * @author  timur
 */
/**
 * send message to SrmSpaceManager to cancel use space for file
 * with a given pnfspath.
 */
public final class SrmUnmarkSpaceAsBeingUsedCompanion {
    private final static Logger _log =
            LoggerFactory.getLogger(SrmUnmarkSpaceAsBeingUsedCompanion.class);
    private final long spaceToken;
    private final String pnfPath;
    private final SrmCancelUseOfSpaceCallbacks callbacks;

    /** Creates a new instance of StageAndPinCompanion */
    private SrmUnmarkSpaceAsBeingUsedCompanion(
            long spaceToken,
            String pnfPath,
            SrmCancelUseOfSpaceCallbacks callbacks) {
        this.spaceToken = spaceToken;
        this.pnfPath = pnfPath;
        this.callbacks = callbacks;
    }

    @Override
    public String toString() {
        return getClass().getName() + " [token:" + spaceToken +
            " path:" + pnfPath + "]";
    }

    public void failure(int rc, Object error) {
        _log.error("Unmarking Space as Being Used Failed: rc=" + rc +
                   " error:" + error);
        callbacks.CancelUseOfSpaceFailed(
                                         "Unmarking Space as Being Used failed: rc=" + rc +
                                         " error:" + error);
    }

    public void noroute() {
        _log.error("Unmarking Space as Being Used Failed : No route to " +
                   "SrmSpaceManager");
        callbacks.CancelUseOfSpaceFailed("Unmarking Space as Being Used " +
                                         "Failed : No Route to SrmSpaceManager");
    }

    public void success(CancelUse message) {
        _log.debug("success");
        callbacks.UseOfSpaceSpaceCanceled();
    }

    public void timeout() {
        _log.error("Timeout waiting for answer from SrmSpaceManager");
        callbacks.CancelUseOfSpaceFailed("Timeout waiting for answer from " +
                "SrmSpaceManager");
    }

    public static void unmarkSpace(
            Subject subject,
            long spaceToken,
            String pnfsPath,
            SrmCancelUseOfSpaceCallbacks callbacks,
            CellStub spaceManagerStub) {
        _log.trace("SrmMarkSpaceAsBeingUsedCompanion.markSpace({} for spaceToken={} pnfsPath={})",
                subject.getPrincipals(), spaceToken, pnfsPath);
        SrmUnmarkSpaceAsBeingUsedCompanion companion =
                new SrmUnmarkSpaceAsBeingUsedCompanion(
                spaceToken,
                pnfsPath,
                callbacks);
        CancelUse cancelUse =
                new CancelUse(
                spaceToken,
                pnfsPath,
                null);
        cancelUse.setReplyRequired(true);
        try {
            cancelUse=spaceManagerStub.sendAndWait(cancelUse, CancelUse.class);
            if (cancelUse.getReturnCode()!=0) {
                companion.failure(cancelUse.getReturnCode(),
                                  cancelUse.getErrorObject());
            }
            else {
                companion.success(cancelUse);
            }
        }
        catch (TimeoutCacheException e) {
            companion.timeout();
        }
        catch (InterruptedException e) {
            callbacks.CancelUseOfSpaceFailed("Unmark space as being used was "+
                                             "interrupted "+companion);
        }
        catch (CacheException e) {
            if (e.getRc()!=0) {
                companion.failure(e.getRc(),e);
            }
            else {
                // we could be here if SrmSpaceManager was disabled in configuration
                companion.success(null);
            }
        }
    }
}

