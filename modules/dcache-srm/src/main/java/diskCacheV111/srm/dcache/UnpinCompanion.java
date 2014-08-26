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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;

import static diskCacheV111.util.CacheException.TIMEOUT;

public class UnpinCompanion
    extends AbstractMessageCallback<PinManagerUnpinMessage>
{
    private static final Logger _log =
        LoggerFactory.getLogger(UnpinCompanion.class);

    private final PnfsId pnfsId;
    private final SettableFuture<String> future = SettableFuture.create();

    /** Creates a new instance of StageAndPinCompanion */
    private UnpinCompanion(PnfsId pnfsId)
    {
        this.pnfsId = pnfsId;
    }

    @Override
    public void success(PinManagerUnpinMessage message)
    {
        future.set(String.valueOf(message.getPinId()));
    }

    @Override
    public void failure(int rc, Object error)
    {
        switch (rc) {
        case TIMEOUT:
            _log.error(error.toString());
            future.setException(new SRMInternalErrorException("Unpinning failed due to internal timeout."));
            break;

        default:
            _log.error("Unpinning failed for {} [rc={},msg={}]", pnfsId, rc, error);
            String reason =
                String.format("Failed to unpin file [rc=%d,msg=%s]", rc, error);
            future.setException(new SRMException(reason));
            break;
        }
    }

    public String toString()
    {
        return getClass().getName() + " " + pnfsId;
    }

    public static ListenableFuture<String> unpinFile(Subject subject,
                                                     PnfsId pnfsId,
                                                     long pinId,
                                                     CellStub pinManagerStub)
    {
        _log.info("UnpinCompanion.unpinFile({})", pnfsId);
        UnpinCompanion companion = new UnpinCompanion(pnfsId);
        PinManagerUnpinMessage msg =
            new PinManagerUnpinMessage(pnfsId);
        msg.setPinId(pinId);
        msg.setSubject(subject);
        CellStub.addCallback(pinManagerStub.send(msg), companion, MoreExecutors.sameThreadExecutor());
        return companion.future;
    }

    public static ListenableFuture<String> unpinFileBySrmRequestId(Subject subject,
                                                                   PnfsId pnfsId,
                                                                   String requestToken,
                                                                   CellStub pinManagerStub)
    {
        _log.info("UnpinCompanion.unpinFile({})", pnfsId);
        UnpinCompanion companion = new UnpinCompanion(pnfsId);
        PinManagerUnpinMessage msg =
            new PinManagerUnpinMessage(pnfsId);
        msg.setRequestId(requestToken);
        msg.setSubject(subject);
        CellStub.addCallback(pinManagerStub.send(msg), companion, MoreExecutors.sameThreadExecutor());
        return companion.future;
    }

    public static ListenableFuture<String> unpinFile(Subject subject,
                                                     PnfsId pnfsId,
                                                     CellStub pinManagerStub)
    {
        _log.info("UnpinCompanion.unpinFile({}", pnfsId);
        UnpinCompanion companion = new UnpinCompanion(pnfsId);
        PinManagerUnpinMessage msg =
            new PinManagerUnpinMessage(pnfsId);
        msg.setSubject(subject);
        CellStub.addCallback(pinManagerStub.send(msg), companion, MoreExecutors.sameThreadExecutor());
        return companion.future;
    }
}

