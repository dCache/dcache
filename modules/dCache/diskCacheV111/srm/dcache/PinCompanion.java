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

import java.net.InetAddress;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PinManagerPinMessage;

import org.dcache.auth.AuthorizationRecord;
import org.dcache.srm.PinCallbacks;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.cells.ThreadManagerMessageCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static diskCacheV111.util.CacheException.*;

public class PinCompanion
    implements MessageCallback<PinManagerPinMessage>
{
    private final static Logger _log = LoggerFactory.getLogger(PinCompanion.class);

    private final PinCallbacks callbacks;
    private final String fileId;

    private PinCompanion(String fileId, PinCallbacks callbacks)
    {
        this.fileId = fileId;
        this.callbacks = callbacks;
    }

    public void success(PinManagerPinMessage message)
    {
        callbacks.Pinned(message.getPinRequestId());
    }

    public void failure(int rc, Object error)
    {
        switch (rc) {
        case TIMEOUT:
            _log.error(error.toString());
            callbacks.Timeout();
            break;

        default:
            _log.error(String.format("Pinning failed for %s [rc=%d,msg=%s]", fileId, rc, error));

            String reason =
                String.format("Failed to pin file [rc=%d,msg=%s]",
                              rc, error);
            callbacks.PinningFailed(reason);
            break;
        }
    }

    public void noroute()
    {
        failure(TIMEOUT, "No route to PinManager");
    }

    public void timeout()
    {
        failure(TIMEOUT, "Pinning timed out");
    }

    public String toString()
    {
        return getClass().getName() + "[" + fileId + "]";
    }

    public static void pinFile(AuthorizationRecord user,
                               String fileId,
                               String clientHost,
                               PinCallbacks callbacks,
                               DcacheFileMetaData dfmd,
                               long pinLifetime,
                               long requestId,
                               CellStub pinManagerStub)
    {
        _log.debug("PinCompanion.pinFile(" + fileId + ")");

        PinCompanion companion =
            new PinCompanion(fileId, callbacks);
        PinManagerPinMessage pinRequest =
            new PinManagerPinMessage(new PnfsId(fileId),
                                     clientHost,
                                     pinLifetime,
                                     requestId);
        pinRequest.setAuthorizationRecord(user);
        pinRequest.setStorageInfo(dfmd.getFileAttributes().getStorageInfo());

        pinManagerStub.send(pinRequest, PinManagerPinMessage.class,
                            new ThreadManagerMessageCallback(companion));
    }
}

