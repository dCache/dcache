//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________
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

import java.util.EnumSet;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.srm.RemoveFileCallback;

import static org.dcache.namespace.FileType.LINK;
import static org.dcache.namespace.FileType.REGULAR;

public class RemoveFileCompanion
    extends AbstractMessageCallback<PnfsDeleteEntryMessage>
{
    private static Logger _log =
        LoggerFactory.getLogger(RemoveFileCompanion.class);

    private final static CellPath BILLING_PATH = new CellPath("billing");

    private final Subject _subject;
    private final RemoveFileCallback _callback;
    private final String _path;
    private final CellEndpoint _endpoint;

    private RemoveFileCompanion(Subject subject,
                                String path,
                                RemoveFileCallback callbacks,
                                CellEndpoint endpoint)
    {
        _subject = subject;
        _path = path;
        _callback = callbacks;
        _endpoint = endpoint;
    }

    public static void removeFile(Subject subject,
                                  String path,
                                  RemoveFileCallback callbacks,
                                  CellStub pnfsStub,
                                  CellEndpoint endpoint)
    {
        RemoveFileCompanion companion =
            new RemoveFileCompanion(subject, path, callbacks, endpoint);
        PnfsDeleteEntryMessage message =
            new PnfsDeleteEntryMessage(path, EnumSet.of(LINK, REGULAR));
        message.setSubject(subject);
        CellStub.addCallback(pnfsStub.send(message), companion, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void success(PnfsDeleteEntryMessage message)
    {
        sendRemoveInfoToBilling(message.getPnfsId());
        _callback.success();
    }

    @Override
    public void failure(int rc, Object error)
    {
        switch (rc) {
        case CacheException.FILE_NOT_FOUND:
            _callback.notFound("No such file");
            break;

        case CacheException.NOT_FILE:
            _callback.notFound("Not a file");
            break;

        case CacheException.TIMEOUT:
            timeout();
            break;

        case CacheException.PERMISSION_DENIED:
            _callback.permissionDenied();
            break;

        default:
            _callback.failure(String.format("Deletion failed [rc=%d,msg=%s]",
                    rc, error));
            break;
        }
    }

    public void noroute()
    {
        /* No route and timeout are both transient errors and to the client it doesn't
         * matter whether we fail because of one or the other condition.
         */
        _callback.timeout();
    }

    public void timeout()
    {
        _callback.timeout();
    }

    private void sendRemoveInfoToBilling(PnfsId pnfsid)
    {
        try {
            CellInfo info = _endpoint.getCellInfo();
            DoorRequestInfoMessage msg =
                new DoorRequestInfoMessage(info.getCellName() + "@" +
                                           info.getDomainName(), "remove");
            msg.setSubject(_subject);
            msg.setPath(_path);
            msg.setPnfsId(pnfsid);

            _endpoint.sendMessage(new CellMessage(BILLING_PATH, msg));
        } catch (NoRouteToCellException e) {
            _log.error("Failed to send remove message to billing: " +
                       e.getMessage());
        }
    }
}

