// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.7  2006/07/04 22:23:37  timur
// Use Credential Id to reffer to the remote credential in delegation step, reformated some classes
//
// Revision 1.6  2005/03/11 21:17:28  timur
// making srm compatible with cern tools again
//
// Revision 1.5  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.4  2004/11/08 23:02:40  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.3  2004/08/26 21:22:30  timur
// scheduler bug (not setting job value to null) in a loop serching for the next job to execute
//
// Revision 1.2  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//

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

import diskCacheV111.util.PnfsId;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.ThreadManagerMessageCallback;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.srm.UnpinCallbacks;

import static diskCacheV111.util.CacheException.TIMEOUT;

/**
 *
 * @author  timur
 */
public class UnpinCompanion
    extends AbstractMessageCallback<PinManagerUnpinMessage>
{
    private static final Logger _log =
        LoggerFactory.getLogger(UnpinCompanion.class);

    private UnpinCallbacks callbacks;
    private PnfsId pnfsId;

    /** Creates a new instance of StageAndPinCompanion */
    private UnpinCompanion(PnfsId pnfsId, UnpinCallbacks callbacks)
    {
        this.pnfsId = pnfsId;
        this.callbacks = callbacks;
    }

    @Override
    public void success(PinManagerUnpinMessage message)
    {
        callbacks.Unpinned(String.valueOf(message.getPinId()));
    }

    @Override
    public void failure(int rc, Object error)
    {
        switch (rc) {
        case TIMEOUT:
            _log.error(error.toString());
            callbacks.Timeout();
            break;

        default:
            _log.error("Unpinning failed for {} [rc={},msg={}]", pnfsId, rc, error);

            String reason =
                String.format("Failed to unpin file [rc=%d,msg=%s]",
                              rc, error);
            callbacks.UnpinningFailed(reason);
            break;
        }
    }

    public String toString()
    {
        return getClass().getName() + " " + pnfsId;
    }

    public static void unpinFile(Subject subject,
                                 PnfsId pnfsId,
                                 long pinId,
                                 UnpinCallbacks callbacks,
                                 CellStub pinManagerStub)
    {
        _log.info("UnpinCompanion.unpinFile({})", pnfsId);
        UnpinCompanion companion = new UnpinCompanion(pnfsId, callbacks);
        PinManagerUnpinMessage msg =
            new PinManagerUnpinMessage(pnfsId);
        msg.setPinId(pinId);
        msg.setSubject(subject);
        pinManagerStub.send(msg, PinManagerUnpinMessage.class,
                            new ThreadManagerMessageCallback(companion));
    }

    public static void unpinFileBySrmRequestId(Subject subject,
                                               PnfsId pnfsId,
                                               String requestToken,
                                               UnpinCallbacks callbacks,
                                               CellStub pinManagerStub)
    {
        _log.info("UnpinCompanion.unpinFile({})", pnfsId);
        UnpinCompanion companion = new UnpinCompanion(pnfsId, callbacks);
        PinManagerUnpinMessage msg =
            new PinManagerUnpinMessage(pnfsId);
        msg.setRequestId(requestToken);
        msg.setSubject(subject);
        pinManagerStub.send(msg, PinManagerUnpinMessage.class,
                            new ThreadManagerMessageCallback(companion));
    }

    public static void unpinFile(Subject subject,
                                 PnfsId pnfsId,
                                 UnpinCallbacks callbacks,
                                 CellStub pinManagerStub)
    {
        _log.info("UnpinCompanion.unpinFile({}", pnfsId);
        UnpinCompanion companion = new UnpinCompanion(pnfsId, callbacks);
        PinManagerUnpinMessage msg =
            new PinManagerUnpinMessage(pnfsId);
        msg.setSubject(subject);
        pinManagerStub.send(msg, PinManagerUnpinMessage.class,
                            new ThreadManagerMessageCallback(companion));
    }
}

