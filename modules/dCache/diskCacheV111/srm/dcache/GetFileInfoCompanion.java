// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.8  2006/09/25 20:30:58  timur
// modify srm companions and srm cell to use ThreadManager
//
// Revision 1.7  2005/10/07 22:59:46  timur
// work towards v2
//
// Revision 1.6  2005/05/04 20:22:20  timur
//  better error if file not found
//
// Revision 1.5  2005/04/28 13:13:03  timur
// make more meaningfull error messade for prepare to get error
//
// Revision 1.4  2005/03/11 21:17:28  timur
// making srm compatible with cern tools again
//
// Revision 1.3  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.4  2004/06/15 22:15:42  timur
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

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;

import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.cells.ThreadManagerMessageCallback;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.Subjects;
import org.dcache.srm.GetFileInfoCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static diskCacheV111.util.CacheException.*;

public class GetFileInfoCompanion
    implements MessageCallback<PnfsGetStorageInfoMessage>
{
    private final GetFileInfoCallbacks callbacks;
    private final String path;
    private final AuthorizationRecord user;

    private GetFileInfoCompanion(AuthorizationRecord user,
                                 String path,
                                 GetFileInfoCallbacks callbacks)
    {
        this.user = user;
        this.path = path;
        this.callbacks = callbacks;
    }

    public void success(PnfsGetStorageInfoMessage message)
    {
        StorageInfo storageInfo = message.getStorageInfo() ;
        diskCacheV111.util.FileMetaData fmd = message.getMetaData();
        PnfsId pnfsId = message.getPnfsId();
        FileMetaData srm_fmd =
            Storage.getFileMetaData(user, path, pnfsId, storageInfo, fmd);

        callbacks.StorageInfoArrived(srm_fmd.fileId, srm_fmd);
    }

    public void failure(int rc, Object error)
    {
        switch (rc) {
        case FILE_NOT_FOUND:
            callbacks.FileNotFound("No such file");
            break;

        case TIMEOUT:
            callbacks.Timeout();
            break;

        case PERMISSION_DENIED:
            callbacks.Error("Permission denied");
            break;

        default:
            String reason =
                String.format("Failed to retrieve file information [rc=%d,msg=%s]",
                              rc, error);
            callbacks.GetStorageInfoFailed(reason);
            break;
        }
    }

    public void noroute()
    {
        failure(TIMEOUT, "Internal communication failure");
    }

    public void timeout()
    {
        failure(TIMEOUT, "Timeout");
    }

    public String toString()
    {
        return getClass().getName() + "[" + path + "]";
    }

    public static void getFileInfo(AuthorizationRecord user,
                                   String path,
                                   GetFileInfoCallbacks callbacks,
                                   CellStub pnfsStub)
    {
        GetFileInfoCompanion companion =
            new GetFileInfoCompanion(user, path, callbacks);
        PnfsGetStorageInfoMessage msg =
            new PnfsGetStorageInfoMessage();
        msg.setPnfsPath(path);
        msg.setSubject(Subjects.getSubject(user));
        pnfsStub.send(msg, PnfsGetStorageInfoMessage.class,
                      new ThreadManagerMessageCallback(companion));
    }

}

