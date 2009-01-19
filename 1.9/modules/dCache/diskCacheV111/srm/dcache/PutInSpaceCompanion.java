// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.8  2006/09/25 20:30:58  timur
// modify srm companions and srm cell to use ThreadManager
//
// Revision 1.7  2005/10/07 22:59:46  timur
// work towards v2
//
// Revision 1.6  2005/03/11 21:17:28  timur
// making srm compatible with cern tools again
//
// Revision 1.5  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.4  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.3.2.3  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//
// Revision 1.3.2.2  2004/05/21 21:46:47  timur
//  refactored interface, work on srm get implementation pining part
//
// Revision 1.3.2.1  2004/05/18 21:40:30  timur
// incorporation of the new scheduler into srm, repackaging of all the srm classes
//
// Revision 1.3  2004/03/01 03:05:58  timur
// fixed directory creation code, started implementation of advisory delete
//
// Revision 1.2  2003/11/06 17:44:35  cvs
// timur: working on space reservation
//
// Revision 1.1  2003/10/31 23:03:08  cvs
// timur: working on prepateToPut function of srm v2_1
//
// Revision 1.6  2003/10/02 18:50:28  cvs
// timur: added cvs version and log in comments
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

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DCapProtocolInfo;

import java.net.InetAddress;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.srm.PrepareToPutInSpaceCallbacks;
import org.dcache.srm.FileMetaData;

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
public class PutInSpaceCompanion implements CellMessageAnswerable {
    private AuthorizationRecord user;
    private dmg.cells.nucleus.CellAdapter cell;
    private PrepareToPutInSpaceCallbacks callbacks;
    private CellMessage request = null;
    private String path;
    private long size;
    private PnfsId pnfsId;
    private StorageInfo storageInfo;
    boolean directory_info;
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(" PutCompanion : "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.esay(" PutCompanion : "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(cell!=null) {
            cell.esay(" PutCompanion exception : ");
            cell.esay(t);
        }
    }
    /** Creates a new instance of StageAndPinCompanion */
    
    private PutInSpaceCompanion(AuthorizationRecord user,String path,long size, PrepareToPutInSpaceCallbacks callbacks, CellAdapter cell) {
        this.user = user;
        this.path = path;
        this.size = size;
        this.cell = cell;
        this.callbacks = callbacks;
        say(" constructor path = "+path);
    }
    
    public void answerArrived( final CellMessage req , final CellMessage answer ) {
        say("answerArrived");
        diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessage(req,answer);
            }
        });
    }
    
    private void processMessage( CellMessage req , CellMessage answer ) {
        say("answerArrived");
        request = req;
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if( message instanceof PnfsGetStorageInfoMessage ) {
                if(!directory_info) {
                    
                    if(message.getReturnCode() == 0) {
                        esay("GetStorageInfoFailed: file exists,"+
                        " cannot write ");
                        PnfsGetStorageInfoMessage storage_info_msg =
                        (PnfsGetStorageInfoMessage)message;
                        StorageInfo storageinfo=  storage_info_msg.getStorageInfo();
                        diskCacheV111.util.FileMetaData fmd = storage_info_msg.getMetaData();
                        //say("storage_info_msg = "+storage_info_msg);
                        //say("storageinfo = "+storageinfo);
                        //say("file meta data = "+fmd);
                        callbacks.GetStorageInfoFailed("GetStorageInfoFailed : file exists,"+
                        " cannot write ");
                        return ;
                    }
                    say("PutCompanion PnfsGetStorageInfoMessage for file failed"+
                    " file does not exist");
                    say("PutCompanion PnfsGetStorageInfoMessage now expect info for directory");
                    // next time we will go into different path
                    directory_info = true;
                    
                    //
                    //this is how we get the directory containing this path
                    //
                    FsPath directory_path = new FsPath(path);
                    // go one level up
                    directory_path.add("..");
                    String directory = directory_path.toString();
                    PnfsGetStorageInfoMessage storageInfoMsg =
                    new PnfsGetStorageInfoMessage() ;
                    storageInfoMsg.setPnfsPath( directory ) ;
                    
                    try {
                        cell.sendMessage( new CellMessage(
                        new CellPath("PnfsManager") ,
                        storageInfoMsg ) ,
                        true , true ,
                        this ,
                        1*24*60*60*1000) ;
                    }
                    catch(Exception ee ) {
                        esay(ee);
                        callbacks.GetStorageInfoFailed(ee.toString());
                    }
                    return;
                    
                }
                else {
                    say("PutCompanion PnfsGetStorageInfoMessage for parent "+
                    "directory arrived");
                    if(message.getReturnCode() != 0) {
                        esay("GetStorageInfoFailed message.getReturnCode () != 0");
                        callbacks.GetStorageInfoFailed("GetStorageInfoFailed PnfsGetStorageInfoMessage.getReturnCode () != 0 => parrent directory does not exist");
                        return ;
                    }
                    say(" got storage info");
                    PnfsGetStorageInfoMessage storage_info_msg =
                    (PnfsGetStorageInfoMessage)message;
                    say("storage_info_msg = "+storage_info_msg );
                    storageInfo = storage_info_msg.getStorageInfo() ;
                    say("storageInfo = "+storageInfo );
                    //long size = storageInfo.getFileSize ();
                    diskCacheV111.util.FileMetaData fmd =
                    storage_info_msg.getMetaData();
                    diskCacheV111.util.FileMetaData.Permissions perms =
                    fmd.getUserPermissions();
                    int permissions = (perms.canRead()    ? 4 : 0) |
                    (perms.canWrite()   ? 2 : 0) |
                    (perms.canExecute() ? 1 : 0) ;
                    permissions <<=3;
                    perms = fmd.getGroupPermissions();
                    permissions |=    (perms.canRead()    ? 4 : 0) |
                    (perms.canWrite()   ? 2 : 0) |
                    (perms.canExecute() ? 1 : 0) ;
                    permissions <<=3;
                    perms = fmd.getWorldPermissions();
                    permissions |=    (perms.canRead()    ? 4 : 0) |
                    (perms.canWrite()   ? 2 : 0) |
                    (perms.canExecute() ? 1 : 0) ;
                    pnfsId = storage_info_msg.getPnfsId();
                    say("calling callbacks.ParentStorageInfoArrived() ");
                    String fileId = pnfsId.toString();
                    FileMetaData srm_fmd = Storage.getFileMetaData(user,path,pnfsId,storageInfo,fmd);
                    if(Storage._canWrite(user,null,null,fileId,srm_fmd,false)) {
                        callbacks.StorageInfoArrived(fileId);
                    }
                    else {
                        callbacks.GetStorageInfoFailed("user has no permissions to write the file");
                    }
                    return;
                }
            }
        }
        else {
            esay(this.toString()+" got unknown object "+
            " : "+o);
            callbacks.Error(this.toString()+" got unknown object "+
            " : "+o) ;
        }
    }
    
    public void exceptionArrived( CellMessage request , Exception exception ) {
        esay("exceptionArrived "+exception+" for request "+request);
        callbacks.Exception(exception);
    }
    public void answerTimedOut( CellMessage request ) {
        esay("answerTimedOut for request "+request);
        callbacks.Timeout();
    }
    public String toString() {
        
        return this.getClass().getName()+
        (pnfsId == null?path:pnfsId.toString());
    }
    
    public static void PrepareToPutFile(
    AuthorizationRecord user,
    String path,
    long size,
    PrepareToPutInSpaceCallbacks callbacks,
    CellAdapter cell) {
        if(cell == null) {
            throw new IllegalArgumentException(" cell is null!!!");
        }
        cell.say(" PutCompanion.PrepareToPutFile("+path+")");
        if(callbacks == null) {
            throw new IllegalArgumentException(" callbacks is null!!!");
        }
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(" FileRequest does not specify path!!!");
        }
        
        PnfsGetStorageInfoMessage storageInfoMsg =
        new PnfsGetStorageInfoMessage() ;
        storageInfoMsg.setPnfsPath( pnfsPath ) ;
        PutInSpaceCompanion companion = new PutInSpaceCompanion(user,path,
        size,
        callbacks,
        cell);
        
        cell.say("sending PnfsGetStorageInfoMessage :"+storageInfoMsg);
        
        try {
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            storageInfoMsg ) ,
            true , true ,
            companion ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            callbacks.GetStorageInfoFailed("can not contact pnfs manger: "+ee.toString());
        }
    }
    
}

