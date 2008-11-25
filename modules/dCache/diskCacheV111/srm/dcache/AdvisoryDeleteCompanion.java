// $Id$

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
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import org.dcache.srm.AdvisoryDeleteCallbacks;
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
public class AdvisoryDeleteCompanion implements CellMessageAnswerable {
    private static final int INITIAL_STATE=0;
    private static final int WAITING_FOR_FILE_INFO_MESSAGE=1;
    private static final int RESEIVED_FILE_INFO_MESSAGE=2;
    private static final int WAITING_FOR_PNFS_DELETE_MESSAGE=5;
    private static final int RESEIVED_PNFS_DELETE_MESSAGE=6;
    //this state is assumed when we sent multiple "change  persistancy"
    // messages to pools and a message that sets "d" flag in pnfs
    // and we are waiting for replies for all of these messages
    private static final int WAITING_FOR_PNFS_AND_POOL_REPLIES_MESSAGES=7;
    private static final int FINAL_STATE=8;
    
    private volatile int state = INITIAL_STATE;
    private dmg.cells.nucleus.CellAdapter cell;
    private AdvisoryDeleteCallbacks callbacks;
    private CellMessage request = null;
    private String path;
    private PnfsId pnfsId;
    private StorageInfo storageInfo;
    private String      poolName = null ;
    private DCacheUser user;
    private boolean advisoryDeleteEnabled;
    
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(" AdvisoryDeleteCompanion ("+path+
            (pnfsId == null?"":","+pnfsId.toString())+
            ") : "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.say(" AdvisoryDeleteCompanion ("+path+
            (pnfsId == null?"":","+pnfsId.toString())+
            ") : "+words_of_despare);
        }
    }
    
    private void esay(Throwable t) {
        if(cell!=null) {
            cell.esay(" AdvisoryDeleteCompanion exception : ");
            cell.esay(t);
        }
    }
    
    /** Creates a new instance of StageAndPinCompanion */
    
    /** Creates a new instance of StageAndPinCompanion */
    
    private AdvisoryDeleteCompanion(DCacheUser user, String path,  
        AdvisoryDeleteCallbacks callbacks, 
        CellAdapter cell,
        boolean advisoryDeleteEnabled) {
        this.user = user;
        this.path = path;
        this.cell = cell;
        this.callbacks = callbacks;
        this.advisoryDeleteEnabled = advisoryDeleteEnabled;
        say(" constructor ");
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
        if(state == FINAL_STATE) {
            say("answerArrived("+req+","+answer+"), state is final, ignore all messages");
            return;
        }
        say("answerArrived("+req+","+answer+"), state ="+state);
        
        request = req;
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if( message instanceof PnfsGetStorageInfoMessage ) {
                PnfsGetStorageInfoMessage storage_info_msg =
                (PnfsGetStorageInfoMessage)message;
                if(state == WAITING_FOR_FILE_INFO_MESSAGE) {
                    state = RESEIVED_FILE_INFO_MESSAGE;
                    fileInfoArrived(storage_info_msg);
                    return;
                }
                esay(this.toString()+" got unexpected PnfsGetStorageInfoMessage "+
                " : "+storage_info_msg+" ; Ignoring");
            }
            else if( message instanceof PnfsDeleteEntryMessage ) {
                
                PnfsDeleteEntryMessage delete_reply =
                (PnfsDeleteEntryMessage) message;
                if(state == WAITING_FOR_PNFS_DELETE_MESSAGE) {
                    state = RESEIVED_PNFS_DELETE_MESSAGE;
                    if(delete_reply.getReturnCode() == 0) {
                        callbacks.AdvisoryDeleteSuccesseded();
                    }
                    else {
                        callbacks.Error("Delete failed: "+delete_reply);
                    }
                    return;
                }
                esay(this.toString()+" got unexpected PnfsDeleteEntryMessage "+
                " : "+delete_reply+" ; Ignoring");
            }
            else {
                esay(this.toString()+" got unknown message "+
                " : "+message.getErrorObject());
                
                callbacks.Error( this.toString()+" got unknown message "+
                " : "+message.getErrorObject()) ;
            }
        }
        else {
            esay(this.toString()+" got unknown object "+
            " : "+o);
            callbacks.Error(this.toString()+" got unknown object "+
            " : "+o) ;
        }
    }
    
    public void deleteEntry() {

        try {
            PnfsDeleteEntryMessage delete_request =
            new PnfsDeleteEntryMessage(path);
            delete_request.setReplyRequired(true);
            try {
                state = WAITING_FOR_PNFS_DELETE_MESSAGE;
                cell.sendMessage( new CellMessage(
                new CellPath("PnfsManager") ,
                delete_request ) ,
                true , true ,
                this ,
                1*24*60*60*1000) ;
            }
            catch(Exception ee ) {
                state = FINAL_STATE;
                esay(ee);
                callbacks.Exception(ee);
            }
          //  return;
        //}
        
        }
        catch(Exception ee ) {
            state = FINAL_STATE;
            esay(ee);
            callbacks.Exception(ee);
        }
    }
    
   
    
    public void fileInfoArrived(
    PnfsGetStorageInfoMessage storage_info_msg) {
        if(storage_info_msg.getReturnCode() != 0) {
            callbacks.Error("file does not exist, cannot delete");
            return;
        }
        
        say("storage_info_msg = "+storage_info_msg );
        storageInfo = storage_info_msg.getStorageInfo() ;
        say("storageInfo = "+storageInfo );
        long size = storageInfo.getFileSize();
        diskCacheV111.util.FileMetaData fmd =
        storage_info_msg.getMetaData();
        if(fmd.isDirectory()) {
            callbacks.Error("file is a directory, can not delete");
            return;
        }
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
        String fileId = pnfsId.toString();
        FileMetaData srm_fmd = Storage.getFileMetaData(user,path,pnfsId,storageInfo,fmd);
        if(!Storage._canDelete(user,fileId,srm_fmd)) {
            callbacks.Error("user "+user+" has no permission to delete "+pnfsId);
            return;
        }
        if(!advisoryDeleteEnabled) {
            // we just checked the permissions and we are now going to return 
            // without doing anything
            callbacks.AdvisoryDeleteSuccesseded();
        }
        deleteEntry();
        return;
        
    }
    
    public void exceptionArrived( CellMessage request , Exception exception ) {
        state = FINAL_STATE;
        esay("exceptionArrived "+exception+" for request "+request);
        callbacks.Exception(exception);
    }
    public void answerTimedOut( CellMessage request ) {
        esay("answerTimedOut for request "+request);
        callbacks.Timeout();
    }
    public String toString() {
        
        if( poolName != null ) {
            return this.getClass().getName()+
            (pnfsId == null?path:pnfsId.toString())+
            " Staged at : "+poolName+" ;";
        }else {
            return this.getClass().getName()+
            (pnfsId == null?path:pnfsId.toString());
        }
    }
    
    public static void advisoryDelete(
    DCacheUser user,
    String path,
    AdvisoryDeleteCallbacks callbacks,
    CellAdapter cell,
    boolean advisoryDeleteEnabled) {
        cell.say("AdvisoryDeleteCompanion.advisoryDelete("+path+")");
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        
        
        PnfsGetStorageInfoMessage storageInfoMsg =
        new PnfsGetStorageInfoMessage() ;
        storageInfoMsg.setPnfsPath( pnfsPath ) ;
        
        AdvisoryDeleteCompanion companion = new AdvisoryDeleteCompanion(user,path,
        callbacks,
        cell,
        advisoryDeleteEnabled);
        
        companion.state = WAITING_FOR_FILE_INFO_MESSAGE;
        
        try {
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            storageInfoMsg ) ,
            true , true ,
            companion ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            cell.esay(ee);
            callbacks.Exception(ee);
        }
    }
    
}

// $Log: not supported by cvs2svn $
// Revision 1.9  2007/05/24 13:51:09  tigran
// merge of 1.7.1 and the head
//
// Revision 1.8  2006/09/25 20:30:58  timur
// modify srm companions and srm cell to use ThreadManager
//
// Revision 1.7.8.2  2007/09/26 04:07:06  timur
// make advisory delete only remove the pnfs entry without changing the files to cached
//
// Revision 1.7.8.1  2006/09/20 22:05:28  timur
// modify most srm companions to use ThreadManager
//
// Revision 1.7  2005/10/07 22:59:46  timur
// work towards v2
//
// Revision 1.6  2005/06/02 06:16:58  timur
// changes to advisory delete behavior
//
// Revision 1.5  2005/03/11 21:17:27  timur
// making srm compatible with cern tools again
//
// Revision 1.4  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.3  2004/12/16 21:08:56  timur
// modified database to prevent the slow down of srm caused by increase of number of records
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.3  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
//

