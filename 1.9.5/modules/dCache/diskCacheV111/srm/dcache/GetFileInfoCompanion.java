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

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
//import org.dcache.srm.util.PnfsFileId;

import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
//import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DCapProtocolInfo;

//import org.dcache.srm.util.FileRequest;
//import org.dcache.srm.security.AuthorizationRecord;
import java.net.InetAddress;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.srm.GetFileInfoCallbacks;
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
public class GetFileInfoCompanion implements CellMessageAnswerable {
    
    private static final int INITIAL_STATE=0;
    private static final int SENT_PNFS_GET_STORAGE_INFO_MSG = 1;
    private static final int RECEIVED_PNFS_GET_STORAGE_INFO_MSG = 2;
    private volatile int  state = INITIAL_STATE;
    private dmg.cells.nucleus.CellAdapter cell;
    private GetFileInfoCallbacks callbacks;
    private CellMessage request = null;
    private String path;
    private PnfsId pnfsId;
    private String host;
    private StorageInfo storageInfo;
    private AuthorizationRecord user;
    
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(" GetFileInfoCompanion : "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.esay(" GetFileInfoCompanion : "+words_of_despare);
        }
    }
    /** Creates a new instance of StageAndPinCompanion */
    
    private GetFileInfoCompanion(AuthorizationRecord user,
    String path,
    String host,
    GetFileInfoCallbacks callbacks,
    CellAdapter cell) {
        this.user = user;
        this.path = path;
        this.host   = host ;
        this.cell = cell;
        this.callbacks = callbacks;
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
        say("answerArrived");
        request = req;
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if( message instanceof PnfsGetStorageInfoMessage ) {
                if(state != SENT_PNFS_GET_STORAGE_INFO_MSG) {
                    esay("received PnfsGetStorageInfoMessage, state ="+state);
                    return;
                }
                state = RECEIVED_PNFS_GET_STORAGE_INFO_MSG;
                say(" message is PnfsGetStorageInfoMessage");
                
                PnfsGetStorageInfoMessage storage_info_msg =
                (PnfsGetStorageInfoMessage)message;
                pnfsGetStorageInfoMessageArrived(storage_info_msg);
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
    
    private void pnfsGetStorageInfoMessageArrived( PnfsGetStorageInfoMessage storage_info_msg) {
        if(storage_info_msg.getReturnCode() != 0) {
            String error = "file not found"; 
            Object errorObj = storage_info_msg.getErrorObject();
            if(errorObj != null) {
                if(errorObj instanceof Exception) {
                    
                    error += " : "+((Exception)errorObj).getMessage();
                }
                else if(errorObj instanceof String){
                    error += " : "+(String) errorObj;
                } else {
                    error += " : "+errorObj.toString();
                }
            }
            esay(error);
            callbacks.FileNotFound(error);
            return ;
        }
        say("storage_info_msg = "+storage_info_msg );
        storageInfo = storage_info_msg.getStorageInfo() ;
        say("storageInfo = "+storageInfo );
        diskCacheV111.util.FileMetaData fmd =
        storage_info_msg.getMetaData();
        pnfsId = storage_info_msg.getPnfsId();
        String fileId = pnfsId.toString();
        FileMetaData srm_fmd = Storage.getFileMetaData(user,path,pnfsId,storageInfo,fmd);
        if(!Storage._canRead(user,fileId,srm_fmd)) {
            callbacks.Error("user "+user+"cannot read "+fileId);
            return;
        }
        
        say("calling callbacks.StorageInfoArrived() ");
        callbacks.StorageInfoArrived(fileId, srm_fmd);
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
    
    public static void getFileInfo(
    AuthorizationRecord user,
    String path,
    GetFileInfoCallbacks callbacks,
    CellAdapter cell) {
        cell.say("StageAndPinCompanion.StageAndPinFile("+path+")");
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(" FileRequest does not specify path!!!");
        }
        
        PnfsGetStorageInfoMessage storageInfoMsg =
        new PnfsGetStorageInfoMessage() ;
        storageInfoMsg.setPnfsPath( pnfsPath ) ;
        String thishost;
        try {
            thishost  = InetAddress.getLocalHost().getHostName();
        }
        catch(java.net.UnknownHostException uhe) {
            //process error
            thishost = "localhost";
            
        }
        
        GetFileInfoCompanion companion = new GetFileInfoCompanion(user,path,
        thishost,
        callbacks,
        cell);
        
        companion.state = SENT_PNFS_GET_STORAGE_INFO_MSG;
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

