// $Id: PutCompanion.java,v 1.19 2007-09-13 19:48:02 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.18  2007/03/28 02:58:23  timur
// Support overwrite, less logging
//
// Revision 1.17  2007/03/10 00:13:21  timur
// started work on adding support for optional overwrite
//
// Revision 1.16  2007/02/20 01:46:35  timur
// more changes to report status according to the spec
//
// Revision 1.15  2007/02/17 05:48:48  timur
// detect file name that is too long for pnfs in putCompanion
//
// Revision 1.14  2006/11/17 21:59:30  litvinse
// put 10 minite timeouts for PNFS messages
//
// Revision 1.13  2006/11/17 21:17:09  litvinse
// create missing directory subpath with permissions and ownership based
// on its parent
//
// Revision 1.12  2006/09/25 20:30:58  timur
// modify srm companions and srm cell to use ThreadManager
//
// Revision 1.11  2005/10/07 22:59:46  timur
// work towards v2
//
// Revision 1.10  2005/05/12 21:45:00  timur
// better error report
//
// Revision 1.9  2005/04/07 21:13:53  timur
// check that the file metadata in the PnfsGetFileMetaDataMessage is not null, even when the return code is 0
//
// Revision 1.8  2005/03/11 21:17:28  timur
// making srm compatible with cern tools again
//
// Revision 1.7  2005/03/07 22:59:11  timur
// use pnfs get metadata instead of get storageInfo on directories
//
// Revision 1.6  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.5  2004/10/20 21:29:46  timur
// corrected one log message format
//
// Revision 1.4  2004/09/21 16:55:04  timur
// fixed unregesering the directory creators
//
// Revision 1.3  2004/09/21 03:24:02  timur
// fixed the bug, leaving the directory creation hanging
//
// Revision 1.2  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.3  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//
// Revision 1.1.2.2  2004/05/21 21:46:47  timur
//  refactored interface, work on srm get implementation pining part
//
// Revision 1.1.2.1  2004/05/18 21:40:30  timur
// incorporation of the new scheduler into srm, repackaging of all the srm classes
//
// Revision 1.11  2004/03/02 19:30:55  timur
// implemented advisory delete
//
// Revision 1.10  2004/03/01 03:05:58  timur
// fixed directory creation code, started implementation of advisory delete
//
// Revision 1.9  2004/02/24 21:37:39  timur
// added information provider, working on advisory delete
//
// Revision 1.8  2004/02/12 20:14:30  timur
// dirrectory creation code is modified
//
// Revision 1.7  2004/01/28 13:02:27  timur
// added support for dynamic directoy tree creation, when writing into unexisting , but legitimate path through srm
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
import org.dcache.srm.util.OneToManyMap;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.util.Constants;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DCapProtocolInfo;

import java.net.InetAddress;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
 * The code was added that performs the automatic creation of the directories.
 *
 */

public class PutCompanion implements CellMessageAnswerable {
    private static final int PNFS_MAX_FILE_NAME_LENGTH=199;
    public  static final long PNFS_TIMEOUT = 10*Constants.MINUTE;
    private static final int INITIAL_STATE=0;
    private static final int WAITING_FOR_FILE_INFO_MESSAGE=1;
    private static final int RESEIVED_FILE_INFO_MESSAGE=2;
    private static final int WAITING_FOR_DIRECTORY_INFO_MESSAGE=3;
    private static final int RECEIVED_DIRECTORY_INFO_MESSAGE=4;
    private static final int WAITING_FOR_CREATE_DIRECTORY_RESPONCE_MESSAGE=5;
    private static final int RECEIVED_CREATE_DIRECTORY_RESPONCE_MESSAGE=6;
    private static final int FINAL_STATE=8;
    private volatile int state=INITIAL_STATE;
    
    private dmg.cells.nucleus.CellAdapter cell;
    private PrepareToPutCallbacks callbacks;
    private CellMessage request = null;
    private String path;
    private boolean recursive_directory_creation;
    private java.util.List pathItems=null;
    private int current_dir_depth = -1;
    private DCacheUser user;
    private boolean overwrite;
    private String fileId;
    private FileMetaData fileFMD;
    
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(toString()+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.esay(toString()+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(cell!=null) {
            cell.esay(" PutCompanion exception : ");
            cell.esay(t);
        }
    }
    /** Creates a new instance of StageAndPinCompanion */
    
    private PutCompanion(DCacheUser user,
    String path,
    PrepareToPutCallbacks callbacks,
    CellAdapter cell,
    boolean recursive_directory_creation,
    boolean overwrite) {
        this.user =  user;
        this.path = path;
        this.cell = cell;
        this.callbacks = callbacks;
        this.recursive_directory_creation = recursive_directory_creation;
        this.overwrite = overwrite;
        pathItems = (new FsPath(path)).getPathItemsList();

        say(" constructor path = "+path+" overwrite="+overwrite);
    }
    
     public void answerArrived( final CellMessage req , final CellMessage answer ) {
        diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessage(req,answer);
            }
        });
    }
    
    private void processMessage( CellMessage req , CellMessage answer ) {
        say("answerArrived("+req+","+answer+")");
        request = req;
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            // note that PnfsCreateDirectoryMessage is a subclass
            // of PnfsGetStorageInfoMessage
            if( message instanceof PnfsCreateDirectoryMessage) {
                PnfsCreateDirectoryMessage create_directory_responce =
                (PnfsCreateDirectoryMessage)message;
                if( state == WAITING_FOR_CREATE_DIRECTORY_RESPONCE_MESSAGE) {
                    state = RECEIVED_CREATE_DIRECTORY_RESPONCE_MESSAGE;
                    directoryInfoArrived(create_directory_responce);
                    return;
                }
                esay(this.toString()+" got unexpected PnfsCreateDirectoryMessage "+
                " : "+create_directory_responce+" ; Ignoring");
            }
            else if( message instanceof PnfsGetStorageInfoMessage ) {
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
            else if( message instanceof PnfsGetFileMetaDataMessage ) {
                PnfsGetFileMetaDataMessage metadata_msg =
                (PnfsGetFileMetaDataMessage)message;
                if(state == WAITING_FOR_DIRECTORY_INFO_MESSAGE) {
                    state = RECEIVED_DIRECTORY_INFO_MESSAGE;
                    directoryInfoArrived(metadata_msg);
                    return;
                }
                esay(this.toString()+" got unexpected PnfsGetFileMetaDataMessage "+
                " : "+metadata_msg+" ; Ignoring");
            }
            
        }
        else {
            esay(this.toString()+" got unknown object "+
            " : "+o);
            callbacks.Error(this.toString()+" got unknown object "+
            " : "+o) ;
        }
    }
    
    public void fileInfoArrived(
    PnfsGetStorageInfoMessage storage_info_msg) {
        if(storage_info_msg.getReturnCode() == 0) {
            if(overwrite)  {
               diskCacheV111.util.FileMetaData fmd =
                storage_info_msg.getMetaData();
               PnfsId pnfsId = storage_info_msg.getPnfsId();
                fileId = pnfsId.toString();
                fileFMD = Storage.getFileMetaData(user,path,pnfsId,null,fmd,null);
            } else {
                esay("GetStorageInfoFailed: file exists,"+
                " cannot write ");
                StorageInfo storageinfo=  storage_info_msg.getStorageInfo();
                diskCacheV111.util.FileMetaData fmd = storage_info_msg.getMetaData();
                //say("storage_info_msg = "+storage_info_msg);
                //say("storageinfo = "+storageinfo);
                //say("file meta data = "+fmd);
                callbacks.DuplicationError(" file exists ");
                return ;
            }
        } else {
            
            String fileName = (String) pathItems.get(pathItems.size()-1);
            if(fileName.length() >PNFS_MAX_FILE_NAME_LENGTH) {
                callbacks.Error("File name is too long");
                return;
            }
            say("file does not exist, now get info for directory");
        }
        // next time we will go into different path
        
        //
        //this is how we get the directory containing this path
        //
        current_dir_depth = pathItems.size();
        askPnfsForParentInfo();
    }
    
    public void directoryInfoArrived(
    PnfsGetFileMetaDataMessage metadata_msg) {
        try{
        
        if(metadata_msg.getReturnCode() != 0) {
            if(state == RECEIVED_CREATE_DIRECTORY_RESPONCE_MESSAGE) {
                String error = "directory creation failed: "+getCurrentDirPath()+" reason: "+
                metadata_msg.getErrorObject();
                unregisterAndFailCreator(error);
                //  notify all waiting of error (on all levels)
                //   unregisterCreator(pnfsPath, this, message);
                callbacks.Error(error);
                return;
            }
            if(state == RECEIVED_DIRECTORY_INFO_MESSAGE) {
                if(recursive_directory_creation) {
                    askPnfsForParentInfo();
                    return;
                }
                String error = "GetStorageInfoFailed message.getReturnCode () != 0";
                unregisterAndFailCreator(error);
                esay(error);
                callbacks.GetStorageInfoFailed("GetStorageInfoFailed PnfsGetStorageInfoMessage.getReturnCode () != 0 => parrent directory does not exist");
                return ;
            }
        }
        unregisterCreator(metadata_msg);
        //StorageInfo storageInfo = storage_info_msg.getStorageInfo() ;
        //say("storageInfo = "+storageInfo );
        //long size = storageInfo.getFileSize ();
        
        diskCacheV111.util.FileMetaData dirFmd =
        metadata_msg.getMetaData();
        
        if(dirFmd == null) {
            String error ="internal error: file metadata is null for directory: "+metadata_msg.getPnfsPath();
                esay(error);
                unregisterAndFailCreator(error);
                callbacks.GetStorageInfoFailed(error);
                return;
        }
        
        if(!dirFmd.isDirectory()) {
            String error ="file "+metadata_msg.getPnfsPath()+
            " is not a directory";
                esay(error);
                unregisterAndFailCreator(error);
                callbacks.InvalidPathError(error);
                return;
        }
        
        say("file is a directory");
        PnfsId dirPnfsId = metadata_msg.getPnfsId();
        String dirFileId = dirPnfsId.toString();
        FileMetaData srm_dirFmd = Storage.getFileMetaData(user,path,dirPnfsId,null,dirFmd,null);
        if((pathItems.size() -1 ) >current_dir_depth) {
            
            if(Storage._canWrite(user,null,null,dirFileId,srm_dirFmd,overwrite)) {
                createNextDirectory(srm_dirFmd);
                return;
            }
            else {
                String error = "path does not exist and user has no permissions to create it";
                esay(error);
                unregisterAndFailCreator(error);
                callbacks.InvalidPathError(error);
                return;
            }
        }
        else {
            if(Storage._canWrite(user,fileId,fileFMD,dirFileId,srm_dirFmd,overwrite)) {
                callbacks.StorageInfoArrived(fileId,fileFMD,dirFileId,srm_dirFmd);
            }
            else {
                String error = "user has no permission to write into path "+getCurrentDirPath();
                say(error);
                callbacks.AuthorizationError(error);
            }
            return;
        }
        }
        catch(java.lang.RuntimeException re) {
            esay(re);
            throw re;
        }
    }
    
    /*private static PnfsFileId createFileId(PnfsId pnfsId,StorageInfo storageInfo,
        diskCacheV111.util.FileMetaData fmd)
    {
        diskCacheV111.util.FileMetaData.Permissions perms =
        fmd.getUserPermissions ();
        int permissions = (perms.canRead ()    ? 4 : 0) |
        (perms.canWrite ()   ? 2 : 0) |
        (perms.canExecute () ? 1 : 0) ;
        permissions <<=3;
        perms = fmd.getGroupPermissions ();
        permissions |=    (perms.canRead ()    ? 4 : 0) |
        (perms.canWrite ()   ? 2 : 0) |
        (perms.canExecute () ? 1 : 0) ;
        permissions <<=3;
        perms = fmd.getWorldPermissions ();
        permissions |=    (perms.canRead ()    ? 4 : 0) |
        (perms.canWrite ()   ? 2 : 0) |
        (perms.canExecute () ? 1 : 0) ;
        PnfsFileId fileId = new PnfsFileId(null,pnfsId,
          fmd.getUid(), fmd.getGid(), permissions);
        return fileId;
    }*/
    
    
    /** 
	This code was added to perform the automatic creation of the directories
	the permissions and ownership is inherited from the parent path
     */
    private void createNextDirectory(FileMetaData parentFmd) {
        current_dir_depth++;
        String newDirPath = getCurrentDirPath();
        
	int uid = user.getUid();
	int gid = user.getGid();
	int perm = 0755;
	
	if ( parentFmd != null ) { 
		uid = Integer.parseInt(parentFmd.owner);
		gid = Integer.parseInt(parentFmd.group);
		perm = parentFmd.permMode;
	}

        say("attempting to create "+newDirPath+" with uid="+uid+" gid="+gid);
        
        PnfsGetStorageInfoMessage dirMsg
        = new PnfsCreateDirectoryMessage(newDirPath,uid,gid,perm) ;
        state = WAITING_FOR_CREATE_DIRECTORY_RESPONCE_MESSAGE;
        
        dirMsg.setReplyRequired(true);
        
        
        try {
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            dirMsg ) ,
            true , true ,
            this ,
            PNFS_TIMEOUT) ;
        }
        catch(Exception ee ) {
            esay(ee);
            callbacks.Exception(ee);
        }
    }
    
    private String getCurrentDirPath() {
        if(pathItems != null) {
            return FsPath.toString(pathItems.subList(0, current_dir_depth));
        }
        return "";
    }
    private String getCurrentDirPath(int i) {
        if(pathItems != null) {
            return FsPath.toString(pathItems.subList(0, i));
        }
        return "";
    }
    
    private void askPnfsForParentInfo() {
        if(current_dir_depth <= 1 ) {
            String error = "we reached the root of the directories, none of the elements exist from the pnfs manager point of vieww, we do not have permission to create this directory tree: "+path;
            unregisterAndFailCreator(error);
            callbacks.AuthorizationError(error);
            return;
        }
        
        current_dir_depth--;
        String directory = getCurrentDirPath();
        if(!registerCreatorOrWaitForCreation(directory, this)) {
            // if false is returned by registerCreatorOrWaitForCreation
            // we do not need to continue,
            // someone else is checking, creating this directory
            // we will be notified, when the direcory is created
            return;
        }
        PnfsGetFileMetaDataMessage metadataMsg =
        new PnfsGetFileMetaDataMessage() ;
        metadataMsg.setPnfsPath( directory ) ;
        
        try {
            state = WAITING_FOR_DIRECTORY_INFO_MESSAGE;
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            metadataMsg ) ,
            true , true ,
            this ,
            PNFS_TIMEOUT) ;
        }
        catch(Exception ee ) {
            esay(ee);
            callbacks.GetStorageInfoFailed(ee.toString());
        }
    }
    
    public void exceptionArrived( CellMessage request , Exception exception ) {
        esay("exceptionArrived "+exception+" for request "+request);
        unregisterAndFailCreator("exceptionArrived "+exception+" for request "+request);
        callbacks.Exception(exception);
    }
    public void answerTimedOut( CellMessage request ) {
        esay("answerTimedOut for request "+request);
        unregisterAndFailCreator("answerTimedOut for request "+request);
        callbacks.Timeout();
    }
    public String toString() {
        
        return "PutCompanion: "+
        " path=\""+path+"\" state="+getStateString()+ " : ";
    }
    
    public static void PrepareToPutFile(DCacheUser user,
    String path,
    PrepareToPutCallbacks callbacks,
    CellAdapter cell,
    boolean recursive_directory_creation,
    boolean overwrite) {
        
        if(user == null) {
            callbacks.AuthorizationError("user unknown, can not write");
            return;
        }
        
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(" FileRequest does not specify path!!!");
        }
        
        PnfsGetStorageInfoMessage storageInfoMsg =
        new PnfsGetStorageInfoMessage() ;
        storageInfoMsg.setPnfsPath( pnfsPath ) ;
        PutCompanion companion = new PutCompanion(
        user,
        path,
        callbacks,
        cell,
        recursive_directory_creation,
        overwrite);
        
        
        try {
            companion.state= WAITING_FOR_FILE_INFO_MESSAGE;
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            storageInfoMsg ) ,
            true , true ,
            companion ,
            PNFS_TIMEOUT) ;
        }
        catch(Exception ee ) {
            callbacks.GetStorageInfoFailed("can not contact pnfs manger: "+ee.toString());
        }
    }
    
    public void removeThisFromDirectoryCreators() {
        synchronized(directoryCreators) {
            while(directoryCreators.containsValue(this)) {
                for( Iterator i = directoryCreators.keySet().iterator();
                i.hasNext();) {
                    Object nextKey = i.next();
                    if(this == directoryCreators.get(nextKey)) {
                        directoryCreators.remove(nextKey);
                        break;
                    }
                }
            }
        }
    }
    
    private static HashMap directoryCreators = new HashMap();
    private OneToManyMap waitingForCreators = new OneToManyMap();
    
    private static boolean registerCreatorOrWaitForCreation(String pnfsPath,
    PutCompanion thisCreator) {
        synchronized( directoryCreators) {
            if(directoryCreators.containsKey(pnfsPath)) {
                PutCompanion creatorCompanion =   (PutCompanion)directoryCreators.get(pnfsPath);
                creatorCompanion.waitingForCreators.put(pnfsPath, thisCreator);
                thisCreator.say("registerCreatorOrWaitForCreation("+pnfsPath+","+thisCreator+")"+
                " directoryCreators already contains the creator for the path, store and return false"
                );
                return false;
            }
            else {
                thisCreator.say("registerCreatorOrWaitForCreation("+pnfsPath+","+thisCreator+")"+
                " storing this creator"
                );
                directoryCreators.put(pnfsPath,thisCreator);
                return true;
            }
        }
    }
    
    
    private static void unregisterCreator(String pnfsPath,PutCompanion thisCreator,
    PnfsGetFileMetaDataMessage message) {
        HashSet  removed = new HashSet();
        
        synchronized( directoryCreators) {
            if(directoryCreators.containsValue(thisCreator)) {
                thisCreator.say("unregisterCreator("+
                pnfsPath+","+thisCreator+")"
                );
                
                directoryCreators.remove(pnfsPath);
            }
            while(thisCreator.waitingForCreators.containsKey(pnfsPath)) {
                Object o = thisCreator.waitingForCreators.remove(pnfsPath);
                thisCreator.say("unregisterCreator("+
                pnfsPath+","+thisCreator+") removing "+o);
                removed.add(o);
            }
        }
        
        for(Iterator i = removed.iterator(); i.hasNext();) {
            PutCompanion waitingcompanion =  (PutCompanion) i.next();
            thisCreator.say("  unregisterCreator("+
            pnfsPath+","+thisCreator+") notifying "+waitingcompanion
            );
            waitingcompanion.directoryInfoArrived(message);
        }
        thisCreator.say(" unregisterCreator("+
        pnfsPath+","+thisCreator+") returning");
        
    }
    
    private void unregisterCreator( PnfsGetFileMetaDataMessage message) {
        unregisterCreator(this.getCurrentDirPath(), this, message);
    }
    
    private void unregisterAndFailCreator(String error) {
        //directory creation failed, notify all waiting on this companion
        for( int i = this.current_dir_depth; i<(this.pathItems.size());
        ++i) {
            String pnfsPath = this.getCurrentDirPath(i);
            unregisterAndFailCreator(pnfsPath, this, error);
        }
    }
    private static void unregisterAndFailCreator(String pnfsPath,PutCompanion thisCreator,
    String error) {
        HashSet  removed = new HashSet();
        
        synchronized( directoryCreators) {
            if(directoryCreators.containsValue(thisCreator)) {
                thisCreator.esay(" unregisterAndFailCreator("+
                pnfsPath+","+thisCreator+")"
                );
                
                directoryCreators.remove(pnfsPath);
            }
            while(thisCreator.waitingForCreators.containsKey(pnfsPath)) {
                Object o = thisCreator.waitingForCreators.remove(pnfsPath);
                thisCreator.esay("  unregisterAndFailCreator("+
                pnfsPath+","+thisCreator+") removing "+o);
                removed.add(o);
            }
        }
        
        for(Iterator i = removed.iterator(); i.hasNext();) {
            PutCompanion waitingcompanion =  (PutCompanion) i.next();
            thisCreator.esay(" unregisterAndFailCreator("+
            pnfsPath+","+thisCreator+") notifying "+waitingcompanion
            );
            waitingcompanion.callbacks.Error(error);
        }
        thisCreator.esay(" unregisterAndFailCreator("+
        pnfsPath+","+thisCreator+") returning");
        
    }
    public String getStateString() {
        switch (state) {
            case  INITIAL_STATE:{ 
                return "INITIAL_STATE";
            } 
            case  WAITING_FOR_FILE_INFO_MESSAGE:{ 
                return "WAITING_FOR_FILE_INFO_MESSAGE";
            } 
            case  RESEIVED_FILE_INFO_MESSAGE:{
                return "RESEIVED_FILE_INFO_MESSAGE";
            } 
            case  WAITING_FOR_DIRECTORY_INFO_MESSAGE: { 
                return "WAITING_FOR_DIRECTORY_INFO_MESSAGE"; 
            } 
            case  RECEIVED_DIRECTORY_INFO_MESSAGE: { 
                return "RECEIVED_DIRECTORY_INFO_MESSAGE"; 
            } 
            case  WAITING_FOR_CREATE_DIRECTORY_RESPONCE_MESSAGE:    { 
                return "WAITING_FOR_CREATE_DIRECTORY_RESPONCE_MESSAGE ("+
                        getCurrentDirPath()+")"; 
            } 
            case  RECEIVED_CREATE_DIRECTORY_RESPONCE_MESSAGE:{ 
                return "RECEIVED_CREATE_DIRECTORY_RESPONCE_MESSAGE ("+
                        getCurrentDirPath()+")"; 
            } 
            case  FINAL_STATE :{ 
                return "FINAL_STATE"; 
            }
            default: {
                return "unknown";
            }
        }
    }
}



