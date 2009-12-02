// $Id$
// $Log: not supported by cvs2svn $
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
import org.dcache.auth.AuthorizationRecord;
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
import java.util.Set;
import java.util.Date;
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
    private static final int WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE=5;
    private static final int RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE=6;
    private static final int FINAL_STATE=8;
    private static final int PASSIVELY_WAITING_FOR_DIRECTORY_INFO_MESSAGE=9;
    private volatile int state=INITIAL_STATE;
    
    private dmg.cells.nucleus.CellAdapter cell;
    private PrepareToPutCallbacks callbacks;
    private CellMessage request = null;
    private String path;
    private boolean recursive_directory_creation;
    private java.util.List pathItems=null;
    private int current_dir_depth = -1;
    private AuthorizationRecord user;
    private boolean overwrite;
    private String fileId;
    private FileMetaData fileFMD;
    private long creationTime = System.currentTimeMillis();
    private long lastOperationTime = creationTime;
    
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
    
    private PutCompanion(AuthorizationRecord user,
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
                PnfsCreateDirectoryMessage create_directory_response =
                (PnfsCreateDirectoryMessage)message;
                if( state == WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE) {
                    state = RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE;
                    directoryInfoArrived(create_directory_response);
                    return;
                }
                esay(this.toString()+" got unexpected PnfsCreateDirectoryMessage "+
                " : "+create_directory_response+" ; Ignoring");
            }
            else if( message instanceof PnfsGetStorageInfoMessage ) {
                PnfsGetStorageInfoMessage storage_info_msg =
                (PnfsGetStorageInfoMessage)message;
                if(state == WAITING_FOR_FILE_INFO_MESSAGE) {
                    state = RESEIVED_FILE_INFO_MESSAGE;
                    fileInfoArrived(storage_info_msg);
                    return;
                } else if(state == WAITING_FOR_DIRECTORY_INFO_MESSAGE) {
                    state = RECEIVED_DIRECTORY_INFO_MESSAGE;
                    directoryInfoArrived(storage_info_msg);
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
                fileFMD = Storage.getFileMetaData(user,path,pnfsId,null,fmd);
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
            if(state == RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE) {
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
        
        StorageInfo storageInfo = null;
        if(metadata_msg instanceof PnfsGetStorageInfoMessage) {
            PnfsGetStorageInfoMessage storage_info_msg  = 
                (PnfsGetStorageInfoMessage) metadata_msg;
            storageInfo = storage_info_msg.getStorageInfo() ;
            say("storageInfo = "+storageInfo );
        }
        
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
        say(" calling Storage.getFileMetaData()");
        FileMetaData srm_dirFmd = Storage.getFileMetaData(user,path,dirPnfsId,storageInfo,dirFmd);
        say(" got  srm_dirFmd.retentionPolicyInfo ="+srm_dirFmd.retentionPolicyInfo);
        if(srm_dirFmd.retentionPolicyInfo != null) {
            say(" got  srm_dirFmd.retentionPolicyInfo.AccessLatency ="+srm_dirFmd.retentionPolicyInfo.getAccessLatency());
            say(" got  srm_dirFmd.retentionPolicyInfo.RetentionPolicy ="+srm_dirFmd.retentionPolicyInfo.getRetentionPolicy());
            say(" got  srm_dirFmd.spaceTokens ="+(srm_dirFmd.spaceTokens!=null?srm_dirFmd.spaceTokens[0]:"NONE"));
        }   
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
        state = WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE;
        
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
        lastOperationTime = System.currentTimeMillis();
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
            state = PASSIVELY_WAITING_FOR_DIRECTORY_INFO_MESSAGE;
            lastOperationTime = System.currentTimeMillis();
           return;
        }
        PnfsGetFileMetaDataMessage metadataMsg;
        if(current_dir_depth == (pathItems.size() -1)) {
            metadataMsg =
            new PnfsGetStorageInfoMessage() ;
            
        } else {

            metadataMsg =
            new PnfsGetFileMetaDataMessage() ;
        }
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
            unregisterAndFailCreator(ee.toString());
            callbacks.GetStorageInfoFailed(ee.toString());
        }
        lastOperationTime= System.currentTimeMillis();
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
        StringBuffer sb = new StringBuffer();
        toString(sb,false);
        return sb.toString();
    }
    
    public void toString(StringBuffer sb, boolean longFormat) {
        
        sb.append("PutCompanion: ");
        sb.append(" p=\"").append(path);
        sb.append("\" s=\"").append(getStateString());
        sb.append("\" d=\"").append(current_dir_depth);
        sb.append("\" created:").append(new Date(creationTime).toString());
        sb.append("\" lastOperation:").append(new Date(lastOperationTime).toString());
        
       if (state == WAITING_FOR_DIRECTORY_INFO_MESSAGE ||
            state == WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE) {
            try {
               for( int i = current_dir_depth;
                         i<pathItems.size();
                       ++i) {
                    String pnfsPath = getCurrentDirPath(i); 
                    sb.append("\n it is creating/getting info for path=").append(pnfsPath);
                    Set waitingSet = this.waitingForCreators.getValues(pnfsPath);
                    if(waitingSet != null) {
                        if(longFormat) {
                            for(Iterator j = waitingSet.iterator(); j.hasNext();) {
                                PutCompanion waitingCompanion = (PutCompanion) j.next();
                                sb.append("\n waiting companion:");
                                waitingCompanion.toString(sb,true);

                            }
                        }
                        else {
                            sb.append(" num of waiting companions:").append(waitingSet.size());
                        }
                    }
                    else {
                       sb.append(" num of waiting companions: 0");   
                    }
                }
            } catch(Exception e) {
                sb.append("listing error: ").append(e);
            }
       }
    }
    
   private String getStateString() {
        switch (state) {
            case INITIAL_STATE: 
                return "INITIAL_STATE";
            case WAITING_FOR_FILE_INFO_MESSAGE:
                return "WAITING_FOR_FILE_INFO_MESSAGE";
            case RESEIVED_FILE_INFO_MESSAGE:
                return "RESEIVED_FILE_INFO_MESSAGE";
            case WAITING_FOR_DIRECTORY_INFO_MESSAGE:
                return "WAITING_FOR_DIRECTORY_INFO_MESSAGE";
            case RECEIVED_DIRECTORY_INFO_MESSAGE:
                return "RECEIVED_DIRECTORY_INFO_MESSAGE";
            case WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE:
                return "WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE";
            case RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE:
                return "RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE";
            case FINAL_STATE:
                return "FINAL_STATE";
            case PASSIVELY_WAITING_FOR_DIRECTORY_INFO_MESSAGE:
                return "PASSIVELY_WAITING_FOR_DIRECTORY_INFO_MESSAGE";
            default:
                return "unknown_STATE";

        }
    }
    
    public static void PrepareToPutFile(AuthorizationRecord user,
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
    
    public static void listDirectoriesWaitingForCreation(StringBuffer sb, boolean longformat) {
        synchronized( directoryCreators) {
            Set directories = directoryCreators.keySet();
            for(Iterator iter=directories.iterator(); iter.hasNext();) {
                String directory = (String) iter.next();
                sb.append("directorty: ").append(directory).append('\n');
                PutCompanion creatorCompanion =   (PutCompanion)directoryCreators.get(directory);
                sb.append("\n creating/getting info companion:");
                creatorCompanion.toString(sb,longformat);
             }
        }                
    }
    
    public static void failCreatorsForPath(String pnfsPath, StringBuffer sb) {
        PutCompanion creatorCompanion  =   
            (PutCompanion)directoryCreators.get(pnfsPath);
        if(creatorCompanion == null) {
            sb.append("no creators for path ").append(pnfsPath).append("found");
            return;
        }
        creatorCompanion.unregisterAndFailCreator("canceled by the admin command");
        sb.append("Done");
        return;
    }
    
     private static boolean registerCreatorOrWaitForCreation(String pnfsPath,
    PutCompanion thisCreator) {
        long creater_operTime=0; 
        long currentTime=0;
        PutCompanion creatorCompanion = null;
        synchronized( directoryCreators) {
            if(directoryCreators.containsKey(pnfsPath)) {
                creatorCompanion =   (PutCompanion)directoryCreators.get(pnfsPath);
                creatorCompanion.waitingForCreators.put(pnfsPath, thisCreator);
                thisCreator.say("registerCreatorOrWaitForCreation("+pnfsPath+","+thisCreator+")"+
                " directoryCreators already contains the creator for the path, store and return false"
                );
                creater_operTime = creatorCompanion.lastOperationTime;
                currentTime = System.currentTimeMillis();
            }
            else {
                thisCreator.say("registerCreatorOrWaitForCreation("+pnfsPath+","+thisCreator+")"+
                " storing this creator"
                );
                directoryCreators.put(pnfsPath,thisCreator);
                return true;
            }
        }
        
        if(creatorCompanion != null &&
            currentTime - creater_operTime > creatorCompanion.PNFS_TIMEOUT ) {
            creatorCompanion.unregisterAndFailCreator("pnfs manager timeout");
        }
        return false;
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
        if(pathItems != null && current_dir_depth != -1) {
            //directory creation failed, notify all waiting on this companion
            for( int i = this.current_dir_depth; i<(this.pathItems.size());
            ++i) {
                String pnfsPath = this.getCurrentDirPath(i);
                unregisterAndFailCreator(pnfsPath, this, error);
            }
        } else {
            callbacks.Error(error);
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

}



