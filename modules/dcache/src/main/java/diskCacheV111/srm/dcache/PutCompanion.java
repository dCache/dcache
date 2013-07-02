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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;

import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.ThreadManagerMessageCallback;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileType.LINK;
import static org.dcache.namespace.FileType.REGULAR;


/**
 *
 * @author  timur
 */

public final class PutCompanion extends AbstractMessageCallback<PnfsMessage>
{
    private final static Logger _log = LoggerFactory.getLogger(PutCompanion.class);

    public  static final long PNFS_TIMEOUT =  TimeUnit.MINUTES.toMillis(3);

    public enum State {
        INITIAL_STATE,
        WAITING_FOR_FILE_INFO_MESSAGE,
        RECEIVED_FILE_INFO_MESSAGE,
        WAITING_FOR_DIRECTORY_INFO_MESSAGE,
        RECEIVED_DIRECTORY_INFO_MESSAGE,
        WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE,
        RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE,
        WAITING_FOR_FILE_DELETE_RESPONSE_MESSAGE,
        RECEIVED_FILE_DELETE_RESPONSE_MESSAGE,
        FINAL_STATE,
        PASSIVELY_WAITING_FOR_DIRECTORY_INFO_MESSAGE
    }

    private volatile State state=State.INITIAL_STATE;

    private CellStub pnfsStub;
    private PrepareToPutCallbacks callbacks;
    private String path;
    private boolean recursive_directory_creation;
    private List<String> pathItems;
    private int current_dir_depth = -1;
    private Subject subject;
    private boolean overwrite;
    private String fileId;
    private FileMetaData fileFMD;
    private long creationTime = System.currentTimeMillis();
    private long lastOperationTime = creationTime;
    private PermissionHandler permissionHandler;

    private PutCompanion(Subject subject,
                         PermissionHandler permissionHandler,
                         String path,
                         PrepareToPutCallbacks callbacks,
                         CellStub pnfsStub,
                         boolean recursive_directory_creation,
                         boolean overwrite) {
        this.subject = subject;
        this.permissionHandler = permissionHandler;
        this.path = path;
        this.pnfsStub = pnfsStub;
        this.callbacks = callbacks;
        this.recursive_directory_creation = recursive_directory_creation;
        this.overwrite = overwrite;
        pathItems = (new FsPath(path)).getPathItemsList();
        _log.debug(" constructor path = "+path+" overwrite="+overwrite);
    }

    @Override
    public void success(PnfsMessage message) {
        if( message instanceof PnfsCreateDirectoryMessage) {
            PnfsCreateDirectoryMessage response = (PnfsCreateDirectoryMessage)message;
            if( state == State.WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE) {
                state = State.RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE;
                directoryInfoArrived(response);
            }
            else {
                _log.warn(" {}  : unexpected PnfsCreateDirectoryMessage : {} , Ignoring",
                          this.toString(),
                          message);
            }
        }
        else if (message instanceof PnfsMapPathMessage) {
            PnfsMapPathMessage response = (PnfsMapPathMessage) message;
            if (state == State.WAITING_FOR_FILE_INFO_MESSAGE) {
                state = State.RECEIVED_FILE_INFO_MESSAGE;
                fileExists(response);
            }
            else {
                _log.warn(" {}  : unexpected PnfsMapPathMessage : {} , Ignoring",
                          this.toString(),
                          message);
            }
        }
        else if (message instanceof PnfsGetStorageInfoMessage) {
            PnfsGetStorageInfoMessage response =
                (PnfsGetStorageInfoMessage)message;
            if (state == State.WAITING_FOR_DIRECTORY_INFO_MESSAGE) {
                state = State.RECEIVED_DIRECTORY_INFO_MESSAGE;
                directoryInfoArrived(response);
            }
            else {
                _log.warn(" {}  : unexpected PnfsGetStorageInfoMessage : {} , Ignoring",
                          this.toString(),
                          message);
            }
        }
        else if( message instanceof PnfsGetFileMetaDataMessage ) {
            PnfsGetFileMetaDataMessage response =
                (PnfsGetFileMetaDataMessage)message;
            if(state == State.WAITING_FOR_DIRECTORY_INFO_MESSAGE) {
                state = State.RECEIVED_DIRECTORY_INFO_MESSAGE;
                directoryInfoArrived(response);
            }
            else  {
                _log.warn(" {}  : unexpected PnfsGetFileMetaDataMessage : {} , Ignoring",
                          this.toString(),
                          message);
            }
        }
        else if( message instanceof PnfsDeleteEntryMessage ) {
            if (state == State.WAITING_FOR_FILE_DELETE_RESPONSE_MESSAGE) {
                state = State.RECEIVED_FILE_DELETE_RESPONSE_MESSAGE;
                askPnfsForParentInfo();
            }
            else {
                _log.warn(" {}  : unexpected  PnfsDeleteEntryMessage : {} , Ignoring",
                          this.toString(),
                          message);
            }
        }
        else {
            _log.warn(" {}  : unexpected Message : {} , Ignoring",
                      this.toString(),
                      message);
        }
    }

    @Override
    public void failure(int rc, Object error) {
        String errorString = error.toString();
        if (_log.isDebugEnabled()) {
            _log.debug("PutCompanion.failure() {}, rc={} error={}", this.toString(), rc, errorString);
        }
        if (state == State.WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE) {
            state = State.RECEIVED_CREATE_DIRECTORY_RESPONSE_MESSAGE;
            errorString = "directory creation failed: "+
                getCurrentDirPath()+" reason: "+error;
            unregisterAndFailCreator(errorString);
        }
        else if (state == State.WAITING_FOR_FILE_INFO_MESSAGE) {
            state = State.RECEIVED_FILE_INFO_MESSAGE;
            if (rc == CacheException.FILE_NOT_FOUND) {
                current_dir_depth = pathItems.size();
            }
        }
        else if (state == State.WAITING_FOR_DIRECTORY_INFO_MESSAGE) {
            state = State.RECEIVED_DIRECTORY_INFO_MESSAGE;
            if (rc != CacheException.FILE_NOT_FOUND ||
                !recursive_directory_creation)  {
                unregisterAndFailCreator(error.toString());
            }
        }
        else if (state == State.WAITING_FOR_FILE_DELETE_RESPONSE_MESSAGE) {
            state = State.RECEIVED_FILE_DELETE_RESPONSE_MESSAGE;
        }

        switch (rc) {
        case CacheException.PERMISSION_DENIED:
            callbacks.AuthorizationError(errorString);
            break;
        case CacheException.FILE_NOT_FOUND:
            if (state == State.RECEIVED_FILE_INFO_MESSAGE ||
                state == State.RECEIVED_FILE_DELETE_RESPONSE_MESSAGE ||
                state == State.RECEIVED_DIRECTORY_INFO_MESSAGE) {
                askPnfsForParentInfo();
            }
            break;
        case CacheException.INVALID_ARGS:
            if (state == State.RECEIVED_FILE_DELETE_RESPONSE_MESSAGE) {
                errorString = "Destination is not a file";
            }
            callbacks.InvalidPathError(errorString);
            break;
        case CacheException.TIMEOUT:
            callbacks.Timeout();
            break;
        default:
            if (state == State.RECEIVED_DIRECTORY_INFO_MESSAGE) {
                callbacks.GetStorageInfoFailed("GetStorageInfoFailed " +
                                               "PnfsGetStorageInfoMessage.getReturnCode () != 0 => " +
                                               "parrent directory does not exist");
            }
            else {
                callbacks.Error(errorString);
            }
        }
    }

    public void noroute() {
        _log.error(this.toString()+" No Route to PnfsManager");
        unregisterAndFailCreator("No Route to PnfsManager");
        callbacks.Error("No Route to PnfsManager");
    }

    public void timeout() {
        _log.error(this.toString()+" PnfsManager request Timed Out");
        unregisterAndFailCreator("PnfsManager request Timed Out");
        callbacks.Timeout();
    }


    private void fileExists(PnfsMapPathMessage message) {

        if(!overwrite) {
            String errorString = String.format("file/directory %s exists, overwite is not allowed ",path);
            _log.debug(errorString);
            callbacks.DuplicationError(errorString);
            return;
        }
        //
        //  remove existing file
        //
        state = State.WAITING_FOR_FILE_DELETE_RESPONSE_MESSAGE;
        PnfsDeleteEntryMessage deleteMessage = new PnfsDeleteEntryMessage(message.getPnfsId(),
                                                                          EnumSet.of(LINK, REGULAR));
        deleteMessage.setSubject(subject);
        pnfsStub.send(deleteMessage,
                      PnfsDeleteEntryMessage.class,
                      new ThreadManagerMessageCallback(this));
        current_dir_depth = pathItems.size();
    }

    public void directoryInfoArrived(PnfsGetFileMetaDataMessage metadata_msg) {
        try{
            unregisterCreator(metadata_msg);
            FileAttributes attributes = metadata_msg.getFileAttributes();
            attributes.setPnfsId(metadata_msg.getPnfsId());
            if (attributes.getFileType() != FileType.DIR) {
                String error ="file "+metadata_msg.getPnfsPath()+
                " is not a directory";
                unregisterAndFailCreator(error);
                callbacks.InvalidPathError(error);
                return;
            }
            FileMetaData srm_dirFmd = new DcacheFileMetaData(attributes);
            if((pathItems.size() -1 ) >current_dir_depth) {
                AccessType canCreateSubDir =
                        permissionHandler.canCreateSubDir(subject,
                        attributes);
                if(canCreateSubDir == AccessType.ACCESS_ALLOWED) {
                    createNextDirectory(srm_dirFmd);
                }
                else {
                    String error = "path does not exist and user has no " +
                            "permissions to create it";
                    _log.warn(error);
                    unregisterAndFailCreator(error);
                    callbacks.InvalidPathError(error);
                }
            }
            else {
                 AccessType canCreateFile =
                          permissionHandler.canCreateFile(subject,
                          attributes);
                if(canCreateFile != AccessType.ACCESS_ALLOWED ) {
                    String error = "user has no permission to create file "+
                            getCurrentDirPath();
                    _log.warn(error);
                    callbacks.AuthorizationError(error);
                    return;

                }

                callbacks.StorageInfoArrived(fileId,
                                             fileFMD,
                                             srm_dirFmd.fileId,
                                             srm_dirFmd);
            }
        }
        catch(RuntimeException re) {
            _log.error(re.toString(), re);
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

        int uid = (int) Subjects.getUid(subject);
        int gid = (int) Subjects.getPrimaryGid(subject);
        int perm = 0755;

        if ( parentFmd != null ) {
            uid = Integer.parseInt(parentFmd.owner);
            gid = Integer.parseInt(parentFmd.group);
            perm = parentFmd.permMode;
        }

        _log.info("attempting to create "+newDirPath+" with uid="+uid+" gid="+gid);

        PnfsGetStorageInfoMessage dirMsg
        = new PnfsCreateDirectoryMessage(newDirPath,uid,gid,perm,
                permissionHandler.getRequiredAttributes()) ;
        state = State.WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE;

        dirMsg.setReplyRequired(true);


        try {
            pnfsStub.send(dirMsg,PnfsMessage.class,
                    new ThreadManagerMessageCallback(this) );
        }
        catch(Exception ee ) {
            _log.error(ee.toString());
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
        if(current_dir_depth < 1 ) {
            String error = "we reached the root of the directories, " +
                    "none of the elements exist from the pnfs manager " +
                    "point of view, we do not have permission to create " +
                    "this directory tree: "+path;
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
            state = State.PASSIVELY_WAITING_FOR_DIRECTORY_INFO_MESSAGE;
            lastOperationTime = System.currentTimeMillis();
           return;
        }
        PnfsGetFileMetaDataMessage metadataMsg;
        if(current_dir_depth == (pathItems.size() -1)) {
            metadataMsg =
            new PnfsGetStorageInfoMessage(
                    permissionHandler.getRequiredAttributes()) ;

        } else {

            metadataMsg =
            new PnfsGetFileMetaDataMessage(
                    permissionHandler.getRequiredAttributes()) ;
        }
        metadataMsg.setPnfsPath( directory ) ;
        state = State.WAITING_FOR_DIRECTORY_INFO_MESSAGE;
        pnfsStub.send(metadataMsg, PnfsMessage.class,
                new ThreadManagerMessageCallback(this) );
        lastOperationTime= System.currentTimeMillis();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb,false);
        return sb.toString();
    }

    public void toString(StringBuilder sb, boolean longFormat) {

        sb.append("PutCompanion: ");
        sb.append(" p=\"").append(path);
        sb.append("\" s=\"").append(getStateString());
        sb.append("\" d=\"").append(current_dir_depth);
        sb.append("\" created:").append(new Date(creationTime));
        sb.append("\" lastOperation:").append(new Date(lastOperationTime));

       if (state == State.WAITING_FOR_DIRECTORY_INFO_MESSAGE ||
            state == State.WAITING_FOR_CREATE_DIRECTORY_RESPONSE_MESSAGE) {
            try {
               for( int i = current_dir_depth;
                         i<pathItems.size();
                       ++i) {
                    String pnfsPath = getCurrentDirPath(i);
                    sb.append("\n it is creating/getting info for path=");
                    sb.append(pnfsPath);
                    Collection<PutCompanion> waitingSet =
                        this.waitingForCreators.get(pnfsPath);
                    if(waitingSet != null) {
                        if(longFormat) {
                            for (PutCompanion waitingCompanion: waitingSet) {
                                sb.append("\n waiting companion:");
                                waitingCompanion.toString(sb,true);
                            }
                        }
                        else {
                            sb.append(" num of waiting companions:");
                            sb.append(waitingSet.size());
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
       return state.toString();
   }

    public static void PrepareToPutFile(Subject subject,
                                        PermissionHandler permissionHandler,
                                        String path,
                                        PrepareToPutCallbacks callbacks,
                                        CellStub pnfsStub,
                                        boolean recursive_directory_creation,
                                        boolean overwrite) {
        if(subject == null) {
            callbacks.AuthorizationError("user unknown, can not write");
            return;
        }

        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(
                    " FileRequest does not specify path!!!");
        }
        PnfsMapPathMessage message = new PnfsMapPathMessage(pnfsPath);
        message.setSubject(subject);
        PutCompanion companion = new PutCompanion(
            subject,
            permissionHandler,
            path,
            callbacks,
            pnfsStub,
            recursive_directory_creation,
            overwrite);
        _log.debug("sending " +message+" to PnfsManager");
        companion.state = State.WAITING_FOR_FILE_INFO_MESSAGE;
        pnfsStub.send(message,
                      PnfsMapPathMessage.class,
                      new ThreadManagerMessageCallback(companion) ) ;
    }

    public void removeThisFromDirectoryCreators()
    {
        synchronized(directoryCreators) {
            Iterator<PutCompanion> i = directoryCreators.values().iterator();
            while (i.hasNext()) {
                if (this == i.next()) {
                    i.remove();
                    break;
                }
            }
        }
    }

    private static final Map<String,PutCompanion> directoryCreators =
        new HashMap<>();
    private Multimap<String,PutCompanion> waitingForCreators = HashMultimap.create();

    public static void listDirectoriesWaitingForCreation(StringBuilder sb,
            boolean longformat)
    {
        synchronized (directoryCreators) {
            for (Map.Entry<String,PutCompanion> entry: directoryCreators.entrySet()) {
                String directory = entry.getKey();
                PutCompanion companion = entry.getValue();
                sb.append("directorty: ").append(directory).append('\n');
                sb.append("\n creating/getting info companion:");
                companion.toString(sb,longformat);
             }
        }
    }

    public static void failCreatorsForPath(String pnfsPath, StringBuilder sb) {
        PutCompanion creatorCompanion = directoryCreators.get(pnfsPath);
        if(creatorCompanion == null) {
            sb.append("no creators for path ").append(pnfsPath).append("found");
            return;
        }
        creatorCompanion.unregisterAndFailCreator("canceled by the admin command");
        sb.append("Done");
    }

     private static boolean registerCreatorOrWaitForCreation(String pnfsPath,
    PutCompanion thisCreator) {
        long creater_operTime;
        long currentTime;
        PutCompanion creatorCompanion;
        synchronized( directoryCreators) {
            if(directoryCreators.containsKey(pnfsPath)) {
                creatorCompanion = directoryCreators.get(pnfsPath);
                creatorCompanion.waitingForCreators.put(pnfsPath, thisCreator);
                _log.debug("registerCreatorOrWaitForCreation("+pnfsPath+","+
                        thisCreator+")"+
                " directoryCreators already contains the creator for the path," +
                        " store and return false"
                );
                creater_operTime = creatorCompanion.lastOperationTime;
                currentTime = System.currentTimeMillis();
            }
            else {
                _log.debug("registerCreatorOrWaitForCreation("+pnfsPath+","+
                        thisCreator+")"+
                " storing this creator"
                );
                directoryCreators.put(pnfsPath,thisCreator);
                return true;
            }
        }

        if(currentTime - creater_operTime > PNFS_TIMEOUT ) {
            creatorCompanion.unregisterAndFailCreator("pnfs manager timeout");
        }
        return false;
    }


    private static void unregisterCreator(String pnfsPath,PutCompanion thisCreator,
    PnfsGetFileMetaDataMessage message) {
        Collection<PutCompanion> removed;

        synchronized( directoryCreators) {
            if(directoryCreators.containsValue(thisCreator)) {
                _log.debug("unregisterCreator("+pnfsPath+","+thisCreator+")");

                directoryCreators.remove(pnfsPath);
            }
            removed = thisCreator.waitingForCreators.removeAll(pnfsPath);
            for (PutCompanion companion: removed) {
                _log.debug("unregisterCreator(" +
                        pnfsPath + "," + thisCreator + ") removing " + companion);
            }
        }

        for (PutCompanion waitingcompanion: removed) {
            _log.debug("  unregisterCreator("+pnfsPath+
                       ","+thisCreator+") notifying "+waitingcompanion);
            waitingcompanion.directoryInfoArrived(message);
        }
        _log.debug(" unregisterCreator("+
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


    private static void unregisterAndFailCreator(String pnfsPath,
            PutCompanion thisCreator,
            String error) {
        Collection<PutCompanion> removed;

        synchronized( directoryCreators) {
            if(directoryCreators.containsValue(thisCreator)) {
                _log.debug(" unregisterAndFailCreator("+
                           pnfsPath+","+thisCreator+")");

                directoryCreators.remove(pnfsPath);
            }
            removed = thisCreator.waitingForCreators.removeAll(pnfsPath);
        }
        for (PutCompanion companion: removed) {
            _log.debug("  unregisterAndFailCreator("+
                    pnfsPath+","+thisCreator+") removing " + companion);
        }

        for (PutCompanion waitingcompanion: removed) {
            _log.debug(" unregisterAndFailCreator("+pnfsPath+
                       ","+thisCreator+") notifying "+waitingcompanion);
            waitingcompanion.callbacks.Error(error);
        }
        _log.debug(" unregisterAndFailCreator("+
                   pnfsPath+","+thisCreator+") returning");
    }
}



