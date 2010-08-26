// $Id: ReserveSpaceCompanion.java,v 1.12 2005-12-19 23:06:54 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11  2005/10/07 22:59:47  timur
// work towards v2
//
// Revision 1.10  2005/09/30 21:47:39  timur
// more space reservation - pnfs communication improvements
//
// Revision 1.9  2005/09/28 21:36:07  timur
// removed unused file, more debugging when deleting pnfs entry
//
// Revision 1.8  2005/09/27 21:46:51  timur
// do not leave pnfs entry behind after space reservation is created
//
// Revision 1.7  2005/03/10 23:12:07  timur
// Fisrt working version of space reservation module
//
// Revision 1.6  2005/03/09 23:22:57  timur
// more space reservation code
//
// Revision 1.5  2005/03/07 22:57:44  timur
// more work on space reservation
//
// Revision 1.4  2005/02/17 02:16:30  timur
//  added a debug message
//
// Revision 1.3  2005/02/02 22:57:21  timur
// working on space manager
//
// Revision 1.2  2005/01/31 22:52:04  timur
// started working on space reservation
//
// Revision 1.1  2004/10/20 21:32:30  timur
// adding classes for space management
//
// Revision 1.2  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//
// Revision 1.1.2.1  2004/05/18 21:40:30  timur
// incorporation of the new scheduler into srm, repackaging of all the srm classes
//
// Revision 1.1  2003/10/30 00:15:57  cvs
// srmReserveSpace implemented, started working on srmPrepareToPut
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

package diskCacheV111.services;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolReserveSpaceMessage;

import java.net.InetAddress;

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
public class ReserveSpaceCompanion implements CellMessageAnswerable {
    private  static final int NOT_WAITING_STATE=0;
    private  static final int WAITING_PNFS_GET_STORAGE_INFO_RESPONSE_STATE=1;
    private  static final int WAITING_POOLMANAGER_RESPONSE_STATE=2;
    private  static final int WAITING_POOL_RESPONSE_STATE=3;
    private  static final int WAITING_PNFS_CREATE_ENTRY_RESPONSE_STATE=4;
    private  static final int WAITING_PNFS_DELETE_ENTRY_RESPONSE_STATE=5;
    private  static final int WAITING_PNFS_GET_PARENT_STORAGE_INFO_RESPONSE_STATE=6;

    private volatile int state = NOT_WAITING_STATE;
    private dmg.cells.nucleus.CellAdapter cell;
    private ReserveSpaceCallbacks callbacks;
    private CellMessage request = null;
    // this is a path of the root directory of the user
    private String path;
    // this is pnfsid of the root
    private PnfsId pnfsId;
    // this is the name of the pool that we will reserve space on
    private String pool;
    private StorageInfo storageInfo;
    private long spaceSize;
    private long totalPoolReservedSpaceSize;
    private String spaceUserHost;
    private int uid=-1;
    private int gid = -1;
    private boolean created =false;
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(" ReserveSpaceCompanion : "+words_of_wisdom);
        }
    }

    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.esay(" ReserveSpaceCompanion : "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(cell!=null) {
            cell.esay(" ReserveSpaceCompanion exception : ");
            cell.esay(t);
        }
    }

    public static final String getStateString(int state) {
        switch(state) {
            case NOT_WAITING_STATE:
                return "NOT_WAITING_STATE";
            case WAITING_PNFS_GET_STORAGE_INFO_RESPONSE_STATE:
                return "WAITING_PNFS_GET_STORAGE_INFO_RESPONSE_STATE";
            case WAITING_POOLMANAGER_RESPONSE_STATE:
                return "WAITING_POOLMANAGER_RESPONSE_STATE";
            case WAITING_POOL_RESPONSE_STATE:
                return "WAITING_POOL_RESPONSE_STATE";
            case WAITING_PNFS_CREATE_ENTRY_RESPONSE_STATE:
                return "WAITING_PNFS_CREATE_ENTRY_RESPONSE_STATE";
            case WAITING_PNFS_DELETE_ENTRY_RESPONSE_STATE:
                return "WAITING_PNFS_DELETE_ENTRY_RESPONSE_STATE";
            case WAITING_PNFS_GET_PARENT_STORAGE_INFO_RESPONSE_STATE:
                return "WAITING_PNFS_GET_PARENT_STORAGE_INFO_RESPONSE_STATE";
            default:
                return "UNKNOWN";
        }
    }


    /** Creates a new instance of StageAndPinCompanion */

    private ReserveSpaceCompanion(
    String path,
    ReserveSpaceCallbacks callbacks,
    long spaceSize,
    String spaceUserHost,
    CellAdapter cell) {
        this.path = path;
        this.cell = cell;
        this.spaceSize = spaceSize;
        this.callbacks = callbacks;
        this.spaceUserHost = spaceUserHost;
        say(" constructor path = "+path);
    }


    private ReserveSpaceCompanion(
    String path,
    StorageInfo storageInfo,
    PnfsId pnfsId,
    ReserveSpaceCallbacks callbacks,
    long spaceSize,
    String spaceUserHost,
    CellAdapter cell) {
        this.path = path;
        this.pnfsId = pnfsId;
        this.storageInfo = storageInfo;
        this.cell = cell;
        this.spaceSize = spaceSize;
        this.callbacks = callbacks;
        this.spaceUserHost = spaceUserHost;
        say(" constructor path = "+path);
    }

    private ReserveSpaceCompanion(
    String path,
    int uid,
    int gid,
    ReserveSpaceCallbacks callbacks,
    long spaceSize,
    String spaceUserHost,
    CellAdapter cell) {
        this.path = path;
        this.uid = uid;
        this.gid = gid;
        this.cell = cell;
        this.spaceSize = spaceSize;
        this.callbacks = callbacks;
        this.spaceUserHost = spaceUserHost;
        say(" constructor path = "+path);
    }

    public void answerArrived( CellMessage req , CellMessage answer ) {
        int current_state = state;
        say("answerArrived, state="+getStateString(current_state));
        request = req;
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if( message instanceof PnfsGetStorageInfoMessage  &&
                current_state == WAITING_PNFS_GET_STORAGE_INFO_RESPONSE_STATE) {
                state=NOT_WAITING_STATE;
                PnfsGetStorageInfoMessage storage_info_msg =
                (PnfsGetStorageInfoMessage)message;
                storageInfoArrived(storage_info_msg);
                return;
            }
            if( message instanceof PnfsGetStorageInfoMessage  &&
                current_state == WAITING_PNFS_GET_PARENT_STORAGE_INFO_RESPONSE_STATE) {
                state=NOT_WAITING_STATE;
                PnfsGetStorageInfoMessage storage_info_msg =
                (PnfsGetStorageInfoMessage)message;
                parentStorageInfoArrived(storage_info_msg);
                return;
            }
            else if( message instanceof PnfsCreateEntryMessage  &&
                current_state == WAITING_PNFS_CREATE_ENTRY_RESPONSE_STATE) {
                state=NOT_WAITING_STATE;
                PnfsCreateEntryMessage create_entry_msg =
                (PnfsCreateEntryMessage)message;
                createEntryArrived(create_entry_msg);
                return;
            }
            else if(message instanceof PoolMgrSelectWritePoolMsg  &&
                current_state == WAITING_POOLMANAGER_RESPONSE_STATE) {
                state=NOT_WAITING_STATE;
                PoolMgrSelectWritePoolMsg msg =
                (PoolMgrSelectWritePoolMsg)message;
                poolManagerSelectPoolMsgArrived(msg);
                return;

            }
            else if(message instanceof PoolReserveSpaceMessage  &&
                current_state == WAITING_POOL_RESPONSE_STATE) {
                state=NOT_WAITING_STATE;
                PoolReserveSpaceMessage msg =
                (PoolReserveSpaceMessage)message;
                poolSpaceReservedArrived(msg);
            }
            else if(message instanceof PnfsDeleteEntryMessage &&
                current_state == WAITING_PNFS_DELETE_ENTRY_RESPONSE_STATE){
                    callbacks.SpaceReserved(pool,totalPoolReservedSpaceSize);
                    return;
            }
            else {
                esay("ignoring unexpected message : "+message);
                //callbacks.ReserveSpaceFailed("unexpected message arrived:"+message);
                return ;
            }
        }
        else {
            esay(" got unknown object. ignoring "+
            " : "+o);
            //callbacks.Error(this.toString ()+" got unknown object "+
            //" : "+o) ;
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

    public void createEntryArrived(PnfsCreateEntryMessage create_entry_msg) {
       say("PnfsGetStorageInfoMessage for root "+
        "directory arrived");
        if(create_entry_msg.getReturnCode() != 0) {
            esay("PnfsCreateEntryMessage message.getReturnCode () != 0");
            reserveSpaceFailed("PnfsCreateEntryMessage message.getReturnCode () != 0"+
            " error object: "+
            create_entry_msg.getErrorObject());
            return ;
        }
        say(" got storage info from create response");
        pnfsId = create_entry_msg.getPnfsId();
        storageInfo =
        create_entry_msg.getStorageInfo();
        say("create_entry_msg = "+create_entry_msg );
        say("storageInfo = "+storageInfo );
        created = true;
        selectPool();
    }


    public void storageInfoArrived(PnfsGetStorageInfoMessage storage_info_msg){
        say("PnfsGetStorageInfoMessage arrived");
        if(storage_info_msg.getReturnCode() != 0) {
            esay("PnfsGetStorageInfoFailed message.getReturnCode () != 0");
            FsPath pnfsPathFile = new FsPath(path);
            pnfsPathFile.add("..");
            String parentPnfsPath =pnfsPathFile.toString();
            state = WAITING_PNFS_GET_PARENT_STORAGE_INFO_RESPONSE_STATE;
            askStorageInfo(parentPnfsPath);
            return ;
        }
        say(" got storage info");
        pnfsId = storage_info_msg.getPnfsId();
        storageInfo =
        storage_info_msg.getStorageInfo();
        say("storage_info_msg = "+storage_info_msg );
        say("storageInfo = "+storageInfo );
        selectPool();
    }

    public void parentStorageInfoArrived(PnfsGetStorageInfoMessage storage_info_msg){
        say("parent PnfsGetStorageInfoMessage arrived");
        if(storage_info_msg.getReturnCode() != 0) {
            esay("parent PnfsGetStorageInfoFailed message.getReturnCode () != 0");
            if(uid == -1) {

                reserveSpaceFailed("parent PnfsGetStorageInfoFailed  message.getReturnCode () != 0"+
                " error object: "+
                storage_info_msg.getErrorObject());
            }
            else
            {
                createPnfsEntry();
            }
            return ;
        }
        say(" got parent storage info");
        pnfsId = storage_info_msg.getPnfsId();
        storageInfo =
        storage_info_msg.getStorageInfo();
        say("storage_info_msg = "+storage_info_msg );
        say("storageInfo = "+storageInfo );
        selectPool();
    }

    public void poolManagerSelectPoolMsgArrived (PoolMgrSelectWritePoolMsg msg)
    {
        say("PoolMgrSelectWritePoolMsg arrived");
        if(msg.getReturnCode() != 0) {
            esay("PoolMgrSelectWritePoolMsg.getReturnCode () != 0:"+msg);
            reserveSpaceFailed(
            "PoolMgrSelectWritePoolMsg failed"+
            "getReturnCode () != 0 => can not find an appropriate pool: "+msg.getErrorObject());
            esay("error object = "+msg.getErrorObject());
            return ;
        }
        say(" got pool info");
        this.pool = msg.getPoolName();

        PoolReserveSpaceMessage reserve =
        new PoolReserveSpaceMessage( pool , spaceSize) ;
        state = WAITING_POOL_RESPONSE_STATE;
        try {
            cell.sendMessage( new CellMessage(
            new CellPath(pool) ,
            reserve ) ,
            true ,
            true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            reserveSpaceFailed(
            "can not contact pool: "+ee.toString());
        }
    }

    public void poolSpaceReservedArrived ( PoolReserveSpaceMessage msg ) {
        say("PoolReserveSpaceMessage arrived");
        if(msg.getReturnCode() != 0) {
            esay("PoolReserveSpaceMessage message.getReturnCode () != 0");
            reserveSpaceFailed(
            "PoolReserveSpaceMessage failed"+
            "getReturnCode () != 0 => can not allocate space");
            return ;
        }
        say(" got pool info");
        if(msg.getSpaceReservationSize() != spaceSize) {
            String error = "PoolReserveSpaceMessage.getSpaceReservationSize() is incorrect "+
            "expected "+spaceSize+" received "+msg.getSpaceReservationSize();
            esay(error);
            reserveSpaceFailed(error);
            return ;
        }
        say("total reserved space on pool="+pool+" is "+
        msg.getReservedSpace());
        totalPoolReservedSpaceSize = msg.getReservedSpace();
        if(created) {
            state = WAITING_PNFS_DELETE_ENTRY_RESPONSE_STATE;
            try {
                deletePnfsEntry(true);
            }
            catch (Exception e)
            {
                esay(e);
                reserveSpaceFailed(e.toString());
            }
            return;
        }
        else
        {
            callbacks.SpaceReserved(pool, msg.getReservedSpace());
            return;
        }
    }

    public static void reserveSpace(
    String path,
    ReserveSpaceCallbacks callbacks,
    long spaceSize,
    String space_user_host,
    CellAdapter cell) {
        cell.say(" ReserveSpaceCompanion.reserveSpace("+path+")");
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(" FileRequest does not specify root path!!!");
        }

        PnfsGetStorageInfoMessage storageInfoMsg =
        new PnfsGetStorageInfoMessage() ;
        storageInfoMsg.setPnfsPath( pnfsPath ) ;
        ReserveSpaceCompanion companion =
        new ReserveSpaceCompanion(
        path,
        callbacks,
        spaceSize,
        space_user_host,
        cell);

        companion.state = WAITING_PNFS_GET_STORAGE_INFO_RESPONSE_STATE;
        try {
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            storageInfoMsg ) ,
            true , true ,
            companion ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            callbacks.ReserveSpaceFailed("can not contact pnfs manger: " +
            ee.toString());
        }
    }

    public void selectPool() {
                // all we need is a pool
        GFtpProtocolInfo protocolInfo =
        new GFtpProtocolInfo(
        "GFtp",//protocol
        1, //major
        0, //minor
        spaceUserHost, //host to be used to determine the pool on basis of network afinity
        0, //port
        0,//start
        0, //min
        0, //max
        0,//bufferSize
        0,//offset
        spaceSize, null) ;//size
        PoolMgrSelectPoolMsg selectPoolMsg =
        new PoolMgrSelectWritePoolMsg(
        pnfsId,
        storageInfo,
        protocolInfo ,
        spaceSize );
        this.state = WAITING_POOLMANAGER_RESPONSE_STATE;
        try {
            cell.sendMessage( new CellMessage(
            new CellPath("PoolManager") ,
            selectPoolMsg ) ,
            true , true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            reserveSpaceFailed(
            "can not contact pool manger: "+ee.toString());
        }

    }

    public static void reserveSpace(
    String path,
    StorageInfo storageInfo,
    PnfsId pnfsId,
    ReserveSpaceCallbacks callbacks,
    long spaceSize,
    String space_user_host,
    CellAdapter cell) {
        cell.say(" ReserveSpaceCompanion.reserveSpace("+path+")");
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(" FileRequest does not specify root path!!!");
        }

        ReserveSpaceCompanion companion =
        new ReserveSpaceCompanion(
        path,
        storageInfo,
        pnfsId,
        callbacks,
        spaceSize,
        space_user_host,
        cell);
        companion.selectPool();
    }

    public static void reserveSpace(
    String path,
    int uid,
    int gid,
    ReserveSpaceCallbacks callbacks,
    long spaceSize,
    String space_user_host,
    CellAdapter cell) {
        cell.say(" ReserveSpaceCompanion.reserveSpace("+path+")");
        FsPath pnfsPathFile = new FsPath(path);
        String pnfsPath = pnfsPathFile.toString();
        if(pnfsPath == null) {
            throw new IllegalArgumentException(" FileRequest does not specify root path!!!");
        }
        ReserveSpaceCompanion companion =
        new ReserveSpaceCompanion(
        path,
        uid,
        gid,
        callbacks,
        spaceSize,
        space_user_host,
        cell);
        companion.state= WAITING_PNFS_GET_STORAGE_INFO_RESPONSE_STATE;
        companion.askStorageInfo(pnfsPath);

    }

    private void askStorageInfo(String pnfsPath) {
        PnfsGetStorageInfoMessage storageInfoMsg =
        new PnfsGetStorageInfoMessage() ;
        storageInfoMsg.setPnfsPath( pnfsPath ) ;

        try {

            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            storageInfoMsg ) ,
            true , true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            callbacks.ReserveSpaceFailed("can not contact pnfs manger: "+ee.toString());
        }

    }

    private void createPnfsEntry() {
        PnfsCreateEntryMessage createPnfsEntry =
            new PnfsCreateEntryMessage( path , uid , gid , 0644 );
        state = WAITING_PNFS_CREATE_ENTRY_RESPONSE_STATE;
         try {
            cell.sendMessage( new CellMessage(
            new CellPath("PnfsManager") ,
            createPnfsEntry ) ,
            true , true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            reserveSpaceFailed("can not contact pnfs manger: "+ee.toString());
        }

    }

    private void deletePnfsEntry(boolean replyRequired) throws Exception {
        say("deletePnfsEntry");
        if(created) {
            created = false;
            PnfsDeleteEntryMessage deletePnfsEntry =
                new PnfsDeleteEntryMessage(pnfsId );
                 deletePnfsEntry.setReplyRequired(replyRequired);
            CellMessage cellMessage =   new CellMessage(
            new CellPath("PnfsManager") ,
            deletePnfsEntry );
            if(replyRequired){
                cell.sendMessage( cellMessage ,
                    true , true ,
                    this ,
                    1*24*60*60*1000) ;
            } else {
                cell.sendMessage(cellMessage );
            }
        }

    }

    private void reserveSpaceFailed(String error) {
        callbacks.ReserveSpaceFailed(
        error);
        if(created) {
            try {
                deletePnfsEntry(false);
            }
            catch(Exception ee ) {
                esay(ee);
            }
        }
    }
}

