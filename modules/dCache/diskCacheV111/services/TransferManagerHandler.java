//______________________________________________________________________________
//
// $id: TransferManagerHandler.java,v 1.4 2006/05/18 20:16:09 litvinse Exp $
// $Author: litvinse $
//
// created 05/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package diskCacheV111.services;

import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.FsPath;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.DoorMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferManagerMessage;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferFailedMessage;
import diskCacheV111.vehicles.transferManager.TransferCompleteMessage;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.spaceManager.SpaceManagerGetInfoAndLockReservationMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerUtilizedSpaceMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerUnlockSpaceMessage;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import diskCacheV111.doors.FTPTransactionLog;
import javax.jdo.JDOHelper;

public class TransferManagerHandler implements CellMessageAnswerable { 
	private TransferManager manager;
	private TransferManagerMessage transferRequest;
	private CellPath sourcePath;
	private int uid;
	private int gid;
	private String pnfsPath;
	private transient String parentDir;
	boolean store;
	boolean created = false;
	private PnfsId          pnfsId;
	private String          pnfsIdString;
	private String          remoteUrl;
	private StorageInfo     storageInfo;
	transient boolean locked = false;
	private String pool;
	private FTPTransactionLog tlog;
	private diskCacheV111.util.FileMetaData metadata;
	public static final int INITIAL_STATE=0;
	public static final int WAITING_FOR_PNFS_INFO_STATE=1;
	public static final int RECEIVED_PNFS_INFO_STATE=2;
	public static final int WAITING_FOR_PNFS_PARENT_INFO_STATE=3;
	public static final int RECEIVED_PNFS_PARENT_INFO_STATE=4;
	public static final int WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE=5;
	public static final int RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE=6;
	public static final int WAITING_FOR_POOL_INFO_STATE=7;
	public static final int RECEIVED_POOL_INFO_STATE=8;
	public static final int WAITING_FIRST_POOL_REPLY_STATE=9;
	public static final int RECEIVED_FIRST_POOL_REPLY_STATE=10;
	public static final int WAITING_FOR_SPACE_INFO_STATE=11;
	public static final int RECEIVED_SPACE_INFO_STATE=12;
	public static final int SENT_ERROR_REPLY_STATE=-1;
	public static final int SENT_SUCCESS_REPLY_STATE=-2;
	public int state = INITIAL_STATE;
	private long id;
	private Integer moverId;
	private IpProtocolInfo protocol_info;
	private String spaceReservationId;
	private transient Long size;
	private transient SpaceManagerGetInfoAndLockReservationMessage spaceInfo;
	private transient boolean space_reservation_strict;
	private long creationTime;
	private long lifeTime;
        private Long credentialId;
	
	private TransferManagerHandler() { 
	}
/**      */        
	public TransferManagerHandler(TransferManager tManager, 
				      TransferManagerMessage message,
				      CellPath sourcePath)  {
		creationTime = System.currentTimeMillis();
		manager      = tManager;
		id           = manager.getNextMessageID();
		message.setId(id);
		this.transferRequest = message;
		Long longId          = new Long(id);
                
		uid      = transferRequest.getUid();
		gid      = transferRequest.getGid();
		pnfsPath = transferRequest.getPnfsPath();
		store    = transferRequest.isStore();
		remoteUrl= transferRequest.getRemoteURL();
                credentialId = transferRequest.getCredentialId();
		this.sourcePath = sourcePath;
		try {
			if(manager.getLogRootName() != null) {
				tlog = new FTPTransactionLog(manager.getLogRootName(),manager);
				String user_info = transferRequest.getUser()+
					"("+uid +"."+gid+")";
				String rw = store?"write":"read";
				java.net.InetAddress remoteaddr = 
					java.net.InetAddress.getByName(
						new org.globus.util.GlobusURL(transferRequest.getRemoteURL()).getHost());
				tlog.begin(user_info, "remotegsiftp", rw, transferRequest.getPnfsPath(), remoteaddr);
			}
		}
		catch(Exception e) {
			esay("starting tlog failed :");
			esay(e);
		}
		this.spaceReservationId       = transferRequest.getSpaceReservationId();
		this.space_reservation_strict = transferRequest.isSpaceReservationStrict();
		this.size                     = transferRequest.getSize();
		synchronized(manager.activeTransfersIDs) {
			manager.addActiveTransfer(longId,this);
		}
		setState(INITIAL_STATE);
	}
/**      */        
	public void say(String s) {
		manager.say("["+toString()+"]:"+s);
	}
/**      */        
	public void esay(String s) {
		manager.esay("["+toString()+"]:"+s);
	}
/**      */        
	public void esay(Throwable t){
		manager.esay(t);
	}
/**      */        
	public void handle() {
		say("handling:  "+toString(true));
		int last_slash_pos = pnfsPath.lastIndexOf('/');
		if(last_slash_pos == -1) {
			transferRequest.setFailed(2,  
						  new java.io.IOException("pnfsFilePath is not absolute:"+pnfsPath));
			return;
		}
		parentDir = pnfsPath.substring(0,last_slash_pos);
		PnfsGetFileMetaDataMessage sInfo;
		if(store) {
			sInfo = new PnfsGetFileMetaDataMessage() ;
			sInfo.setPnfsPath( parentDir ) ;
			setState(WAITING_FOR_PNFS_PARENT_INFO_STATE);
		} 
		else {
			sInfo = new PnfsGetStorageInfoMessage() ;
			sInfo.setPnfsPath( pnfsPath ) ;
			setState(WAITING_FOR_PNFS_INFO_STATE);
		}
		manager.persist(this);
		try {
			manager.sendMessage(
				new CellMessage(new CellPath(manager.getPnfsManagerName()),
						sInfo ),
				true , 
				true,
				this,
				manager.getPnfsManagerTimeout()*1000
				);
		}
		catch(Exception ee ) {
			esay(ee);
			//we do not need to send the new message 
			// since the original reply has not been sent yet
			transferRequest.setFailed(2, ee);
			return ;
		}
	}
/**      */
	public void answerArrived(CellMessage req, CellMessage answer) {
		say("answerArrived("+req+","+answer+"), state ="+state);
		Object o = answer.getMessageObject();
		if(o instanceof Message) {
			Message message = (Message)answer.getMessageObject() ;
			if ( message instanceof PnfsCreateEntryMessage) {
				PnfsCreateEntryMessage  create_msg =
					(PnfsCreateEntryMessage)message;
				if( state == WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE) {
					setState(RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE);
					createEntryResponceArrived(create_msg);
					return;
				}
				esay(this.toString()+" got unexpected PnfsCreateEntryMessage "+
				     " : "+create_msg+" ; Ignoring");
			} 
			else     if( message instanceof PnfsGetStorageInfoMessage) {
				PnfsGetStorageInfoMessage storage_info_msg =
					(PnfsGetStorageInfoMessage)message;
				if( state == WAITING_FOR_PNFS_INFO_STATE ) {
					setState(RECEIVED_PNFS_INFO_STATE);
					storageInfoArrived(storage_info_msg);
					return;
				}
				esay(this.toString()+" got unexpected PnfsGetStorageInfoMessage "+
				     " : "+storage_info_msg+" ; Ignoring");
			} 
			else     if( message instanceof PnfsGetFileMetaDataMessage) {
				PnfsGetFileMetaDataMessage storage_metadata =
					(PnfsGetFileMetaDataMessage)message;
				if(state == WAITING_FOR_PNFS_PARENT_INFO_STATE) {
					setState(RECEIVED_PNFS_PARENT_INFO_STATE);
					parentInfoArrived(storage_metadata);
					return;
				}
				esay(this.toString()+" got unexpected PnfsGetFileMetaDataMessage "+
				     " : "+storage_metadata+" ; Ignoring");
			}            
			else if(message instanceof PoolMgrSelectPoolMsg) {
				PoolMgrSelectPoolMsg select_pool_msg =
					(PoolMgrSelectPoolMsg)message;
				if( state == WAITING_FOR_POOL_INFO_STATE) {
					setState(RECEIVED_POOL_INFO_STATE);
					poolInfoArrived(select_pool_msg);
					return;
				}
				esay(this.toString()+" got unexpected PoolMgrSelectPoolMsg "+
				     " : "+select_pool_msg+" ; Ignoring");
			}
			else if(message instanceof PoolIoFileMessage) {
				PoolIoFileMessage first_pool_reply = 
					(PoolIoFileMessage)message;
				if( state == WAITING_FIRST_POOL_REPLY_STATE) {
					setState(RECEIVED_FIRST_POOL_REPLY_STATE);
					poolFirstReplyArrived(first_pool_reply);
					return;
				}
				esay(this.toString()+" got unexpected PoolIoFileMessage "+
				     " : "+first_pool_reply+" ; Ignoring");
			} 
			else if (message instanceof SpaceManagerGetInfoAndLockReservationMessage) {
				SpaceManagerGetInfoAndLockReservationMessage spaceInfo= 
					(SpaceManagerGetInfoAndLockReservationMessage)message;
				if( state == WAITING_FOR_SPACE_INFO_STATE) {
					setState(RECEIVED_SPACE_INFO_STATE);
					spaceInfoArrived(spaceInfo);
					return;
				}
				esay(this.toString()+" got unexpected SpaceManagerGetInfoAndLockReservationMessage "+
				     " : "+spaceInfo+" ; Ignoring");
			}
		}
		manager.persist(this);
	}
/**      */        
	public void answerTimedOut(CellMessage request) {
	}
/**      */	   
	public void exceptionArrived(CellMessage request, Exception exception) {
	}
/**      */        
	public void parentInfoArrived(PnfsGetFileMetaDataMessage file_metadata) {
		say("parentInfoArrived(TransferManagerHandler)");
		if(file_metadata.getReturnCode() != 0) {
			sendErrorReply(3,  new java.io.IOException(
					       "can't get metadata for parent directory "+parentDir));
			return;
		}
		diskCacheV111.util.FileMetaData metadata = 
			file_metadata.getMetaData();
		boolean can_write = (metadata.getUid() == uid) &&
			metadata.getUserPermissions().canWrite() &&
			metadata.getUserPermissions().canExecute();
		    
		can_write |= (metadata.getGid() == gid ) &&
			metadata.getGroupPermissions().canWrite() &&
			metadata.getGroupPermissions().canExecute();
			
		can_write |= metadata.getWorldPermissions().canWrite() &&
			metadata.getWorldPermissions().canExecute();
		if(!can_write) {
			sendErrorReply(3,  new java.io.IOException(
					       "user has no permission to write to directory"+parentDir));
			return;
		}
		PnfsCreateEntryMessage create = new PnfsCreateEntryMessage( pnfsPath , uid , gid , 0644 ) ;
		setState(WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE);
		manager.persist(this);
		try {
			manager.sendMessage(new CellMessage(new CellPath( manager.getPnfsManagerName()),
						    create ) ,
				    true , 
				    true,
				    this,
				    manager.getPnfsManagerTimeout()*1000
				);
		}
		catch(Exception ee ) {
			esay(ee);
			sendErrorReply(4,ee);
			return ;
		}
	}
/**      */
	public void spaceInfoArrived(SpaceManagerGetInfoAndLockReservationMessage spaceInfo) {
		say("spaceInfo = "+spaceInfo);
		if( spaceInfo.getReturnCode() != 0 ) {
			sendErrorReply(5, new
				       CacheException( "Space Manager error: "+
						       spaceInfo.getErrorObject() ) );
			return;
		}
		this.spaceInfo = spaceInfo;
		this.locked    = true;
		pool           = spaceInfo.getPool();
		this.state     = WAITING_FOR_PNFS_INFO_STATE;
		manager.persist(this);
		say("Positive reply from Space Manager: space reserved at "+pool);
		PnfsGetStorageInfoMessage sInfo = new PnfsGetStorageInfoMessage() ;
		sInfo.setPnfsPath( pnfsPath ) ;
		manager.persist(this);
		try {
			manager.sendMessage(new CellMessage(new CellPath(manager.getPnfsManagerName()),
						    sInfo),
				    true, 
				    true,
				    this,
				    manager.getPnfsManagerTimeout()*1000
				);
		}
		catch(Exception ee ) {
			esay(ee);
			transferRequest.setFailed(2, ee);
			return ;
		}
		return ;
	}
/**      */	    
	public void createEntryResponceArrived(PnfsCreateEntryMessage create) {
		if(create.getReturnCode() == 0) {
			created = true;
			manager.persist(this);
		}
		else {
			sendErrorReply(5, "failed to create pnfs entry: "+create.getErrorObject());
			return;
		}
		if(spaceReservationId != null){
			SpaceManagerGetInfoAndLockReservationMessage   spaceReservationInfo = 
				new SpaceManagerGetInfoAndLockReservationMessage(Long.parseLong(spaceReservationId));
			setState(WAITING_FOR_SPACE_INFO_STATE);
			manager.persist(this);
			try {
				manager.sendMessage(new CellMessage(new CellPath(manager.getSpaceManagerName()),
							    spaceReservationInfo ) ,
					    true , 
					    true,
					    this,
					    manager.getSpaceManagerTimeout()*1000
					);
				return;
			}
			catch(Exception ee ) {
				esay(ee);
				sendErrorReply(4,ee);
				return ;
			}
		}
		PnfsGetStorageInfoMessage sInfo = new PnfsGetStorageInfoMessage() ;
		sInfo.setPnfsPath( pnfsPath ) ;
		setState(WAITING_FOR_PNFS_INFO_STATE);
		manager.persist(this);
		try {
			manager.sendMessage(new CellMessage(new CellPath(manager.getPnfsManagerName()),
						    sInfo),
				    true, 
				    true,
				    this,
				    manager.getPnfsManagerTimeout()*1000
				);
		}
		catch(Exception ee ) {
			esay(ee);
			transferRequest.setFailed(2, ee);
			return ;
		}
	}
/**      */        
	public void storageInfoArrived( PnfsGetStorageInfoMessage storage_info_msg){
		if( storage_info_msg.getReturnCode() != 0 ) {
			sendErrorReply(6, new
				       CacheException( "cant get storage info for file "+pnfsPath+" : "+
						       storage_info_msg.getErrorObject() ) );
			return;
		}
		if(!store && tlog != null) {
			tlog.middle(storage_info_msg.getStorageInfo().getFileSize());
		}
		//
		// Added by litvinse@fnal.gov
		//
		pnfsId        = storage_info_msg.getPnfsId();
		pnfsIdString  = pnfsId.toString();
		manager.persist(this);
		if ( store ) { 
			synchronized(manager.justRequestedIDs) {
				if (manager.justRequestedIDs.contains(storage_info_msg.getPnfsId())) { 
					sendErrorReply(6, new
						       CacheException( "pnfs pnfsid: "+pnfsId.toString()+" file "+pnfsPath+"  is already there"));
					return;
				}
				Iterator iter = manager.justRequestedIDs.iterator();
				while(iter.hasNext()) {
					PnfsId pnfsid = (PnfsId)iter.next();
					say("found pnfsid: "+pnfsid.toString());
				}
				manager.justRequestedIDs.add(pnfsId);
			}
		}
		storageInfo  = storage_info_msg.getStorageInfo();
		metadata = 
			storage_info_msg.getMetaData();
		say("storageInfoArrived(uid="+uid+" gid="+gid+" pnfsid="+pnfsId+" storageInfo="+storageInfo+" metadata="+metadata);
		if(store) {
			boolean can_write = (metadata.getUid() == uid) &&
				metadata.getUserPermissions().canWrite() ;
			//say("user can write="+can_write);
			can_write |= (metadata.getGid() == gid ) &&
				metadata.getGroupPermissions().canWrite();
			//say("user/group can write="+can_write);
			can_write |= metadata.getWorldPermissions().canWrite();
			//say("user/group/world can write="+can_write);
			if(!can_write) {
				sendErrorReply(3,  new java.io.IOException(
						       "user has no permission to write to file"+pnfsPath));
				return;
			}
			if(metadata.getFileSize() != 0 && !manager.isOverwrite()) {
				sendErrorReply(3,  new java.io.IOException(
						       "file size is not 0, user has no permission to write to file"+pnfsPath));
				return;
					
			}
		} 
		else {
			boolean can_read = (metadata.getUid() == uid) &&
				metadata.getUserPermissions().canRead();
			can_read |= (metadata.getGid() == gid ) &&
				metadata.getGroupPermissions().canRead();
			can_read |= metadata.getWorldPermissions().canRead();
			if(!can_read) {
				sendErrorReply(3,  new java.io.IOException(
						       "user has no permission to read file "+pnfsPath));
				return;
			}
		}
		try {
			protocol_info = manager.getProtocolInfo(getId(),transferRequest);
		}
		catch(IOException ioe) {
			esay(ioe);
			//we do not need to send the new message 
			// since the original reply has not been sent yet
			sendErrorReply(4,ioe);
			return ;
		}
		if(store && spaceInfo != null){
			startMoverOnThePool();
			return;
		}
		Thread current = Thread.currentThread();
		PoolMgrSelectPoolMsg request =
			store ?
			(PoolMgrSelectPoolMsg)
			new PoolMgrSelectWritePoolMsg(
				pnfsId,
				storageInfo,
				protocol_info ,
				0L                 )
			:
			(PoolMgrSelectPoolMsg)
			new PoolMgrSelectReadPoolMsg(
				pnfsId  ,
				storageInfo,
				protocol_info ,
				0L                 );
		say("PoolMgrSelectPoolMsg: " + request );
		setState(WAITING_FOR_POOL_INFO_STATE);
		manager.persist(this);
		try {
			manager.sendMessage(new CellMessage(manager.getPoolManagerPath(),
						    request),
				    true, 
				    true,
				    this,
				    manager.getPoolManagerTimeout()*1000
				);
		}
		catch(Exception e ) {
			esay(e);
			sendErrorReply(4,e);
			return ;
		}
	}
/**      */	    
	public void poolInfoArrived(PoolMgrSelectPoolMsg pool_info)  {
		say("poolManagerReply = "+pool_info);
		if( pool_info.getReturnCode() != 0 ) {
			sendErrorReply(5, new
				       CacheException( "Pool manager error: "+
						       pool_info.getErrorObject() ) );
			return;
		}
		setPool(pool_info.getPoolName());
		manager.persist(this);
		say("Positive reply from pool "+pool);
		startMoverOnThePool();
	}
/**      */         
	public void startMoverOnThePool() {
		if(store && spaceInfo != null) {
			storageInfo.setKey("use-preallocated-space",
					   Long.toString(spaceInfo.getAvailableLockedSize()));
			esay("setting storage info key use-preallocated-space to "+
			     spaceInfo.getAvailableLockedSize());
			if(space_reservation_strict) {
				esay("setting storage info key use-max-space to "+
				     spaceInfo.getAvailableLockedSize());
				storageInfo.setKey("use-max-space",
						   Long.toString(spaceInfo.getAvailableLockedSize()));
			}
				
		}
		PoolIoFileMessage poolMessage = store ?
			(PoolIoFileMessage)
			new PoolAcceptFileMessage(
				pool,
				pnfsId.toString() ,
				protocol_info ,
				storageInfo     ):
			(PoolIoFileMessage)
			new PoolDeliverFileMessage(
				pool,
				pnfsId.toString() ,
				protocol_info ,
				storageInfo     );

		if( manager.getIoQueueName() != null ) { 
			poolMessage.setIoQueueName(manager.getIoQueueName());
		}

		poolMessage.setId( id ) ;
		setState(WAITING_FIRST_POOL_REPLY_STATE);
		manager.persist(this);
		try {
			manager.sendMessage(new CellMessage(new CellPath(pool),
						    poolMessage),
				    true, 
				    true,
				    this,
				    manager.getPoolTimeout()*1000
				);
		}
		catch(Exception ee ) {
			esay(ee);
			sendErrorReply(4,ee);
			return ;
		}
		return ;
	}
/**      */	    
	public void poolFirstReplyArrived(PoolIoFileMessage poolMessage)  {
		say("poolReply = "+poolMessage);
		if( poolMessage.getReturnCode() != 0 ) {
			sendErrorReply(5, new
				       CacheException( "Pool error: "+
						       poolMessage.getErrorObject() ) );
			return;
		}
		say("Pool "+pool+" will deliver file "+pnfsId +" mover id is "+poolMessage.getMoverId());
		say("Starting moverTimeout timer");
		manager.startTimer(id);
		setMoverId(new Integer(poolMessage.getMoverId()));
		manager.persist(this);

	}
/**      */         
	public void releaseSpace() {
		if(spaceInfo != null) {
			say("releaseSpace() space token="+spaceInfo.getSpaceToken()+
			    " size= "+spaceInfo.getAvailableLockedSize());
			SpaceManagerUnlockSpaceMessage unlockSpace = 
				new SpaceManagerUnlockSpaceMessage(spaceInfo.getSpaceToken(),
								   spaceInfo.getAvailableLockedSize());
			try {
				manager.sendMessage(new CellMessage(new CellPath("SpaceManager") ,
							    unlockSpace
						    ));
			}
			catch (Exception e) {
				String errmsg = "Can't send message to SpaceManager "+e;
				esay(errmsg);
				esay(e) ;
			}
			spaceInfo = null;
		}
		else {
			say("releaseSpace() space info is null, do nothing");
		}
	}
/**      */        
	public void poolDoorMessageArrived(DoorTransferFinishedMessage doorMessage) {
		say("poolDoorMessageArrived, doorMessage.getReturnCode()="+doorMessage.getReturnCode());
		if(doorMessage.getReturnCode() != 0 ) {
			releaseSpace();
			sendErrorReply(8,"tranfer failed :"+doorMessage.getErrorObject());
			return;
		}
		if(store && spaceInfo != null) {
			long utilized = doorMessage.getStorageInfo().getFileSize();
			say("reply.getStorageInfo().getFileSize()="+utilized);
			if(utilized > spaceInfo.getAvailableLockedSize()) {
				utilized = spaceInfo.getAvailableLockedSize();
			}
			say("set utilized to "+utilized);
			SpaceManagerUtilizedSpaceMessage utilizedSpace = 
				new SpaceManagerUtilizedSpaceMessage(spaceInfo.getSpaceToken(),utilized);
			try {
				manager.sendMessage(new CellMessage(
						    new CellPath("SpaceManager") ,
						    utilizedSpace
						    ));
			}
			catch (Exception e) {
				String errmsg = "Can't send message to SpaceManager "+e;
				esay(errmsg);
				esay(e) ;
			}
			spaceInfo = null;
		}
		DoorTransferFinishedMessage finished = (DoorTransferFinishedMessage) doorMessage;
		if(store && tlog != null) {
			tlog.middle(finished.getStorageInfo().getFileSize());
		}
		sendSuccessReply();
	}
/**      */         
	public void sendErrorReply(int replyCode, 
				    Object errorObject) {
		sendErrorReply(replyCode,errorObject,true);
	}
/**      */         
	public void sendErrorReply(int replyCode, 
				    Object errorObject, 
				    boolean cancelTimer) {
		esay("sending error reply, reply code="+replyCode+" errorObject="+errorObject+" for "+toString(true));
			
		if(tlog != null) {
			tlog.error("getFromRemoteGsiftpUrl failed: state = "+state+
				   " replyCode="+replyCode+" errorObject="+
				   errorObject);
		}

		setState(SENT_ERROR_REPLY_STATE,errorObject);
		manager.persist(this);

		if(cancelTimer) {
			manager.stopTimer(id);
		}
		


		if ( store ) { 
			synchronized(manager.justRequestedIDs) {
				manager.justRequestedIDs.remove(pnfsId);
			}
		}
		manager.finish_transfer();
		if(store && created && pnfsId != null && metadata != null && metadata.getFileSize() == 0) {
			// if we created a pnfs entry and failed to write into it,
			// we should clean up after ourselves
			esay(" we created the pnfs entry and the store failed: deleting "+pnfsPath);
			PnfsDeleteEntryMessage pnfsMsg = new PnfsDeleteEntryMessage(pnfsPath);
			pnfsMsg.setReplyRequired(false);
			try { 
				manager.sendMessage(new CellMessage(new CellPath(manager.getPnfsManagerName()), pnfsMsg));
			}
			catch (Exception e) {
				esay("sendErrorReply: can not send PnfsDeleteEntryMessage:");
				esay(e);
			}
		}
		if(store && locked && spaceInfo != null) {
			esay(" we locked the space in spaceManager, unlocking: ");
			releaseSpace();
		}
		try {
			TransferFailedMessage errorReply = new TransferFailedMessage(transferRequest,replyCode, errorObject);
			manager.sendMessage(new CellMessage(sourcePath,errorReply));
		}
		catch(Exception e) {
			esay(e);
			//can not do much more here!!!
		}
		Long longId = new Long(id);
		//this will allow the handler to be garbage collected
		// once we sent a response
		synchronized(manager.activeTransfersIDs) {
			manager.removeActiveTransfer(longId);
		}
	}
/**      */        
	public void sendSuccessReply() {
		say("sendSuccessReply for: "+toString(true));
		setState(SENT_SUCCESS_REPLY_STATE);
		manager.persist(this);
		manager.stopTimer(id);
		if ( store ) { 
			synchronized(manager.justRequestedIDs) {
				manager.justRequestedIDs.remove(pnfsId);
			}
		}
		manager.finish_transfer();
		if(tlog != null) {
			tlog.success();
		}
		try {
			TransferCompleteMessage errorReply = new TransferCompleteMessage(transferRequest);
			manager.sendMessage(new CellMessage(sourcePath,errorReply));
		}
		catch(Exception e)  {
			esay(e);
			//can not do much more here!!!
		}
		Long longId = new Long(id);
		//this will allow the handler to be garbage collected
		// once we sent a response
		synchronized(manager.activeTransfersIDs) {
			manager.removeActiveTransfer(longId);
		}
	}
/**      */	    
	public CellPath getRequestSourcePath() {
		return sourcePath;
	}
/**      */        
	public void cancel( ) {
		esay("cancel");
		if(moverId != null) {
			killMover(this.pool,moverId.intValue());
		}
		releaseSpace();
		sendErrorReply(24, new java.io.IOException("canceled"));
	}
/**      */
	public void timeout( ) {
		esay("timeout");
		if(moverId != null) {
			killMover(this.pool,moverId.intValue());
		}
		releaseSpace();
		sendErrorReply(24, new java.io.IOException("timed out while waiting for mover reply"),false);
	}
/**      */        
	public void cancel(CancelTransferMessage cancel ) {
		esay("cancel");
		if(moverId != null) {
			killMover(this.pool,moverId.intValue());
		}
		releaseSpace();
		sendErrorReply(24, new java.io.IOException("canceled"));
	}
/**      */        
	public synchronized String toString(boolean long_format) {
		StringBuffer sb = new StringBuffer("id=");
		sb.append(id);
		if(store) {
			sb.append(" store src=");
			sb.append(transferRequest.getRemoteURL());
			sb.append(" dest=");
			sb.append(transferRequest.getPnfsPath());
		}
		else {
			sb.append("restore src=");
			sb.append(transferRequest.getPnfsPath());
			sb.append(" dest=");
			sb.append(transferRequest.getRemoteURL());
		}
		if(!long_format) {
			return sb.toString();
		}
		sb.append("\n   state").append(state);
		sb.append("\n    uid=").append(uid);
		sb.append(" gid=").append(gid);
		if(pnfsId != null) {
			sb.append("\n   pnfsId=").append(pnfsId);
		}
		if(storageInfo != null) {
			sb.append("\n  storageInfo=").append(storageInfo);
		}
		if(pool != null) {
			sb.append("\n   pool=").append(pool);
			if(moverId != null) {
				sb.append("\n   moverId=").append(moverId);
			}
		}
		return sb.toString();
	}
/**      */	    
	public String toString() {
		return toString(false);
	}
		
/**      */
	public java.lang.String getPool() {
		return pool;
	}
/**      */
	public void setPool(java.lang.String pool) {
		this.pool = pool;
	}

	public void killMover(String pool,int moverId) {
		esay("sending mover kill to pool "+pool+" for moverId="+moverId );
		PoolMoverKillMessage killMessage = new PoolMoverKillMessage(pool,moverId);
		killMessage.setReplyRequired(false);
		try {
			manager.sendMessage( new CellMessage( new CellPath (  pool), killMessage )  );
		}
		catch(Exception e) {
			esay(e);
		}
	}

	public void setState(int istate) { 
		this.state = istate;
		TransferManagerHandlerState ts = new TransferManagerHandlerState(this,null);
		manager.persist(ts);
	}


	public void setState(int istate, Object errorObject) { 
		this.state = istate;
		TransferManagerHandlerState ts = new TransferManagerHandlerState(this,errorObject);
		manager.persist(ts);
	}

	public void setMoverId(Integer moverid) { 
		moverId = moverid;
	}
        
        public static final void main(String[] args) {
            System.out.println("This is a main in handler");
        }

	public int getUid() { return uid; }
	public int getGid() { return gid; }
	public String getPnfsPath() { return pnfsPath; } 
	public boolean getStore() { return store; } 
	public boolean getCreated() { return created; } 
	public boolean getLocked() { return locked; } 
	public String getPnfsIdString() { return pnfsIdString; } 
	public String getRemoteUrl() { return remoteUrl; } 
	public int getState() { return state; } 
	public long getId() { return id; } 
	public Integer getMoverId() { return moverId; } 
	public String getSpaceReservationId () { return spaceReservationId; } 
	public long getCreationTime() { return creationTime; } 
	public long getLifeTime() { return lifeTime; } 
	public Long getCredentialId() { return credentialId; }
}
