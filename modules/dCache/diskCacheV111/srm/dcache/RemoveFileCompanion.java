//______________________________________________________________________________
//
// $Id: RemoveFileCompanion.java,v 1.1.8.1 2006-09-20 22:05:28 timur Exp $
// $Author: timur $ 
// 
// created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________
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
 * RemoveFileCompanion.java
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
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolModifyPersistencyMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import java.net.InetAddress;
import java.util.Vector;
import org.dcache.srm.RemoveFileCallbacks;
import org.dcache.srm.FileMetaData;


public class RemoveFileCompanion implements CellMessageAnswerable {
	private static final int INITIAL_STATE=0;
	private static final int WAITING_FOR_FILE_INFO_MESSAGE=1;
	private static final int RESEIVED_FILE_INFO_MESSAGE=2;
	private static final int WAITING_FOR_CACHE_LOCATIONS_MESSAGE=3;
	private static final int RESEIVED_CACHE_LOCATIONS_MESSAGE=4;
	private static final int WAITING_FOR_PNFS_DELETE_MESSAGE=5;
	private static final int RESEIVED_PNFS_DELETE_MESSAGE=6;
	private static final int WAITING_FOR_PNFS_AND_POOL_REPLIES_MESSAGES=7;
	private static final int FINAL_STATE=8;
	private volatile int state = INITIAL_STATE;
	private dmg.cells.nucleus.CellAdapter cell;
	private RemoveFileCallbacks callbacks;
	private CellMessage request = null;
	private String path;
	private PnfsId pnfsId;
	private StorageInfo storageInfo;
	private String      poolName = null ;
	private DCacheUser user;
	
	private void say(String words_of_wisdom) {
		if (cell!=null) {
			cell.say(toString()+words_of_wisdom);
		}
	}
	private void esay(String words_of_despare) {
		if (cell!=null) {
			cell.esay(toString()+words_of_despare);
			}
	}
	
	private void esay(Throwable t) {
		if(cell!=null) {
			cell.esay(" RemoveFileCompanion exception : ");
			cell.esay(t);
		}
	}
	
	private String name() { 
		String tmp = this.getClass().getName();
		return tmp.substring(tmp.lastIndexOf('.'),tmp.length()-1);
	}
    
	private RemoveFileCompanion(DCacheUser user,
				    String path,
				    RemoveFileCallbacks callbacks,
				    CellAdapter cell) { 
		this.user = user;
		this.path = path;
		this.cell = cell;
		this.callbacks = callbacks;
	}
	
	public static void removeFile(DCacheUser user,
				      String path,
				      RemoveFileCallbacks callbacks,
				      CellAdapter cell, 
				      boolean do_remove) { 

		FsPath pnfsPathFile = new FsPath(path);
		String pnfsPath     = pnfsPathFile.toString();
		PnfsGetStorageInfoMessage storageInfoMsg = new PnfsGetStorageInfoMessage();
		storageInfoMsg.setPnfsPath(pnfsPath);
		RemoveFileCompanion companion = new RemoveFileCompanion(user,
									path,
									callbacks,
									cell);
		companion.state = WAITING_FOR_FILE_INFO_MESSAGE;
		try {
			cell.sendMessage(new CellMessage(
						 new CellPath("PnfsManager") ,
						 storageInfoMsg ) ,
					 true , true ,
					 companion ,
					 1*24*60*60*1000) ;
		}
		catch (Exception ee ) {
			cell.esay(ee);
			callbacks.RemoveFileFailed("Exception");
		}
	}
	
    public void answerArrived( final CellMessage req , final CellMessage answer ) {
        say("answerArrived");
        diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessage(req,answer);
            }
        });
    }
    
    public void processMessage(CellMessage req, 
				  CellMessage answer ) {
		if (state == FINAL_STATE) {
			say(name()+" answerArrived("+req+","+answer+"), state is final, ignore all messages");
			return;
		}
		say(name()+" answerArrived("+req+","+answer+"), state ="+state);
		request  = req;
		Object o = answer.getMessageObject();
		if (o instanceof Message) {
			Message message = (Message)answer.getMessageObject();
			if (message instanceof PnfsGetStorageInfoMessage ) {
				PnfsGetStorageInfoMessage storage_info_msg =
					(PnfsGetStorageInfoMessage)message;
				if (state == WAITING_FOR_FILE_INFO_MESSAGE) {
					state = RESEIVED_FILE_INFO_MESSAGE;
					fileInfoArrived(storage_info_msg);
					return;
				}
				esay(this.toString()+" got unexpected PnfsGetStorageInfoMessage "+
				     " : "+storage_info_msg+" ; Ignoring");
			}
			else if (message instanceof PnfsGetCacheLocationsMessage ) {
				PnfsGetCacheLocationsMessage get_cache_loc_msg =
					(PnfsGetCacheLocationsMessage) message;
				if (state == WAITING_FOR_CACHE_LOCATIONS_MESSAGE) {
					state = RESEIVED_CACHE_LOCATIONS_MESSAGE;
					cacheLocationsArrived(get_cache_loc_msg);
					return;
				}
				esay(this.toString()+" got unexpected PnfsGetCacheLocationsMessage "+
				     " : "+get_cache_loc_msg+" ; Ignoring");
			}
			else if (message instanceof PnfsDeleteEntryMessage ) {
				PnfsDeleteEntryMessage delete_reply =
					(PnfsDeleteEntryMessage) message;
				if(state == WAITING_FOR_PNFS_DELETE_MESSAGE) {
					state = RESEIVED_PNFS_DELETE_MESSAGE;
					if(delete_reply.getReturnCode() == 0) {
						callbacks.RemoveFileSucceeded();
					}
					else {
						callbacks.RemoveFileFailed("Delete failed: "+delete_reply);
					}
					return;
				}
				esay(this.toString()+" got unexpected PnfsDeleteEntryMessage "+
				     " : "+delete_reply+" ; Ignoring");
			}
			else if (message instanceof PoolModifyPersistencyMessage) {
				PoolModifyPersistencyMessage modify_reply =
					(PoolModifyPersistencyMessage) message;
				if (state == WAITING_FOR_PNFS_AND_POOL_REPLIES_MESSAGES) {
					modifyPersistencyReplyArrived(modify_reply);
					return;
				}
				esay(this.toString()+" got unexpected PoolModifyPersistencyMessage "+
				     " : "+modify_reply+" ; Ignoring");
				
			}
			else if(message instanceof PnfsFlagMessage) {
				PnfsFlagMessage flag_reply =
					(PnfsFlagMessage) message;
				if(state == WAITING_FOR_PNFS_AND_POOL_REPLIES_MESSAGES) {
					flagReplyArrived(flag_reply);
					return;
				}
				esay(this.toString()+" got unexpected PnfsFlagMessage "+
				     " : "+flag_reply+" ; Ignoring");
				
			}
			else {
				esay(this.toString()+" got unknown message "+
				     " : "+message.getErrorObject());
				
				callbacks.RemoveFileFailed( this.toString()+" got unknown message "+
						 " : "+message.getErrorObject()) ;
			}
		}
		else {
			esay(this.toString()+" got unknown object "+
			     " : "+o);
			callbacks.RemoveFileFailed(this.toString()+" got unknown object "+
					" : "+o) ;
		}
	}
	
	private Vector cache_locations;
	private boolean flag_set;

	public void cacheLocationsArrived(PnfsGetCacheLocationsMessage cache_loc_msg) {
		if(cache_loc_msg.getReturnCode() != 0) {
			callbacks.RemoveFileFailed("can not get cached locations");
			return;
		}
		cache_locations = cache_loc_msg.getCacheLocations();
		int locations_num = cache_locations.size();
		say("received "+locations_num+" cached locations: ");
		try {
			if (locations_num > 0) {
				state = WAITING_FOR_PNFS_AND_POOL_REPLIES_MESSAGES;
				for (int i = 0 ; i<locations_num; ++i) {
					String pool = (String)cache_locations.get(i);
					say("received cached location "+pool+ " asking to make non-precious");
					PoolModifyPersistencyMessage modifyPersistencyRequest =
						new PoolModifyPersistencyMessage(pool,pnfsId, false);
					cell.sendMessage( new CellMessage(
								  new CellPath(pool) ,
								  modifyPersistencyRequest ) ,
							  true , true ,
							  this ,
							  1*24*60*60*1000);
				}
			}
			PnfsFlagMessage flag =
				new PnfsFlagMessage( pnfsId , "d" , "put" );
			flag.setReplyRequired( true );
			flag.setValue( "true");
			cell.sendMessage( new CellMessage(
						  new CellPath("PnfsManager") ,
						  flag ) ,
					  true , true ,
					  this ,
					  1*24*60*60*1000);
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
				callbacks.RemoveFileFailed("Exception");
			}
		}
		catch(Exception ee ) {
			state = FINAL_STATE;
			esay(ee);
			callbacks.RemoveFileFailed("Exception");
		}
	}
	
	public void flagReplyArrived(PnfsFlagMessage reply) {
		if (reply.getReturnCode() != 0) {
			state = FINAL_STATE;
			String error =
				"can not set flag d=\"true\" of "+path;
			esay(error);
			callbacks.RemoveFileFailed(error);
			return;
		}
		
		flag_set = true;
		if(cache_locations.isEmpty()) {
			state = FINAL_STATE;
			say("Remove Successeded for "+path);
			callbacks.RemoveFileSucceeded();
		}
	}
    
	public void modifyPersistencyReplyArrived(PoolModifyPersistencyMessage reply) {
		String pool = reply.getPoolName();
		if(!cache_locations.contains(pool)) {
			esay("received a responce from "+pool+", was not waiting for it, ignoring");
			return;
		}
		if(reply.getReturnCode() != 0) {
			state = FINAL_STATE;
			String error =
				"can not modify persistency of "+path+ " at pool \""+pool+"\"";
			esay(error);
			callbacks.RemoveFileFailed(error);
			return;
		}
		else {
			cache_locations.remove(pool);
		}
		if(cache_locations.isEmpty() && flag_set) {
			state = FINAL_STATE;
			say("RemoveFile Successeded for "+path);
			callbacks.RemoveFileSucceeded();
		}
	}
	
	public void fileInfoArrived(PnfsGetStorageInfoMessage storage_info_msg) {
		if(storage_info_msg.getReturnCode() != 0) {
			callbacks.RemoveFileFailed("file does not exist, cannot delete");
			return;
		}
		say("storage_info_msg = "+storage_info_msg );
		storageInfo = storage_info_msg.getStorageInfo() ;
		say("storageInfo = "+storageInfo );
		long size = storageInfo.getFileSize();
		diskCacheV111.util.FileMetaData fmd =
			storage_info_msg.getMetaData();
		if(fmd.isDirectory()) {
			callbacks.RemoveFileFailed("file is a directory, can not delete");
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
		FileMetaData srm_fmd = Storage.getFileMetaData(user,path,pnfsId,storageInfo,fmd,null);
		if(!Storage._canDelete(user,fileId,srm_fmd)) {
			callbacks.RemoveFileFailed("permission denied");
			return;
		}

		PnfsGetCacheLocationsMessage getCacheLocMsg =
			new PnfsGetCacheLocationsMessage(pnfsId);
		try {
			state = WAITING_FOR_CACHE_LOCATIONS_MESSAGE;
			cell.sendMessage( new CellMessage(
						  new CellPath("PnfsManager") ,
						  getCacheLocMsg ) ,
					  true , true ,
					  this ,
					  1*24*60*60*1000) ;
		}
		catch (Exception ee ) {
			state = FINAL_STATE;
			esay(ee);
			callbacks.RemoveFileFailed("Exception");
		}
	}
	
	public void exceptionArrived( CellMessage request , Exception exception ) {
		state = FINAL_STATE;
		esay("exceptionArrived "+exception+" for request "+request);
		callbacks.RemoveFileFailed("Exception");
	}

	public void answerTimedOut( CellMessage request ) {
		esay("answerTimedOut for request "+request);
		callbacks.RemoveFileFailed("answerTimedOut for request "+request);
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
}

