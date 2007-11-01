package org.dcache.doors;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.core.stream.TooMuchLogicalStreamsException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.RedirectResponse;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.security.AuthorizationHandler;
import org.dcache.xrootd.util.DoorRequestMsgWrapper;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.ParseException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

public class XrootdDoorListener implements StreamListener {

	private class PnfsFileStatus extends FileStatus {

		private PnfsId pnfsId;

		public PnfsFileStatus(PnfsId id) {
			super();
			pnfsId = id;
		}
		
		public PnfsId getPnfsId() {
			return pnfsId;
		}
		
	}	
	
	private XrootdDoor door;
	private PhysicalXrootdConnection physicalXrootdConnection;
	private PnfsFileStatus fileStatus;
	private DoorRequestMsgWrapper info = new DoorRequestMsgWrapper();
	
	private InetSocketAddress redirectAdress = null;
	private int streamId;

	public XrootdDoorListener(XrootdDoorController controller, int streamID) {
		this.door = controller.getDoor();
		this.physicalXrootdConnection = controller.getXrootdConnection();
		
		try {
			this.physicalXrootdConnection.getStreamManager().getStream(streamID);
		} catch (TooMuchLogicalStreamsException e) {}
		
		this.streamId = streamID;
	}

	
	/**
	 * The Open method is the only request we need to deal with (login and authentication is done
	 * on a per-connection-basis, not for every single file) besides the stat request.
	 * The open, if successful, will always result in a redirect response to the proper pool, hence
	 * no subsequent requests like sync, read, write or close are expected at the door.
	 */
	public void doOnOpen(OpenRequest req) {
		
///////////////////////////////////////////////////////////////
//		debug section: print open flags and mode
///////////////////////////////////////////////////////////////		
		door.say("data size of open request: "
				+ req.getDataLength() + " bytes");

		door.say("request path to open: " + req.getPath());
		
		int options =req.getOptions();
		String openFlags = "options to apply for open path (raw="+options+" ):";

		if ((options & XrootdProtocol.kXR_async) == XrootdProtocol.kXR_async)
			openFlags += " kXR_async";
		if ((options & XrootdProtocol.kXR_compress) == XrootdProtocol.kXR_compress)
			openFlags += " kXR_compress";
		if ((options & XrootdProtocol.kXR_delete) == XrootdProtocol.kXR_delete)
			openFlags += " kXR_delete";
		if ((options & XrootdProtocol.kXR_force) == XrootdProtocol.kXR_force)
			openFlags += " kXR_force";
		if ((options & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new)
			openFlags += " kXR_new";
		if ((options & XrootdProtocol.kXR_open_read) == XrootdProtocol.kXR_open_read)
			openFlags += " kXR_open_read";
		if ((options & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt)
			openFlags += " kXR_open_updt";
		if ((options & XrootdProtocol.kXR_refresh) == XrootdProtocol.kXR_refresh)
			openFlags += " kXR_refresh";
		if ((options & XrootdProtocol.kXR_mkpath) == XrootdProtocol.kXR_mkpath)
			openFlags += " kXR_mkpath";
		if ((options & XrootdProtocol.kXR_open_apnd) == XrootdProtocol.kXR_open_apnd)
			openFlags += " kXR_open_apnd";
		if ((options & XrootdProtocol.kXR_retstat) == XrootdProtocol.kXR_retstat)
			openFlags += " kXR_retstat";
		

		door.say("open flags: "+openFlags);
		
		door.say("mode to apply to open path: ");

		int mode = req.getUMask();
		String s = "";

		if ((mode & XrootdProtocol.kXR_ur) == XrootdProtocol.kXR_ur)
			s += "r";
		else
			s += "-";
		if ((mode & XrootdProtocol.kXR_uw) == XrootdProtocol.kXR_uw)
			s += "w";
		else
			s += "-";
		if ((mode & XrootdProtocol.kXR_ux) == XrootdProtocol.kXR_ux)
			s += "x";
		else
			s += "-";

		s += " ";

		if ((mode & XrootdProtocol.kXR_gr) == XrootdProtocol.kXR_gr)
			s += "r";
		else
			s += "-";
		if ((mode & XrootdProtocol.kXR_gw) == XrootdProtocol.kXR_gw)
			s += "w";
		else
			s += "-";
		if ((mode & XrootdProtocol.kXR_gx) == XrootdProtocol.kXR_gx)
			s += "x";
		else
			s += "-";

		s += " ";

		if ((mode & XrootdProtocol.kXR_or) == XrootdProtocol.kXR_or)
			s += "r";
		else
			s += "-";
		if ((mode & XrootdProtocol.kXR_ow) == XrootdProtocol.kXR_ow)
			s += "w";
		else
			s += "-";
		if ((mode & XrootdProtocol.kXR_ox) == XrootdProtocol.kXR_ox)
			s += "x";
		else
			s += "-";

		door.say(s);
		
///////////////////////////////////////////////////////////////
//		end of debug section
///////////////////////////////////////////////////////////////	
		
			
		String pathToOpen = req.getPath();
		
		this.info.setpath( pathToOpen );
		
		
		boolean isWrite = false;
		
//		write access requested ?
		if (req.isNew() || req.isReadWrite()) {
			
			if (door.isReadOnly()) {
				door.esay("Permission denied. Access is read only.");
				
				respondWithError( req.getStreamID(), XrootdProtocol.kXR_FileLockedr, "Permission denied. Access is read only." );
				
				return;
			}
			
			isWrite = true;			
		}

//		do authorization check if required by configuration
		if (door.authzRequired()) {
			
			if (door.getAuthzFactory() == null) {
				String msg = "Authorization required but appropriate handler is not initialised. Server probably misconfigured.";
				door.esay( msg );
				respondWithError( req.getStreamID(), XrootdProtocol.kXR_ServerError, msg);
				return;
			}
			
			
			
			door.say("checking authorization for "+pathToOpen);
			
			
//			all information neccessary for checking authorization is found in opaque  
			Map opaque = null;
			try {
				
				opaque = req.getOpaque();
			
			} catch (ParseException e) {
				StringBuffer msg = new StringBuffer( "invalid opaque data: " );
				msg.append(e);
				int opaqueStart = req.hasOpaque();
				if (opaqueStart >= 0) {
					msg.append(" opaque=");
					msg.append(new String(req.getData(), opaqueStart, req.getDataLength() - opaqueStart));
				}
				respondWithError( req.getStreamID(), XrootdProtocol.kXR_NotAuthorized, msg.toString() );
				return;
			}
		
			AuthorizationHandler authzHandler = door.getAuthzFactory().getAuthzHandler();
			
			boolean isAuthorized = false;
			
			try {
				isAuthorized = authzHandler.checkAuthz(pathToOpen, opaque, isWrite, door);
			} catch (GeneralSecurityException e) {
				respondWithError( req.getStreamID(), XrootdProtocol.kXR_NotAuthorized, "authorization check failed: "+e.getMessage() );
				return;
			} finally {
				if (authzHandler.getUser() != null) {
					this.info.setUser( authzHandler.getUser() );
				}
			}
			
					
			if (!isAuthorized) {
				respondWithError( req.getStreamID(), XrootdProtocol.kXR_NotAuthorized, "not authorized" );
				return;
			}
			
			
//			In case of enabled authorization, the path in the open request can refer to the lfn.
//			In this case the real path is delivered by the authz plugin
			if (authzHandler.providesPFN()) {
				door.say("access granted for LFN="+pathToOpen+" PFN="+authzHandler.getPFN());
				
//				get the real path (pfn) which we will open 
				pathToOpen = authzHandler.getPFN();
				this.info.setpath( pathToOpen );
			}
			
		}
		
		
//		check if write access is restricted in general and whether the path to open
//		matches the whitelist 
		if (isWrite 
				&& door.getAuthorizedWritePaths() != null
				&& !matchWritePath(pathToOpen, door.getAuthorizedWritePaths())) {
			respondWithError( req.getStreamID(), XrootdProtocol.kXR_FileLockedr, "Write permission denied" );
			return;
		}
		
//////////////////////////////////////////////////////////////////////		
//		interact with core dCache to open the requested file
		
		PnfsGetStorageInfoMessage storageInfoMsg = null;
		try {
			
			storageInfoMsg = door.getStorageInfo(pathToOpen);
						
		} catch (IOException e) {
			door.say("No PnfsId found for path: " + pathToOpen);
		}
		
		if (storageInfoMsg == null) {
			
//			we couldn't find a PNFS id for requested path
			
			if (isWrite) {
//				This is required for write access, because we want to create a new file from scratch
			
//				get parent directory path by truncating filename
				String parentDir = pathToOpen.substring(0, pathToOpen.lastIndexOf("/"));
				FileMetaData parentMD = null;
								
				try {

					// parent directory exists?
					parentMD = door.getFileMetaData(parentDir);

				} catch (CacheException e) {
					// parent directory does not exist
					
					if (e.getRc() != CacheException.FILE_NOT_FOUND) {
						door.esay("unknown PNFS error: "+ e.getMessage());
						respondWithError( req.getStreamID(),
							XrootdProtocol.kXR_ServerError,
						    "Cannot create Pnfs entry : unknown PNFS error with code " + e.getRc() );
						return;
					}
					
					// check for kXR_mkpath-flag (create missing directories along the path)
					if ((options & XrootdProtocol.kXR_mkpath) == XrootdProtocol.kXR_mkpath) {

						try {
							door.makePnfsDir(parentDir);
							parentMD = door.getFileMetaData(parentDir);
						} catch (CacheException e2) {
							respondWithError( req.getStreamID(),
								XrootdProtocol.kXR_NoSpace,
								"Parent PNFS entry does not exit and cannot be created" );
							return;
						}

					} else {
//						no automatic directory creation -> error 
						respondWithError( req.getStreamID(),
							XrootdProtocol.kXR_NoSpace,
							"Error creating pnfs entry: parent directory does not exist");
						return;
					}
				}

				this.info.setMappedIds( parentMD.getGid(), parentMD.getUid() );
				
				// at this point the parent directory exists, now we check permissions
				if (! door.checkWritePermission(parentMD)) {
					respondWithError( req.getStreamID(),
						XrootdProtocol.kXR_NotAuthorized,
						"No permissions to create file");
					return;
				}


//				create the actual PNFS entry with parent uid:gid
				
				door.esay("open request (write mode): trying to create new PNFS entry");
				try {
					
					storageInfoMsg = door.createNewPnfsEntry(pathToOpen, parentMD.getUid(), parentMD.getGid());
				
				} catch (CacheException e) {
					respondWithError( req.getStreamID(),
						XrootdProtocol.kXR_NoSpace,
						"Cannot create Pnfs entry for path" );
					return;
				}
					
							
			} else {
				
//				no PnfsID found, file not in dCache namespace -> read request failed
				
				respondWithError( req.getStreamID(), 
						XrootdProtocol.kXR_NotFound, 
						"No PnfsId (File not found)" );
				return;
			}
			
		} else {
			
//			ok, we found a PnfsId for the requested path
			
			if (isWrite) {
//				we don't allow altering existing files (update)
				this.info.setPnfsId( storageInfoMsg.getPnfsId() );
				respondWithError( req.getStreamID(), 
						XrootdProtocol.kXR_Unsupported, 
						"File already exits. Altering exisiting files not supported" );
				return;
			}
						
		}
		
		this.info.setPnfsId( storageInfoMsg.getPnfsId() );
		
		StorageInfo storageInfo = storageInfoMsg.getStorageInfo();
		
//		storageinfo must be available for the newly created PNFS entry 
		if (storageInfo == null) {
			try {
				door.esay("Cannot create file, deleting PNFS entry");
				door.deletePnfsEntry(storageInfoMsg.getPnfsId());
			} catch (CacheException e1) {
				door.esay(e1);
			} 
			
			respondWithError( req.getStreamID(), 
					XrootdProtocol.kXR_NotFound, 
					"Cannot create file entry (no parent storage this.info available)");
			return;
		}
		

//		get unique fileHandle (PnfsId is not unique in case the same file is opened more than once in this door-instance)		
		int fileHandle = door.getNewFileHandle();
		
		this.info.setFileHandle( fileHandle );
		
		long checksum = req.calcChecksum();
		door.say("checksum of openrequest: "+checksum);
		
		ProtocolInfo protocolInfo = door.createProtocolInfo(storageInfoMsg.getPnfsId(), fileHandle, checksum, physicalXrootdConnection.getNetworkConnection().getClientSocketAddress());
		
		fileStatus = new PnfsFileStatus(storageInfoMsg.getPnfsId());
		fileStatus.setSize(storageInfo.getFileSize());
		fileStatus.setWrite(isWrite);
		fileStatus.setID(fileHandle);
		
		
//		at this point we have the storageinfo and can ask the PoolManager for a pool to handle this transfer
		
		String pool = null;
		
		try {
						
			boolean requestDone = false;
			while (!requestDone) {
			
//				ask the Poolmanager for a pool
				pool = door.askForPool(fileStatus.getPnfsId(), storageInfo, protocolInfo, fileStatus.isWrite());
				try {
//					ask the Pool to prepare the transfer
					redirectAdress  = door.askForFile(pool, fileStatus.getPnfsId(), storageInfo, (XrootdProtocolInfo) protocolInfo, fileStatus.isWrite());
					requestDone = true;
					
				} catch (CacheException e) {
					if  (e.getRc() != CacheException.FILE_NOT_IN_REPOSITORY) {
						throw e;
					}
//					FILE_NOT_IN_REPOSITORY means file not in that pool, 
//					but pnfs is still pointing to this pool. the pool will delete cacheentry in pnfs by 
//					itself, so this	exception return code can be used to recover, because PoolManager 
//					will return another pool when asked again.
					requestDone = false;
					door.say("pool says FILE_NOT_IN_REPOSITORY, will ask PoolManager for another pool");
				}			
			}
		} catch (Exception e) {
			
			door.esay(e.getMessage());
			
//			remove created pnfs entry
			if (isWrite) {
				try {
					door.deletePnfsEntry(fileStatus.getPnfsId());
				} catch (CacheException e1) {
					door.esay(e1);
				}
			}
			respondWithError( req.getStreamID(), 
					XrootdProtocol.kXR_ServerError, 
					" Cannot find " + (fileStatus.isWrite() ? "write" : "read") + " pool : "+e.getMessage());
			return;

		}
		
		
//		
//		ok, open was successful
//				
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new RedirectResponse(req.getStreamID(), redirectAdress.getHostName(), redirectAdress.getPort()));
		door.sendBillingInfo( this.info );
	}

	private void respondWithError(int streamID, int errorCode, String errMsg) {
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(
				new ErrorResponse(
						streamID, 
						errorCode, 
						errMsg ));
		
		// for billing purposes
		this.info.fileOpenFailed( errorCode, errMsg );
		door.sendBillingInfo( this.info );				
	}


	public void doOnStatus(StatRequest req) {
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new StatResponse(req.getStreamID(), fileStatus));
	}
	
	public void doOnRead(ReadRequest req) {
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
	}

	public void doOnWrite(WriteRequest req) {
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
	}

	public void doOnSync(SyncRequest req) {
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(req.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
	}

	public void doOnClose(CloseRequest request) {
		physicalXrootdConnection.getResponseEngine().sendResponseMessage(new ErrorResponse(request.getStreamID(), XrootdProtocol.kXR_FileNotOpen, " File not open, send a kXR_open Request first."));
	}

	public void handleStreamClose() {
		
//		clean up something?

		door.say("closing logical stream (streamID="+streamId+")");
	}

	/**
	 * check wether the given path matches against a list of allowed paths
	 * @param pathToOpen the path which is going to be checked
	 * @param authorizedWritePathList the list of allowed paths
	 * @return
	 */
	private boolean matchWritePath(String pathToOpen, List authorizedWritePathList) {
		
		for (Iterator it = authorizedWritePathList.iterator(); it.hasNext();) {
			if (pathToOpen.startsWith((String) it.next())) {
				return true;
			}
		}
		return false;
	}



}
