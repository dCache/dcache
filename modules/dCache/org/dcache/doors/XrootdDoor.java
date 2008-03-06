package org.dcache.doors;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.security.AbstractAuthorizationFactory;
import org.dcache.xrootd.util.DoorRequestMsgWrapper;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileMetaData.Permissions;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import dmg.util.StreamEngine;

public class XrootdDoor extends CellAdapter {

	private CellNucleus _nucleus;

	private PnfsHandler _pnfs_handler;
	
	private Socket _doorSocket;

	private final static String _poolManagerName = "PoolManager";

	private final static String _pnfsManagerName = "PnfsManager";

	private Object _redirectSync = new Object();
	
//	fileHandle -> InetSocketAddress
	private Hashtable _redirectTable = new Hashtable();
	
//	fileHandle -> Xrootd logicalStreamID 
	private Hashtable<Integer, Integer> _logicalStreamTable = new Hashtable<Integer, Integer>();
	
	private int _fileHandleCounter = 0;
	
	XrootdDoorController _controller;
	
	private PhysicalXrootdConnection _physicalXrootdConnection;

	private boolean _closeInProgress = false;

//	forbid write access by default
	private boolean _isReadOnly = false;
	
//	indicates for this xrootd connection whether authorization is required
	private boolean _authzRequired = false;

//	dirty hack: will be deprecated soon
	private String _noStrongAuthz;

//	the actual mover queue on the pool onto which this request gets scheduled
	private String _ioQueue = null;

//	the prefix of the transaction string used for billing
	private String _transactionPrefix;

//  the number of max open files per physical xrootd connection   
	private int _maxFileOpens = 5000;

//	the list of paths which are authorized for xrootd write access (via dCacheSetup) 
	private static List _authorizedWritePaths = null;
	
//	holds the factory for the specific plugin. the plugin is loaded only once
	private static AbstractAuthorizationFactory _authzFactory = null;

//	is set true only once (when plugin loading didn't succeed) to avoid multiple trials
	private static boolean _authzPluginLoadFailed = false;
	
	private static long _CellMessageTimeout = 3000;

	private final static String XROOTD_PROTOCOL_STRING = "Xrootd";
	private final static int XROOTD_PROTOCOL_MAJOR_VERSION = 2;
	private final static int XROOD_PROTOCOL_MINOR_VERSION = 7;
	
	public XrootdDoor(String name, StreamEngine engine, Args args)	throws Exception {
		
		// the cell stuff
		super(name, args, false);
		
		_nucleus = getNucleus();

		_pnfs_handler = new PnfsHandler(this, new CellPath(_pnfsManagerName));		
		
		_doorSocket = engine.getSocket();
		
//		forbid write access if configured in batchfile
		if (args.getOpt("isReadOnly").equals("true")) {
			_isReadOnly = true;
		}
		
//		look for colon-seperated path list, which, if present, is used to allow write access to these paths only 
		String pathListString = args.getOpt("allowedPaths");
		if ( !(pathListString == null || pathListString.length() == 0)
				&& _authorizedWritePaths == null)	{
			
			parseAllowedWritePaths(pathListString);
		}
		
//		try to load authorization plugin if required by batch file
		if ( !(args.getOpt("authzPlugin") == null 
				|| args.getOpt("authzPlugin").length() == 0
				|| "none".equals(args.getOpt("authzPlugin"))))	{
			
			_authzRequired = true;
			loadAuthzPlugin(args);
			
//			dirty hack (about to be deprecated soon) for ALICE
//			set the authz logic during file open being less strict
			String noStrongAuthzString = args.getOpt("nostrongauthorization");
			if (	"read".equalsIgnoreCase(noStrongAuthzString)
				|| 	"write".equalsIgnoreCase(noStrongAuthzString)
				|| 	"always".equalsIgnoreCase(noStrongAuthzString)) {
			
				_noStrongAuthz = noStrongAuthzString;
			}
		}
		
		_ioQueue = args.getOpt("io-queue");
		if (_ioQueue == null  || _ioQueue.length() == 0 ) {
			_ioQueue = null;
		}
		esay(_ioQueue != null ? "defined moverqueue: "+_ioQueue : "no moverqueue defined");
		
		String maxFileOpens = args.getOpt("maxFileOpensPerLogin");
		if (maxFileOpens != null && maxFileOpens.length() > 0) {
		    try {
		        _maxFileOpens = Integer.parseInt(maxFileOpens);
		    } catch (NumberFormatException e) {
                esay("invalid format of 'maxFileOpensPerLogin' parameter, defaulting to "+_maxFileOpens);
            }
		}		
		
//		// we have to use 'CellAdapapter.newThread()' instead of
//		// 'new Thread()' because we want to have the worker
//		// thread to be a member of the cell ThreadGroup.
		
		StringBuffer sb = new StringBuffer("door:");
		sb.append(getNucleus().getCellName());
		sb.append("@");
		sb.append(getNucleus().getCellDomainName());
		sb.append(":");
		_transactionPrefix = sb.toString();
		
		_nucleus.newThread(new Runnable() {

			public void run() {
				esay("starting minithread");
				
//				enable Xrootd message processing for this existing network connection
				initXrootd(getDoorSocket());
				
				try {
					synchronized (this) {
						this.wait();
					}	
							
				} catch (InterruptedException e) {}
				
				esay("finishing minithread");
				
			}
		} , "Xrootd-door-MiniThread").start();
		
//		start cell message receiving (important!!!)
		start();
		
	}

	private void parseAllowedWritePaths(String pathListString) {
			
		LinkedList list = new LinkedList();
		String[] paths = pathListString.split(":");
		
				
		for (int i = 0; i < paths.length; i++) {
			String path = paths[i];
			if (!path.endsWith("/")) {
				path += "/";
			}
			
			if (!list.contains(path)) {
				list.add(path);
				esay("allowed write path: "+path);
			}
		}
		Collections.sort(list);
		_authorizedWritePaths = list;		
	}

	private void initXrootd(Socket socket) {
		
//		create new XrootdConnection based on existing socket
		try {
			_physicalXrootdConnection = new PhysicalXrootdConnection(socket, XrootdProtocol.LOAD_BALANCER);
		} catch (IOException e) {
			_physicalXrootdConnection.closeConnection();
			return;
		}
		
//		set controller for this connection to handle login, auth and connection-specific settings
		_controller = new XrootdDoorController(this, _physicalXrootdConnection);
		_physicalXrootdConnection.setConnectionListener(_controller);
		


	}

	
	public void cleanUp() {
		
		if (!isCloseInProgress()) {
			_controller.shutdownXrootd();
		}
	}

	public void getInfo(PrintWriter pw) {
		pw.println("Xrootd Door");
		pw.println("Protocol Version 2.54");
		
		if (_physicalXrootdConnection.getStatus().isConnected()) {
			pw.println("Connected with "+_physicalXrootdConnection.getNetworkConnection().getSocket().getInetAddress());
		} else
			pw.println("Not connected");
		
		pw.println("number of open files: "+_logicalStreamTable.size());
		pw.println("number of logical streams: " + _physicalXrootdConnection.getStreamManager().getNumberOfStreams());
	}

	
	PnfsGetStorageInfoMessage getStorageInfo(String path) throws IOException {
		
		PnfsGetStorageInfoMessage result = null;
		
		try {
		
			result = _pnfs_handler.getStorageInfoByPath(path);
			
		} catch (diskCacheV111.util.CacheException ce) {
			esay("can not find pnfsid of path : " + path);
			esay("CacheException = " + ce);
			throw new IOException("can not find pnfsid of path : " + path
					+ " root error " + ce.getMessage());
		}
		
		return result;
	}
	
	public PnfsGetStorageInfoMessage createNewPnfsEntry(String pnfsPath, int uid, int gid) throws CacheException {
		
		return _pnfs_handler.createPnfsEntry(pnfsPath, uid, gid, 0644);
		
	}
	
	public void deletePnfsEntry(PnfsId pnfsId) throws CacheException {
		_pnfs_handler.deletePnfsEntry(pnfsId);
	}
	
	
	private synchronized void handleRedirectMessage(XrootdDoorAdressInfoMessage reply) {
		
		
		InetSocketAddress redirectAddress = null;
		
//		pick the first IPv4 address from the collection
//		at this point, we can't determine, which of the pool IP-addresses is the right one, so we select the first
		for (Iterator it = reply.getNetworkInterfaces().iterator(); it.hasNext(); ) {
			NetIFContainer container = (NetIFContainer) it.next();
			
			for (Iterator it2 = container.getInetAddresses().iterator(); it2.hasNext();) {
				Object ip = it2.next();
				
				if (ip instanceof Inet4Address) {
					redirectAddress = new InetSocketAddress((Inet4Address) ip, reply.getServerPort());
					break;
				}				
			}	
		}
		
		
		
		if (redirectAddress != null) {
			_redirectTable.put(reply.getXrootdFileHandle(), redirectAddress);
		} else {
			esay("error: no valid IP-adress received from pool. Redirection not possible");
			
//			we have to put a null address to at least notify the right xrootd thread about the failure 
			_redirectTable.put(reply.getXrootdFileHandle(), "");
		}
	}

	// these were taken almost without changes from other doors

	InetSocketAddress askForFile(String pool, PnfsId pnfsId,
			StorageInfo storageInfo, XrootdProtocolInfo protocolInfo, boolean isWrite)
			throws CacheException, Exception {
		
		
		
		say("Trying pool " + pool + " for " + (isWrite ? "Write" : "Read"));
		PoolIoFileMessage poolMessage = isWrite ? (PoolIoFileMessage) new PoolAcceptFileMessage(
				pool, pnfsId.toString(), protocolInfo, storageInfo)
				: (PoolIoFileMessage) new PoolDeliverFileMessage(pool, pnfsId
						.toString(), protocolInfo, storageInfo);


		// specify the desired mover queue
		poolMessage.setIoQueueName(_ioQueue);
		
		// the transaction string will be used by the pool as initiator (-> table join in Billing DB)
		poolMessage.setInitiator( _transactionPrefix + protocolInfo.getXrootdFileHandle());
		
//		PoolManager must be on the path for return message (DoorTransferFinished)
		CellPath path = new CellPath(_poolManagerName);
		path.add(pool);
				
		CellMessage reply = sendAndWait(new CellMessage(path,
				poolMessage), _CellMessageTimeout );
		if (reply == null) {
			throw new Exception("Pool request timed out : " + pool);
		}

		Object replyObject = reply.getMessageObject();

		if (!(replyObject instanceof PoolIoFileMessage)) {
			throw new Exception("Illegal Object received : "
					+ replyObject.getClass().getName());
		}

		PoolIoFileMessage poolReply = (PoolIoFileMessage) replyObject;
		
		if (poolReply.getReturnCode() != 0) {
			
			if (poolReply.getReturnCode() == CacheException.FILE_NOT_IN_REPOSITORY) {
//				file not in pool, but is supposed to be there. 
//				the pool will delete cacheentry in pnfs by itself, so this
//				information can be used to recover, because PoolManager will return another pool
//				when asked again or will raise an exception
				throw new CacheException(CacheException.FILE_NOT_IN_REPOSITORY, "File not in repository");
			} else {
//				general error in pool, cannot recover (request must fail)
				throw new Exception("Pool error: " + poolReply.getErrorObject());
			}
		} 
		
		say("Pool " + pool + (isWrite ? " will accept file" : " will deliver file ") + pnfsId);
		
		Integer key = Integer.valueOf(protocolInfo.getXrootdFileHandle());
		
		try {
			synchronized (_redirectSync) {
				while ( !_redirectTable.containsKey(key)) {
					say("waiting for redirect message from pool "+pool+" (pnfsId="+pnfsId+" fileHandle="+key+")");
					_redirectSync.wait();
				}
			}
		} catch (InterruptedException ie) {}
		
		if (!(_redirectTable.get(key) instanceof InetSocketAddress)) {
			_redirectTable.remove(key);
			throw new CacheException("Pool responded with invalid redirection address, transfer failed");
		}
		
		InetSocketAddress newAddress = (InetSocketAddress) _redirectTable.get(key);
		say("got redirect message from pool "+pool+" (pnfsId="+pnfsId+" fileHandle="+key+")");
		
		_redirectTable.remove(key);
		return newAddress;
	}

	public String askForPool(PnfsId pnfsId, StorageInfo storageInfo,
			ProtocolInfo protocolInfo, boolean isWrite) throws Exception {
		
		say("asking Poolmanager for "+ (isWrite ? "write" : "read") + "pool for PnfsId "+pnfsId);
		
		//
		// ask for a pool
		//
		PoolMgrSelectPoolMsg request = null;
		
		if (isWrite)
			request = new PoolMgrSelectWritePoolMsg(
				pnfsId, storageInfo, protocolInfo, 0L);
		else
			request =  new PoolMgrSelectReadPoolMsg(pnfsId,
						storageInfo, protocolInfo, 0L);

//		Wait almost forever. Taking the PoolMgrSelectPoolMsg very long could be caused by a restage 
//		from tape to pool OR the request is suspended in PoolManager (can be checked by 'rc ls' in admin interface)
		long poolMgrTimeout = Long.MAX_VALUE;	// timeout in ms	
		CellMessage reply = sendAndWait(new CellMessage(new CellPath(_poolManagerName),	request), poolMgrTimeout);
		
		if (reply == null) {
			throw new Exception("PoolMgrSelectReadPoolMsg timed out after "+poolMgrTimeout/1000+" s. Request suspended on server side (PoolManager)?");
		}

		Object replyObject = reply.getMessageObject();

		if (!(replyObject instanceof PoolMgrSelectPoolMsg)) {
			throw new Exception("Not a PoolMgrSelectPoolMsg : "
					+ replyObject.getClass().getName());
		}

		request = (PoolMgrSelectPoolMsg) replyObject;
		say("poolManagerReply = " + request);
		if (request.getReturnCode() != 0) {
			throw new Exception("Pool manager error: "
					+ request.getErrorObject());
		}

		String pool = request.getPoolName();
		
		say("Can " + (isWrite ? "write to" : "read from") + " pool " + pool);

		return pool;
	}


	// handle post-transfer success/failure messages going back to the client
	public void messageArrived(CellMessage msg) {
		Object object = msg.getMessageObject();
		say("Message messageArrived [" + object.getClass() + "]="
				+ object.toString());
		say("Message messageArrived source = " + msg.getSourceAddress());
		if (object instanceof XrootdDoorAdressInfoMessage) {
			XrootdDoorAdressInfoMessage reply = (XrootdDoorAdressInfoMessage) object;
			say("received redirect msg from mover");
			synchronized (_redirectSync) {
				say("got lock on _sync");
				
				handleRedirectMessage(reply);
				_redirectSync.notifyAll();
			}
		} else if (object instanceof DoorTransferFinishedMessage) {
			
			DoorTransferFinishedMessage finishedMsg = (DoorTransferFinishedMessage) object;
			
			if ( (finishedMsg.getProtocolInfo() instanceof XrootdProtocolInfo)) {
			    
			    XrootdProtocolInfo protoInfo = (XrootdProtocolInfo) finishedMsg.getProtocolInfo(); 
			    int fileHandle = protoInfo.getXrootdFileHandle();
			
				
			    if (_logicalStreamTable.containsKey(fileHandle)) {

			        say("received DoorTransferFinished-Message from mover, cleaning up (PnfsId="
		                    + finishedMsg.getPnfsId() + " fileHandle="+fileHandle+")");
				
			        forgetFile(fileHandle);
				
			    }
			}
			
		} else {
			say("Unexpected message class " + object.getClass()+" (source = " + msg.getSourceAddress()+")");
		}
				
	}
	
	public void forgetFile(int fileHandle) {
		int streamID = _logicalStreamTable.remove(fileHandle);
		_physicalXrootdConnection.getStreamManager().destroyStream(streamID);
	}

	public synchronized ProtocolInfo createProtocolInfo(PnfsId pnfsId, int fileHandle, long checksum, InetSocketAddress client) {
		
		ProtocolInfo info = new XrootdProtocolInfo(
				XROOTD_PROTOCOL_STRING , 
				XROOTD_PROTOCOL_MAJOR_VERSION, 
				XROOD_PROTOCOL_MINOR_VERSION, 
				client.getAddress().getHostName(), 
				client.getPort(), 
				new CellPath(getCellName(), getCellDomainName()), 
				pnfsId, 
				fileHandle, 
				checksum);
		
		say("created XrootdProtocolInfo: " + info);
		
		return info;
	}
	
	public void newFileOpen(int fileHandle, int streamID) {
		_logicalStreamTable.put(fileHandle, streamID);
	}
	
	public void clearOpenFiles() {
		_logicalStreamTable.clear();	
	}
	
	public InetAddress getDoorHost() {
		return _doorSocket.getLocalAddress();
	}
	
	public int getDoorPort() {
		return _doorSocket.getLocalPort();
	}
	
//	returns a filehandle unique within this Xrootd-Door instance 
	public synchronized int getNewFileHandle() {
		return ++_fileHandleCounter;
	}

	public Socket getDoorSocket() {
		return _doorSocket;
	}

	public synchronized boolean isCloseInProgress() {
		if (_closeInProgress == false) {
			_closeInProgress = true;
			return false;
		}
		
		return true;
	}

	public boolean isReadOnly() {
		return _isReadOnly ;
	}

	public boolean authzRequired() {
		return _authzRequired;
	}

	public AbstractAuthorizationFactory getAuthzFactory() {
		return _authzFactory;
	}
	
	private void loadAuthzPlugin(Args args) {
		
		if (getAuthzFactory() != null ||	// authorization plugin (static factory) already loaded ?
			_authzPluginLoadFailed) {		// don't try again to load if it failed once
		
			return;
		}
		
		say("trying to load authz plugin");
		
		AbstractAuthorizationFactory newAuthzFactory = null;
		try {
			newAuthzFactory = AbstractAuthorizationFactory.getFactory(args.getOpt("authzPlugin"));
		} catch (ClassNotFoundException e) {
			esay("Could not load authorization plugin "+args.getOpt("authzPlugin")+" cause: "+e);
			_authzPluginLoadFailed = true;
		}
		
		if (newAuthzFactory != null) {
			
			say("trying to find all options required by the plugin");
						
			try {
					
//				get names of all options required by the plugin
				String[] names = newAuthzFactory.getRequiredOptions();
				Map options = new HashMap();
					
//				try to load that options from the batchfile
				for (int i = 0; i < names.length; i++) {
					String value = args.getOpt(names[i]);
				
					if (value == null || value.equals("")) {
						throw new Exception("required option '"+names[i]+"' not found in batchfile");
					} else {
						options.put(names[i], value);
					}
				}
				
				newAuthzFactory.initialize(options);
					
				_authzFactory = newAuthzFactory;
				esay("authorization plugin initialised successfully!");
					
			} catch (Exception e) {
					esay("error initializing the authorization plugin: "+ e);
					_authzPluginLoadFailed = true;
			}							
		}		
		
		if (_authzPluginLoadFailed) {
			esay("Loading authorization plugin failed. All subsequent xrootd requests will fail due to this.\nPlease change batch file configuration and restart xrootd door.");
		}			
	}
	
	public void makePnfsDir(String path) throws CacheException {
		
		say("about to create directory: "+path);
		
		FileMetaData metadata = null;
		try {
//			check if directory already exists
			metadata = _pnfs_handler.getFileMetaDataByPath(path).getMetaData();
		} catch (CacheException e) {
//			ok, no pnfs id found, we can proceed to create the directory
		}
		
		if (metadata != null) {
			throw new CacheException(CacheException.FILE_EXISTS, "Cannot create directory "+path+": File exists");
		}
		
//		get parent directory String
		
		String parentDir = path;
//		truncate '/'-suffix, if present
		if (parentDir.endsWith("/")) {
			parentDir = parentDir.substring(0, parentDir.length() -1);
		}
//		truncate last segment to get the parent (cd ..)
		parentDir = parentDir.substring(0, parentDir.lastIndexOf("/")); 
		
		try {
//			check whether parent is a directory and has write permissions
			
			metadata = getFileMetaData(parentDir);
						
		} catch (CacheException e) {
//			check if parent directory does not exist
			
			if (e.getRc() == CacheException.FILE_NOT_FOUND) {

				say("creating parent directory "+parentDir+" first");

//				create parent directory recursively 
				makePnfsDir(parentDir);	
				metadata = getFileMetaData(parentDir);
				
			} else {
//				critical PNFS problem
				throw e; 
			}
		}
		
		if (! checkWritePermission(metadata)) {
			throw new CacheException("No permission to create directory "+path);
		}
		
//		at this point we assume that the parent directory exists and has all necessary permissions

//		create the directory via PNFS Handler
		PnfsCreateEntryMessage message = _pnfs_handler.createPnfsDirectory(path, metadata.getUid(), metadata.getGid(), 0755);
//		_pnfs_handler.createPnfsDirectory(path, 0, 0, 0755);
		
		if (message.getStorageInfo() == null) {
			
			esay("Error creating directory "+path+" (no storage info)");
			
			if (message.getPnfsId() != null) {
				try {
					_pnfs_handler.deletePnfsEntry(message.getPnfsId());
				} catch (CacheException e1) {
					esay(e1);
				} 
			}
			
			throw new CacheException(CacheException.FILE_NOT_FOUND, "Cannot create directory "+path+" in PNFS");
		}
		
		say("created directory "+path);		
	}

	public boolean checkWritePermission(FileMetaData meta) {
		Permissions user = meta.getUserPermissions();
//		Permissions group = meta.getGroupPermissions();
		
		return meta.isDirectory() && user.canWrite() && user.canExecute(); 
//		return meta.isDirectory() && user.canWrite() && user.canExecute() && group.canWrite() && group.canExecute(); 
	}
	
	public FileMetaData getFileMetaData(String path) throws CacheException {
		return _pnfs_handler.getFileMetaDataByPath(path).getMetaData();
		
	}
	
	public String noStrongAuthorization() {
		return _noStrongAuthz;
	}

	public List getAuthorizedWritePaths() {
		return _authorizedWritePaths ;
	}


	public void sendBillingInfo(DoorRequestMsgWrapper wrapper) {
        
		DoorRequestInfoMessage msg = new DoorRequestInfoMessage( 
                this.getNucleus().getCellName()+"@"+
                this.getNucleus().getCellDomainName() ) ;
        msg.setClient( getDoorSocket().getInetAddress().getHostName() );
        msg.setTransactionTime( System.currentTimeMillis()  );

        
        msg.setPath( wrapper.getPath() );
        
        if ( wrapper.getUser() != null) {
        	msg.setOwner( wrapper.getUser() );
        }
        if ( wrapper.getPnfsId() != null) {
        	msg.setPnfsId( wrapper.getPnfsId() );
        } else
        	msg.setPnfsId( new PnfsId("000000000000000000000000") );
        
        if ( wrapper.getErrorCode() != 0 ) {
        	msg.setResult( wrapper.getErrorCode(), wrapper.getErrorMsg() );
        }
        
        msg.setTrasaction( _transactionPrefix + wrapper.getFileHandle() );
		
		try{
            sendMessage( new CellMessage( new CellPath("billing") ,  msg ) ) ;
        }catch(Exception ee){
            esay("Couldn't send billing info : "+ee );
        }
        
        say( msg.toString() );        
	}

    public int getMaxFileOpens() {
        return _maxFileOpens;
    }
}
