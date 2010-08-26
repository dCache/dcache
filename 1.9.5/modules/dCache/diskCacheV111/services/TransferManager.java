/*
 * Class.java
 *
 * Created on November 12, 2004, 11:58 AM
 *
 * $Id: TransferManager.java,v 1.38 2007-07-23 07:53:22 behrmann Exp $
 * $Author: behrmann $
 */

package diskCacheV111.services;

import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsFile;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolSetStickyMessage;

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
import java.util.Properties;
import diskCacheV111.doors.FTPTransactionLog;

import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;
import org.dcache.srm.scheduler.JobIdGenerator;

import diskCacheV111.util.Pgpass;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
/**
 *
 * @author  timur
 */
public abstract class TransferManager extends CellAdapter {

	private String jdbcUrl="jdbc:postgresql://localhost/srmdcache";
	private String jdbcDriver="org.postgresql.Driver";
	private String user="srmdcache";
	private String pass=null;
	private String pwdfile;

	private PersistenceManager pm;

	public HashSet activeTransfersIDs = new HashSet();
	private HashMap activeTransfersIDsToHandlerMap = new HashMap();
	private int max_transfers = 30;
	private int num_transfers = 0;
	private long poolTimeout        = 5 * 60 ;
	private long poolManagerTimeout = 5* 60;
	private long pnfsManagerTimeout = 5* 60;
	private long spaceManagerTimeout = 5* 60;
	private long moverTimeout = 60*60*2; // two hours
	protected static long   nextMessageID = 0;
	private String _TLogRoot = null;
	protected String poolManager ="PoolManager";
	protected String pnfsManager = "PnfsManager";
	protected String spaceManager = "SpaceManager";
	protected CellNucleus  _nucleus ;
	private CellPath poolMgrPath;
	private boolean overwrite=false;
	private boolean do_database_logging=false;
	private int maxNumberOfDeleteRetries=1;


	// this is the timer which will timeout the
	// transfer requests
	private Timer moverTimeoutTimer = new Timer(true);
	private Map moverTimeoutTimerTasks = new Hashtable();
	private String _ioQueueName = null; // multi io queue option
	private JobIdGenerator idGenerator;

	public HashSet justRequestedIDs =  new HashSet();
        private String poolProxy ;


	/** Creates a new instance of Class */

	/**      */
	public TransferManager(String cellName, String argString) throws Exception {
		super(cellName,TransferManager.class.getName(), argString,false);
		say("Calling constructor(TransferManager) : "+cellName);
		Args _args = new Args(argString);

		jdbcUrl    = _args.getOpt("jdbcUrl");
		jdbcDriver = _args.getOpt("jdbcDriver");
		user       = _args.getOpt("dbUser");
		pass       = _args.getOpt("dbPass");
		String db_log = _args.getOpt("doDbLog");
		pwdfile = _args.getOpt("pgPass");
		if (pwdfile != null && !pwdfile.equals("") ) {
			Pgpass pgpass = new Pgpass(pwdfile);      //VP
			pass = pgpass.getPgpass(jdbcUrl, user);   //VP
		}


		if ( db_log != null) {
			if (db_log.equalsIgnoreCase("true") || db_log.equalsIgnoreCase("t")) {
				setDbLogging(true);
			} else if (db_log.equalsIgnoreCase("false") || db_log.equalsIgnoreCase("f")) {
				setDbLogging(false);
			} else {
				esay("Unrecognized value of \"doDbLog\" option : "+db_log+" , ignored");
			}
		}

		try {
			if ( jdbcUrl != null && jdbcDriver != null && user != null && pass != null ) {
                try {
                    RequestsPropertyStorage.initPropertyStorage(jdbcUrl, jdbcDriver, user, pass, "srmnextrequestid");
                } catch (IllegalStateException ise){
                    // already initialized
                }
                idGenerator = JobIdGeneratorFactory.
                        getJobIdGeneratorFactory().getJobIdGenerator();
			} else {
				idGenerator=null;
			}
		} catch (Exception e) {
			esay("Failed to initialize Data Base connection to generate nextTransferId using default values");
			esay("jdbcUrl="+jdbcUrl+" jdbcDriver="+jdbcDriver+" dbUser="+user+" dbPass="+pass+" pgPass="+pwdfile);
			idGenerator=null;
			//esay(e);
		}

		if ( doDbLogging() == true ) {
			try {
				Properties properties = new Properties();
				properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
						       "org.jpox.PersistenceManagerFactoryImpl");
				properties.setProperty("javax.jdo.option.ConnectionDriverName", jdbcDriver);
				properties.setProperty("javax.jdo.option.ConnectionURL",jdbcUrl);
				properties.setProperty("javax.jdo.option.ConnectionUserName",user);
				properties.setProperty("javax.jdo.option.ConnectionPassword",pass);
				properties.setProperty("javax.jdo.option.DetachAllOnCommit","true");
				properties.setProperty("javax.jdo.option.Optimistic","true");
				properties.setProperty("javax.jdo.option.NontransactionalRead","true");
				// 		properties.setProperty("javax.jdo.option.NontransactionalWrite","true");
				properties.setProperty("javax.jdo.option.RetainValues","true");
				properties.setProperty("javax.jdo.option.Multithreaded","true");
				//   javax.jdo.option.Optimistic: false
				//   javax.jdo.option.RetainValues: false
				//   javax.jdo.option.NontransactionalRead: true
				//   javax.jdo.option.NontransactionalWrite: false
				//   javax.jdo.option.RestoreValues: false
				//   javax.jdo.option.IgnoreCache: false
				//   javax.jdo.option.Multithreaded: false
				properties.setProperty("org.jpox.autoCreateSchema","true");
				properties.setProperty("org.jpox.validateTables","false");
				properties.setProperty("org.jpox.validateConstraints","false");
				properties.setProperty("org.jpox.autoCreateColumns","true");

// below is default, supported are "LowerCase", "PreserveCase"
//                properties.setProperty("org.jpox.identifier.case","UpperCase");

				PersistenceManagerFactory pmf =
					JDOHelper.getPersistenceManagerFactory(properties);
				pm = pmf.getPersistenceManager();
			} catch (Exception e) {
				esay("Failed to initialize Data Base connection using default values");
				esay("jdbcUrl="+jdbcUrl+" jdbcDriver="+jdbcDriver+" dbUser="+user+" dbPass="+pass+" pgPass="+pwdfile);
				esay(e);
				pm = null;
				setDbLogging(false);
			}
		}

		String tmpstr = _args.getOpt("tlog");
		if(tmpstr != null) {
			_TLogRoot = tmpstr;
		}
		tmpstr = _args.getOpt("maxNumberOfDeleteRetries");
		if (tmpstr !=null) {
			try {
				maxNumberOfDeleteRetries =Integer.parseInt(tmpstr);
			}
			catch (Exception e) {
				esay("Failed to initialize maxNumberOfDeleteRetriesm, using default value "+maxNumberOfDeleteRetries);
				esay(e);
			}
		}
		tmpstr = _args.getOpt("pool_manager_timeout");
		if(tmpstr != null) {
			poolManagerTimeout =Integer.parseInt(tmpstr);
		}
		tmpstr = _args.getOpt("pnfs_manager_timeout");
		if(tmpstr != null) {
			pnfsManagerTimeout =Integer.parseInt(tmpstr);
		}
		tmpstr = _args.getOpt("pool_timeout");
		if(tmpstr != null) {
			poolTimeout =Integer.parseInt(tmpstr);
		}
		tmpstr = _args.getOpt("max_transfers");
		if(tmpstr != null) {
			max_transfers =Integer.parseInt(tmpstr);
		}
		tmpstr = _args.getOpt("overwrite");
		if(tmpstr != null) {
			overwrite = tmpstr.equalsIgnoreCase("true");
		}
		if(_args.getOpt("poolManager") != null) {
			poolManager = _args.getOpt("poolManager");
		}
		if(_args.getOpt("pnfsManager") != null) {
		     pnfsManager = _args.getOpt("pnfsManager");
		}
		if(_args.getOpt("mover_timeout")!= null) {
			moverTimeout = Long.parseLong(_args.getOpt("mover_timeout"));
		}
		if(_args.getOpt("io-queue")!= null) {
			_ioQueueName = _args.getOpt("io-queue");
		}
		_ioQueueName = ( _ioQueueName == null ) || ( _ioQueueName.length() == 0 ) ? null : _ioQueueName ;

                poolProxy = _args.getOpt("poolProxy");
                say("Pool Proxy "+( poolProxy == null ? "not set" : ( "set to "+poolProxy ) ) );
		poolMgrPath     = new CellPath( poolManager ) ;
		_nucleus  = getNucleus() ;
		useInterpreter(true);
		getNucleus().export();
		start() ;
	}
	/**      */
	public  CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.38 $" ); }

	public void getInfo( PrintWriter printWriter ) {
		StringBuffer sb = new StringBuffer();
		sb.append("    "+getClass().getName()+"\n");
		sb.append("---------------------------------\n");
		sb.append("Name   : ").
			append(_nucleus.getCellName());
		sb.append("\njdbcClass : ").append(jdbcDriver).append('\n');
		sb.append("\njdbcUrl : ").append(jdbcUrl).append('\n');
		sb.append("\njdbcUser : ").append(user).append('\n');
		if ( doDbLogging()==true) {
			sb.append("dblogging=true\n");
		} else {
			sb.append("dblogging=false\n");
		}
		if (idGenerator!=null) {
			sb.append("TransferID is generated using Data Base \n");
		} else {
			sb.append("TransferID is generated w/o DB access \n");
		}
		sb.append("\nnumber of active transfers : ").
			append(num_transfers);
		sb.append("\nmax number of active transfers  : ").
			append(getMax_transfers());
		sb.append("\nPoolManager  : ").
			append(poolManager);
		sb.append("\nPoolManager timeout : ").
			append(poolManagerTimeout).append(" seconds");
		sb.append("\nPnfsManager timeout : ").
			append(pnfsManagerTimeout).append(" seconds");
		sb.append("\nPool timeout  : ").
			append(poolTimeout).append(" seconds");
		sb.append("\nnext id  : ").
			append(nextMessageID);
		sb.append("\nio-queue  : ").
			append(_ioQueueName);
		sb.append("\nmaxNumberofDeleteRetries  : ").
			append(maxNumberOfDeleteRetries);
		sb.append("\nPool Proxy : ").
			append(poolProxy == null ? "not set" : ( "set to "+poolProxy ));
		printWriter.println( sb.toString()) ;
	}
	/**      */
	public String hh_set_dblogging = "<true/false switch db loggin on/off>" ;
	public String ac_set_dblogging_$_1( Args args )throws CommandException {
		String log = args.argv(0) ;
		StringBuffer sb = new StringBuffer();
		if (log.equalsIgnoreCase("true") || log.equalsIgnoreCase("t")) {
			setDbLogging(true);
			sb.append("remote ftp transaction db logging is on\n");
		} else if ( log.equalsIgnoreCase("false") || log.equalsIgnoreCase("f")) {
			setDbLogging(false);
			sb.append("remote ftp transaction db logging is off\n");
		} else {
			return "unrecognized value : \""+log+"\" only true or false are allowed";
		}
		if (doDbLogging()==true && pm==null) {
			sb.append(_nucleus.getCellName()+" has been started w/ db logging disabed\n");
			sb.append("Attempting to initialize JDO Peristsency Manager using parameters provided at startup\n");
			try {
				Properties properties = new Properties();
				properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
						       "org.jpox.PersistenceManagerFactoryImpl");
				properties.setProperty("javax.jdo.option.ConnectionDriverName", jdbcDriver);
				properties.setProperty("javax.jdo.option.ConnectionURL",jdbcUrl);
				properties.setProperty("javax.jdo.option.ConnectionUserName",user);
				properties.setProperty("javax.jdo.option.ConnectionPassword",pass);
				properties.setProperty("javax.jdo.option.DetachAllOnCommit","true");
				properties.setProperty("javax.jdo.option.Optimistic","true");
				properties.setProperty("javax.jdo.option.NontransactionalRead","true");
				properties.setProperty("javax.jdo.option.RetainValues","true");
				properties.setProperty("javax.jdo.option.Multithreaded","true");
				properties.setProperty("org.jpox.autoCreateSchema","true");
				properties.setProperty("org.jpox.validateTables","false");
				properties.setProperty("org.jpox.validateConstraints","false");
				properties.setProperty("org.jpox.autoCreateColumns","true");
// below is default, supported are "LowerCase", "PreserveCase"
//                properties.setProperty("org.jpox.identifier.case","UpperCase");
				PersistenceManagerFactory pmf =
					JDOHelper.getPersistenceManagerFactory(properties);
				pm = pmf.getPersistenceManager();
				sb.append("Success...\n");
			} catch (Exception e) {
                                esay(e);
				sb.append("Failure...\n");
				sb.append("setting doDbLog back to false. \n");
				sb.append("Try to set correct Jdbc driver, username or password for DB connection.\n");
				pm = null;
				setDbLogging(false);
			}

		}
		return sb.toString();
	}
	public String ac_set_jdbcDriver_$_1( Args args )throws CommandException {
		String driver = args.argv(0) ;
		jdbcDriver=driver;
		StringBuffer sb = new StringBuffer();
		sb.append("setting jdbcDriver to "+jdbcDriver+"\n");
		return sb.toString();
	}
	public String ac_set_maxNumberOfDeleteRetries_$_1( Args args )throws CommandException {
		StringBuffer sb = new StringBuffer();
		String tmpstr = args.argv(0) ;
		try {
			maxNumberOfDeleteRetries =Integer.parseInt(tmpstr);
		}
		catch (Exception e) {
			esay("Failed to initialize maxNumberOfDeleteRetries, using default value "+maxNumberOfDeleteRetries);
			esay(e);
			sb.append("Failed to initialize maxNumberOfDeleteRetries, using default value "+maxNumberOfDeleteRetries+"\n");
			sb.append(e.getMessage());
			return sb.toString();
		}
		sb.append("setting maxNumberOfDeleteRetries "+maxNumberOfDeleteRetries+"\n");
		return sb.toString();
	}
	public String ac_set_jdbcUrl_$_1( Args args )throws CommandException {
		jdbcUrl  = args.argv(0) ;
		StringBuffer sb = new StringBuffer();
		sb.append("setting jdbcUrl to "+jdbcUrl+"\n");
		return sb.toString();
	}
	public String ac_set_dbUser_$_1( Args args )throws CommandException {
		user  = args.argv(0) ;
		StringBuffer sb = new StringBuffer();
		sb.append("setting db to "+user+"\n");
		return sb.toString();
	}
	public String ac_set_dbpass_$_1( Args args )throws CommandException {
		pass  = args.argv(0) ;
		return "OK";
	}

	public String ac_dbinit_$_0( Args args ) throws CommandException {
		if ( idGenerator != null ) {
			return "database connection is already initialized\n";
		}
		try {
              try {
                    RequestsPropertyStorage.initPropertyStorage(jdbcUrl, jdbcDriver, user, pass, "srmnextrequestid");
                } catch (IllegalStateException ise){
                    // already initialized
                }
                idGenerator = RequestsPropertyStorage.
                        getJobIdGeneratorFactory().getJobIdGenerator();
		} catch (Exception e) {
			esay("Failed to initialize Data Base connection to generate nextTransferId\n");
			idGenerator=null;
			esay(e);
			return "Failed to initialize Data Base connection to generate nextTransferId\n";
		}
		return "OK";
	}
	/**      */
	public String hh_set_tlog = "<direcory for ftp logs or \"null\" for none>" ;
	public String ac_set_tlog_$_1( Args args )throws CommandException {
		_TLogRoot = args.argv(0) ;
		if(_TLogRoot.equals("null")) {
			_TLogRoot = null;
			return "remote ftp transaction logging is off";
		}
		return "remote ftp transactions will be logged to "+_TLogRoot;
	}
	/**      */
	public String hh_set_max_transfers = "<#max transfers>" ;
	public String ac_set_max_transfers_$_1( Args args )throws CommandException {
		int max_transfs = Integer.parseInt(args.argv(0)) ;
		if(max_transfs <= 0) {
			return "Error, max transfers number should be greater then 0 ";
		}
		setMax_transfers(max_transfs);
		return "set maximum number of active transfers to "+max_transfs;
	}
	/**      */
	public String hh_set_pool_timeout = "<#seconds>" ;
	public String ac_set_pool_timeout_$_1( Args args )throws CommandException {
		int timeout = Integer.parseInt(args.argv(0)) ;
		if(timeout <= 0) {
			return "Error, pool timeout should be greater then 0 ";
		}
		poolTimeout = timeout;
		return "set pool timeout to "+timeout+ " seconds";
	}
	/**      */
	public String hh_set_pool_manager_timeout = "<#seconds>" ;
	public String ac_set_pool_manager_timeout_$_1( Args args )throws CommandException {
		int timeout = Integer.parseInt(args.argv(0)) ;
		if(timeout <= 0) {
			return "Error, pool manger timeout should be greater then 0 ";
		}
		poolManagerTimeout = timeout;
		return "set pool manager timeout to "+timeout+ " seconds";
	}
	/**      */
	public String hh_set_pnfs_manager_timeout = "<#seconds>" ;
	public String ac_set_pnfs_manager_timeout_$_1( Args args )throws CommandException {
		int timeout = Integer.parseInt(args.argv(0)) ;
		if(timeout <= 0) {
			return "Error, pnfs manger timeout should be greater then 0 ";
		}
		pnfsManagerTimeout = timeout;
		return "set pnfs manager timeout to "+timeout+ " seconds";
	}
	/**      */
	public String hh_ls = "[-l] [<#transferId>]" ;
	public String ac_ls_$_0_1( Args args )throws CommandException {
		boolean long_format = args.getOpt("l") != null;
		Long id = null;
		if(args.argc() >0) {
			id = new Long(Long.parseLong(args.argv(0)));
		}
		if(id != null) {
			synchronized(activeTransfersIDs) {
				if(!activeTransfersIDs.contains(id)) {
					return "ID not found : "+id;
				}
				TransferManagerHandler handler =
					(TransferManagerHandler)
					activeTransfersIDsToHandlerMap.get(id);
				return " transfer id="+id+" : "+
					handler.toString(long_format);
			}
		}
		StringBuffer sb =  new StringBuffer();
		synchronized(activeTransfersIDs) {
			if(activeTransfersIDs.isEmpty()) {
				return "No Active Transfers";
			}
			sb.append("  Active Transfers ");
			Iterator iter = activeTransfersIDs.iterator();
			while(iter.hasNext()) {
				id = (Long) iter.next();
				TransferManagerHandler transferHandler =
					(TransferManagerHandler)
					activeTransfersIDsToHandlerMap.get(id);
				sb.append("\n#").append(id);
				sb.append(" ").append( transferHandler.toString(long_format));
			}

		}
		return sb.toString();
	}
	/**      */
	public String hh_kill = " id" ;
	public String ac_kill_$_1( Args args )throws CommandException {
		Long id = new Long(Long.parseLong(args.argv(0)));
		TransferManagerHandler transferHandler;
		synchronized(activeTransfersIDs) {
			if(!activeTransfersIDs.contains(id)) {
				return "ID not found : "+id;
			}
			transferHandler =
				(TransferManagerHandler)
				activeTransfersIDsToHandlerMap.get(id);
		}
		transferHandler.cancel(null);
		return "this will kill the running mover or the mover queued on the pool!!!\n"+
			"killing the Transfer:\n"+
			transferHandler.toString(true);
	}
	/**      */
	public String hh_killall = " [-p pool] pattern [pool] \n"+
	" for example killall .* ketchup will kill all transfers with movers on the ketchup pool" ;
	public String ac_killall_$_1_2( Args args )throws CommandException {
		try {
			Pattern p = Pattern.compile(args.argv(0));
			String pool = null;
			if(args.argc() >1) {
				pool = args.argv(1);
			}
			Set handlersToKill = new HashSet();
			synchronized(activeTransfersIDs) {
				for(Iterator i = activeTransfersIDs.iterator(); i.hasNext();) {
					Long longid = (Long)i.next();
					String stringid=longid.toString();
					Matcher m = p.matcher(stringid.toString());
					if( m.matches()) {
						say("pattern: \""+args.argv(0)+"\" matches id=\""+stringid+"\"");
						TransferManagerHandler transferHandler =
							(TransferManagerHandler)
							activeTransfersIDsToHandlerMap.get(longid);
						String handlerPool = transferHandler.getPool();
						if (pool != null && pool.equals(handlerPool)) {
							handlersToKill.add(transferHandler);
						} else if(pool == null ) {
							handlersToKill.add(transferHandler);
						}
					} else {
						say("pattern: \""+args.argv(0)+"\" does not match id=\""+stringid+"\"");
					}
				}
			}
			if(handlersToKill.isEmpty()) {
				return "no active transfers match the pattern and the pool";
			}
			StringBuffer sb = new StringBuffer("Killing these transfers: \n");
			for(Iterator i = handlersToKill.iterator(); i.hasNext();) {
				TransferManagerHandler transferHandler =
					(TransferManagerHandler)i.next();
				transferHandler.cancel();
				sb.append(transferHandler.toString(true)).append('\n');
			}
			return sb.toString();
		} catch(Exception e) {
			esay(e);
			return e.toString();
		}
	}
	/**      */
	public String hh_set_io_queue = "<io-queue name >" ;
	public String ac_set_io_queue_$_1( Args args ) throws CommandException {
		String newIoQueueName = args.argv(0) ;
		if(newIoQueueName.equals("null")) {
			_ioQueueName = null;
			return "io-queue is set to null";
		}
		_ioQueueName=newIoQueueName;
		return "io_queue was set to "+_ioQueueName;
	}
	/**      */
	public abstract boolean _messageArrived(CellMessage cellMessage ) ;
	public void messageArrived( CellMessage cellMessage ) {
		say("messageArrived(TransferManager)");
		Object object = cellMessage.getMessageObject();
		if (! (object instanceof Message) ){
			say("Unexpected message class "+object.getClass());
			return;
		}
		Message transferMessage = (Message)object ;
		boolean replyRequired = transferMessage.getReplyRequired() ;
		say("Message messageArrived ["+object.getClass()+"]="+object.toString());
		say("Message messageArrived source = "+cellMessage.getSourceAddress());
		if (_messageArrived(cellMessage)) {
			return;
		} else if (object instanceof DoorTransferFinishedMessage) {
			long id = ((DoorTransferFinishedMessage)object ).getId();
			TransferManagerHandler h = getHandler(id);
			if(h != null) {
				h.poolDoorMessageArrived((DoorTransferFinishedMessage)object );
				return;
			} else {
				esay("can not find handler with id="+id);
			}
		} else if(transferMessage instanceof CancelTransferMessage) {
			CancelTransferMessage cancel = (CancelTransferMessage)object;
			long id = ((CancelTransferMessage)object ).getId();
			TransferManagerHandler h = getHandler(id);
			if(h != null) {
				h.cancel(cancel );
				return;
			} else {
				esay("can not find handler with id="+id);
			}
		} else if(transferMessage instanceof TransferManagerMessage) {
			if(new_transfer()) {
				handleTransfer((TransferManagerMessage) transferMessage,cellMessage.getSourceAddress());
			} else {
				transferMessage.setFailed(TransferManagerMessage.TOO_MANY_TRANSFERS,"too many transfers!");
			}
		} else {
			super.messageArrived(cellMessage);
			return;
		}
		if( replyRequired ) {
			try {
				say("Sending reply "+transferMessage);
				cellMessage.revertDirection();
				sendMessage(cellMessage);
			} catch (Exception e) {
				esay("Can't reply message : "+e);
			}
		}
	}
	/**      */
	private void handleTransfer(TransferManagerMessage message, CellPath sourcePath) {
		say("handleTransfer(TransferManager)");
		TransferManagerHandler h = new TransferManagerHandler(this,message,sourcePath);
		h.handle();
		return;
	}
	/**      */
	private int askForFile( String       pool ,
				PnfsId       pnfsId ,
				StorageInfo  storageInfo ,
				ProtocolInfo protocolInfo ,
				boolean      isWrite,
				long id) throws CacheException {
		say("askForFile(TransferManager) "+pool+" "+pnfsId.toString());
		say("Trying pool "+pool+" for "+(isWrite?"Write":"Read"));
		say("Trying pool "+pool+" for "+(isWrite?"Write":"Read"));

		PoolIoFileMessage poolMessage ;
		if( isWrite ) {
			poolMessage =         new PoolAcceptFileMessage(
				pool,
				pnfsId.toString(),
				protocolInfo,
				storageInfo);
		} else {
			poolMessage =        new PoolDeliverFileMessage(
				pool,
				pnfsId.toString() ,
				protocolInfo ,
				storageInfo     );
		}
		if( _ioQueueName != null ) {
			poolMessage.setIoQueueName( _ioQueueName ) ;
		}
		poolMessage.setId( id ) ;
		CellMessage reply;

                CellPath poolCellPath;
                if( poolProxy == null ){
			poolCellPath = new CellPath(pool);
                }else{
			poolCellPath = new CellPath(poolProxy);
			poolCellPath.add(pool);
                }

		try {
			reply= sendAndWait(new CellMessage(
						   poolCellPath ,
						   poolMessage
						   )  ,
					   poolTimeout*1000
				) ;
		} catch(Exception e) {
			esay(e);
			throw new CacheException(e.toString());
		}
		if( reply == null)
			throw new
				CacheException( "Pool request timed out : "+pool ) ;
		Object replyObject = reply.getMessageObject();
		if( ! ( replyObject instanceof PoolIoFileMessage ) )
			throw new
				CacheException( "Illegal Object received : "+
						replyObject.getClass().getName());
		PoolIoFileMessage poolReply = (PoolIoFileMessage)replyObject;
		if (poolReply.getReturnCode() != 0)
			throw new
				CacheException( "Pool error: "+poolReply.getErrorObject() ) ;
		say("Pool "+pool+" will deliver file "+pnfsId +" mover id is "+poolReply.getMoverId());
		return poolReply.getMoverId();
	}
	/**      */
	public int getMax_transfers() {
		return max_transfers;
	}
	/**      */
	public void setMax_transfers(int max_transfers) {
		this.max_transfers = max_transfers;
	}
	/**      */
	private synchronized boolean new_transfer()  {
			say("new_transfer() num_transfers = "+num_transfers+
			    " max_transfers="+max_transfers);
			if(num_transfers == max_transfers) {
				say("new_transfer() returns false");
				return false;
			}
			say("new_transfer() INCREMENT and return true");
			num_transfers++;
			return true;
		}
	/**      */
	private synchronized int active_transfers() {
		return num_transfers;
	}
	/**      */
	public synchronized void finish_transfer() {
		say("finish_transfer() num_transfers = "+num_transfers+" DECREMENT");
		num_transfers--;
	}
	/**      */
	public synchronized long getNextMessageID() {
		if (idGenerator!=null) {
			try {
				nextMessageID=idGenerator.nextLong();
			} catch (Exception e)  {
				esay("Having trouble getting getNextMessageID from DB");
				esay(e);
				esay("will nullify requestsPropertyStorage");
				idGenerator=null;
				getNextMessageID();
			}
		} else {
			if(nextMessageID == Long.MAX_VALUE)  {
				nextMessageID = 0;
				return Long.MAX_VALUE;
			}
			return nextMessageID++;
		}
		return nextMessageID;
	}
	/**      */
	protected abstract IpProtocolInfo getProtocolInfo(long callerId, TransferManagerMessage transferRequest)
		throws java.io.IOException;
	/**      */
	protected TransferManagerHandler getHandler(long handlerId) {
		Long longId = new Long(handlerId);
		synchronized(activeTransfersIDs) {
			if(!activeTransfersIDs.contains(longId)) {
				return null;
			}
			return (TransferManagerHandler) activeTransfersIDsToHandlerMap.get(longId);
		}
	}

	/**      */
	public void startTimer(long id) {
		final Long lid = new Long(id);
		TimerTask tt = new TimerTask() {
				public void run() {
					esay("timer for handler "+lid+" has expired, killing");
					Object o = moverTimeoutTimerTasks.remove(lid);
					if(o == null) {
						esay("TimerTask.run(): timer task for handler Id="+lid+" not found in moverTimoutTimerTasks hashtable");
						return;
					}
					TransferManagerHandler handler = getHandler(lid.longValue());
					if(handler == null) {
						esay("TimerTask.run(): timer task for handler Id="+lid+" could not find handler !!!");
						return;
					}
					handler.timeout();
				}
			};

		moverTimeoutTimerTasks.put(lid, tt);

		// this is very approximate
		// but we do not need hard real time
		// note that the movertimeout is in seconds,
		// so we need to multiply by 1000
		moverTimeoutTimer.schedule(tt,moverTimeout*1000L);
	}

	public void stopTimer(long id) {
		final Long lid = new Long(id);
		Object o = moverTimeoutTimerTasks.remove(lid);
		if(o == null) {
			esay("stopTimer(): timer not found for Id="+id);
			return;
		}
		say("canceling the mover timer for handler id "+id);
		TimerTask tt = (TimerTask)o;
		tt.cancel();
	}


	public void addActiveTransfer(Long id,
				      TransferManagerHandler handler) {
		activeTransfersIDs.add(id);
		activeTransfersIDsToHandlerMap.put(id,handler);
		if (doDbLogging() && pm !=null) {
			synchronized(pm) {
				Transaction tx = pm.currentTransaction();
				try {
					tx.begin();
					pm.makePersistent(handler);
					// Detach the handler for use
					// working_handler = (TransferManagerHandler)pm.detachCopy(handler);
					tx.commit();
					say("Recording new handler into database.");
				} catch (Exception e) {
					esay(e);
				} finally {
					rollbackIfActive(tx);
				}
			}
		}
	}

	public void removeActiveTransfer(Long id) {
		activeTransfersIDs.remove(id);
		TransferManagerHandler handler =
			(TransferManagerHandler)
			activeTransfersIDsToHandlerMap.get(id);
		if (doDbLogging() && pm!=null) {
			synchronized(pm) {
				TransferManagerHandlerBackup handlerBackup = new TransferManagerHandlerBackup(handler);
				Transaction tx = pm.currentTransaction();
				try {
					tx.begin();
					pm.makePersistent(handler);
					pm.deletePersistent(handler);
					pm.makePersistent(handlerBackup);
					tx.commit();
					say("handler removed from db");
				} catch (Exception e) {
					esay(e);
				} finally {
					rollbackIfActive(tx);
				}
				activeTransfersIDsToHandlerMap.remove(id);
			}
		}
	}

	public int getMaxTransfers() {
		return  max_transfers;
	}

	public int getNumberOfTranfers() {
		return num_transfers;
	}

	public long getPoolTimeout() {
		return poolTimeout;
	}

	public long getPoolManagerTimeout() {
		return poolManagerTimeout;
	}

	public long getPnfsManagerTimeout() {
		return pnfsManagerTimeout;
	}

	public long getSpaceManagerTimeout() {
		return spaceManagerTimeout;
	}

	public long getMoverTimeout() {
		return moverTimeout;
	}

	public String getLogRootName() {
		return _TLogRoot;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public CellPath getPoolManagerPath() {
		return poolMgrPath;
	}

	public String getPoolManagerName() {
		return poolManager;
	}

	public String getPnfsManagerName() {
		return pnfsManager;
	}

	public String getSpaceManagerName() {
		return spaceManager;
	}

	public String getIoQueueName() {
		return _ioQueueName;
	}

	public synchronized PersistenceManager getPersistenceManager() {
		return pm;
	}

	public static void rollbackIfActive(Transaction tx) {
		if (tx != null && tx.isActive()) {
			tx.rollback();
		}
	}
	public boolean doDbLogging() {
		return do_database_logging;
	}
	public void  setDbLogging(boolean yes) {
		do_database_logging=yes;
	}
	public void setMaxNumberOfDeleteRetries(int nretries) {
		maxNumberOfDeleteRetries=nretries;
	}
	public int getMaxNumberOfDeleteRetries() {
		return maxNumberOfDeleteRetries;
	}

	public void persist(Object o) {
		if (doDbLogging() && pm!=null) {
			synchronized(pm) {
				Transaction tx = pm.currentTransaction();
				try {
					tx.begin();
					pm.makePersistent(o);
					tx.commit();
					say("["+o.toString()+"]: Recording new state of handler into database.");
				} catch (Exception e) {
					esay("["+o.toString()+"]: failed to persist obhject "+o.toString());
					esay(e);
				} finally {
					rollbackIfActive(tx);
				}
			}
		}
	}

    public String getPoolProxy() {
        return poolProxy;
    }

    public void setPoolProxy(String poolProxy) {
        this.poolProxy = poolProxy;
    }
}

