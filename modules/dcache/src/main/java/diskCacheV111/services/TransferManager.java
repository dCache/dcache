/* -*- c-basic-offset: 8; indent-tabs-mode: nil -*- */
package diskCacheV111.services;

import com.google.common.base.Strings;
import com.jolbox.bonecp.BoneCPDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.Pgpass;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;

import org.dcache.cells.AbstractCell;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.scheduler.JobIdGenerator;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;

/**
 * Base class for services that transfer files on behalf of SRM. Used
 * to implement server-side srmCopy.
 */
public abstract class TransferManager extends AbstractCell
{
        private static final Logger log = LoggerFactory.getLogger(TransferManager.class);

	private String _jdbcUrl="jdbc:postgresql://localhost/srmdcache";
	private String _jdbcDriver="org.postgresql.Driver";
	private String _user="srmdcache";
	private String _pass;
	private String _pwdFile;

	private PersistenceManager _pm;

	private final Map<Long,TransferManagerHandler> _activeTransfers =
                new ConcurrentHashMap<>();
	private int _maxTransfers;
	private int _numTransfers;
	private long _poolTimeout;
	private long _poolManagerTimeout;
	private long _pnfsManagerTimeout;
	private long _moverTimeout;
	protected static long  nextMessageID;
	private String _tLogRoot;
	protected String _poolManager;
	protected String _pnfsManager;
	private CellPath _poolMgrPath;
	private boolean _overwrite;
	private boolean _doDatabaseLogging;
	private int _maxNumberOfDeleteRetries;

	// this is the timer which will timeout the
	// transfer requests
	private final Timer _moverTimeoutTimer = new Timer("Mover timeout timer", true);
	private final Map<Long,TimerTask> _moverTimeoutTimerTasks =
                new ConcurrentHashMap<>();
	private String _ioQueueName; // multi io queue option
	private JobIdGenerator idGenerator;

	public final Set<PnfsId> justRequestedIDs = new HashSet<>();
        private String _poolProxy;


	/** Creates a new instance of Class */

        public TransferManager(String cellName, String args)
                throws InterruptedException, ExecutionException
        {
                super(cellName, args);
                doInit();
        }

        @Override
        protected void init()
        {
		Args args = getArgs();

		_jdbcUrl    = args.getOpt("jdbcUrl");
		_jdbcDriver = args.getOpt("jdbcDriver");
		_user       = args.getOpt("dbUser");
		_pass       = args.getOpt("dbPass");
		_pwdFile = args.getOpt("pgPass");
		if (_pwdFile != null && !_pwdFile.isEmpty() ) {
			Pgpass pgpass = new Pgpass(_pwdFile);
			_pass = pgpass.getPgpass(_jdbcUrl, _user);
		}

		String dbLog = args.getOpt("doDbLog");
                if (dbLog.equalsIgnoreCase("true") || dbLog.equalsIgnoreCase("t")) {
                        setDbLogging(true);
                } else if (dbLog.equalsIgnoreCase("false") || dbLog.equalsIgnoreCase("f")) {
                        setDbLogging(false);
                } else {
                        log.error("Unrecognized value of \"doDbLog\" option : "+dbLog+" , ignored");
		}

		try {
			if ( _jdbcUrl != null && _jdbcDriver != null && _user != null && _pass != null ) {
                            initIdGenerator();
			} else {
                            idGenerator=null;
			}
		} catch (Exception e) {
			log.error("Failed to initialize Data Base connection to generate nextTransferId using default values");
			log.error("jdbcUrl="+_jdbcUrl+" jdbcDriver="+_jdbcDriver+" dbUser="+_user+" dbPass="+_pass+" pgPass="+_pwdFile);
			idGenerator=null;
			//logString.error(e);
		}

		if (doDbLogging()) {
			try {
				Properties properties = new Properties();
				properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
						       "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
				properties.setProperty("javax.jdo.option.ConnectionDriverName", _jdbcDriver);
				properties.setProperty("javax.jdo.option.ConnectionURL",_jdbcUrl);
				properties.setProperty("javax.jdo.option.ConnectionUserName",_user);
				properties.setProperty("javax.jdo.option.ConnectionPassword",_pass);
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
				properties.setProperty("datanucleus.autoCreateSchema","true");
				properties.setProperty("datanucleus.validateTables","false");
				properties.setProperty("datanucleus.validateConstraints","false");
				properties.setProperty("datanucleus.autoCreateColumns","true");

                                // below is default, supported are "LowerCase", "PreserveCase"
                                //  properties.setProperty("datanucleus.identifier.case","UpperCase");

				PersistenceManagerFactory pmf =
					JDOHelper.getPersistenceManagerFactory(properties);
				_pm = pmf.getPersistenceManager();
			} catch (Exception e) {
				log.error("Failed to initialize Data Base connection using default values");
				log.error("jdbcUrl="+_jdbcUrl+" jdbcDriver="+_jdbcDriver+" dbUser="+_user+" dbPass="+_pass+" pgPass="+_pwdFile);
				log.error(e.toString());
				_pm = null;
				setDbLogging(false);
			}
		}

		_tLogRoot = Strings.emptyToNull(args.getOpt("tlog"));
                _maxNumberOfDeleteRetries = args.getIntOption("maxNumberOfDeleteRetries");
		_poolManagerTimeout = TimeUnit.MILLISECONDS.convert(args.getIntOption("pool_manager_timeout"),
                                                                    TimeUnit.valueOf(args.getOpt("pool_manager_timeout_unit")));
		_pnfsManagerTimeout = TimeUnit.MILLISECONDS.convert(args.getIntOption("pnfs_timeout"),
                                                                    TimeUnit.valueOf(args.getOpt("pnfs_timeout_unit")));
		_poolTimeout = TimeUnit.MILLISECONDS.convert(args.getIntOption("pool_timeout"),
                                                             TimeUnit.valueOf(args.getOpt("pool_timeout_unit")));
		_maxTransfers = args.getIntOption("max_transfers");
		_overwrite = Strings.nullToEmpty(args.getOpt("overwrite")).equalsIgnoreCase("true");
                _poolManager = args.getOpt("poolManager");
                _pnfsManager = args.getOpt("pnfsManager");
                _moverTimeout = TimeUnit.MILLISECONDS.convert(args.getIntOption("mover_timeout"),
                                                              TimeUnit.valueOf(args.getOpt("mover_timeout_unit")));
                _ioQueueName = Strings.emptyToNull(args.getOpt("io-queue"));
                _poolProxy = args.getOpt("poolProxy");
                log.debug("Pool Proxy " + (_poolProxy == null ? "not set" : ("set to " + _poolProxy)));
		_poolMgrPath = new CellPath( _poolManager ) ;
	}

    private void initIdGenerator()
    {
        try {
            final BoneCPDataSource ds = new BoneCPDataSource();
            ds.setDriverClass(_jdbcDriver);
            ds.setJdbcUrl(_jdbcUrl);
            ds.setUsername(_user);
            ds.setPassword(_pass);
            ds.setIdleConnectionTestPeriodInMinutes(60);
            ds.setIdleMaxAgeInMinutes(240);
            ds.setMaxConnectionsPerPartition(50);
            ds.setPartitionCount(1);
            ds.setAcquireIncrement(5);
            ds.setStatementsCacheSize(100);

            RequestsPropertyStorage.initPropertyStorage(
                    new DataSourceTransactionManager(ds), ds, "srmnextrequestid");
        } catch (IllegalStateException ise){
                // already initialized
        }
        idGenerator = JobIdGeneratorFactory.
                getJobIdGeneratorFactory().getJobIdGenerator();
    }

    @Override
	public void getInfo(PrintWriter pw)
        {
		pw.printf("    %s\n", getClass().getName());
		pw.println("---------------------------------");
		pw.printf("Name   : %s\n", getCellName());
		pw.printf("jdbcClass : %s\n", _jdbcDriver);
		pw.printf("jdbcUrl : %s\n", _jdbcUrl);
		pw.printf("jdbcUser : %s\n", _user);
		if (doDbLogging()) {
			pw.println("dblogging=true");
		} else {
			pw.println("dblogging=false");
		}
		if (idGenerator!=null) {
			pw.println("TransferID is generated using Data Base");
		} else {
			pw.println("TransferID is generated w/o DB access");
		}
		pw.printf("number of active transfers : %d\n", _numTransfers);
		pw.printf("max number of active transfers  : %d\n", getMaxTransfers());
		pw.printf("PoolManager  : %s\n", _poolManager);
		pw.printf("PoolManager timeout : %d seconds\n", _poolManagerTimeout);
		pw.printf("PnfsManager timeout : %d seconds\n", _pnfsManagerTimeout);
		pw.printf("Pool timeout  : %d seconds\n", _poolTimeout);
		pw.printf("next id  : %d seconds\n", nextMessageID);
		pw.printf("io-queue  : %s\n", _ioQueueName);
		pw.printf("maxNumberofDeleteRetries  : %d\n", _maxNumberOfDeleteRetries);
		pw.printf("Pool Proxy : %s\n",
                          (_poolProxy == null ? "not set" : ("set to " + _poolProxy)));
	}

	public final static String hh_set_dblogging = "<true/false switch db loggin on/off>";
	public String ac_set_dblogging_$_1( Args args )
        {
		String logString = args.argv(0) ;
		StringBuilder sb = new StringBuilder();
		if (logString.equalsIgnoreCase("true") || logString.equalsIgnoreCase("t")) {
			setDbLogging(true);
			sb.append("remote ftp transaction db logging is on\n");
		} else if (logString.equalsIgnoreCase("false") || logString.equalsIgnoreCase("f")) {
			setDbLogging(false);
			sb.append("remote ftp transaction db logging is off\n");
		} else {
			return "unrecognized value : \""+logString+"\" only true or false are allowed";
		}
		if (doDbLogging()==true && _pm==null) {
			sb.append(getCellName())
                                .append(" has been started w/ db logging disabed\n");
			sb.append("Attempting to initialize JDO Peristsency Manager using parameters provided at startup\n");
			try {
				Properties properties = new Properties();
				properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
						       "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
				properties.setProperty("javax.jdo.option.ConnectionDriverName", _jdbcDriver);
				properties.setProperty("javax.jdo.option.ConnectionURL",_jdbcUrl);
				properties.setProperty("javax.jdo.option.ConnectionUserName",_user);
				properties.setProperty("javax.jdo.option.ConnectionPassword",_pass);
				properties.setProperty("javax.jdo.option.DetachAllOnCommit","true");
				properties.setProperty("javax.jdo.option.Optimistic","true");
				properties.setProperty("javax.jdo.option.NontransactionalRead","true");
				properties.setProperty("javax.jdo.option.RetainValues","true");
				properties.setProperty("javax.jdo.option.Multithreaded","true");
				properties.setProperty("datanucleus.autoCreateSchema","true");
				properties.setProperty("datanucleus.validateTables","false");
				properties.setProperty("datanucleus.validateConstraints","false");
				properties.setProperty("datanucleus.autoCreateColumns","true");
// below is default, supported are "LowerCase", "PreserveCase"
//                properties.setProperty("datanucleus.identifier.case","UpperCase");
				PersistenceManagerFactory pmf =
					JDOHelper.getPersistenceManagerFactory(properties);
				_pm = pmf.getPersistenceManager();
				sb.append("Success...\n");
			} catch (Exception e) {
                                log.error(e.toString());
				sb.append("Failure...\n");
				sb.append("setting doDbLog back to false. \n");
				sb.append("Try to set correct Jdbc driver, username or password for DB connection.\n");
				_pm = null;
				setDbLogging(false);
			}
		}
		return sb.toString();
	}

	public String ac_set_jdbcDriver_$_1(Args args)
        {
		_jdbcDriver = args.argv(0);
		return "setting jdbcDriver to " + _jdbcDriver;
	}

	public String ac_set_maxNumberOfDeleteRetries_$_1(Args args)
        {
                _maxNumberOfDeleteRetries = Integer.parseInt(args.argv(0));
		return "setting maxNumberOfDeleteRetries " +_maxNumberOfDeleteRetries;
	}

	public String ac_set_jdbcUrl_$_1(Args args)
        {
		_jdbcUrl = args.argv(0);
		return "setting jdbcUrl to " + _jdbcUrl;
	}

	public String ac_set_dbUser_$_1(Args args)
        {
		_user = args.argv(0);
		return "setting db to " + _user;
	}

	public String ac_set_dbpass_$_1(Args args)
        {
		_pass = args.argv(0);
		return "OK";
	}

	public String ac_dbinit_$_0(Args args)
        {
		if ( idGenerator != null ) {
			return "database connection is already initialized\n";
		}
		try {
                    initIdGenerator();
		} catch (Exception e) {
			log.error("Failed to initialize Data Base connection to generate nextTransferId\n");
			idGenerator=null;
			log.error(e.toString());
			return "Failed to initialize Data Base connection to generate nextTransferId\n";
		}
		return "OK";
	}

	public final static String hh_set_tlog = "<direcory for ftp logs or \"null\" for none>";
	public String ac_set_tlog_$_1( Args args )
        {
		_tLogRoot = args.argv(0) ;
		if(_tLogRoot.equals("null")) {
			_tLogRoot = null;
			return "remote ftp transaction logging is off";
		}
		return "remote ftp transactions will be logged to "+_tLogRoot;
	}

	public final static String hh_set_max_transfers = "<#max transfers>";
	public String ac_set_max_transfers_$_1(Args args)
        {
		int max = Integer.parseInt(args.argv(0)) ;
		if (max <= 0) {
			return "Error, max transfers number should be greater then 0 ";
		}
		setMaxTransfers(max);
		return "set maximum number of active transfers to " + max;
	}

	public final static String hh_set_pool_timeout = "<#seconds>";
	public String ac_set_pool_timeout_$_1(Args args)
        {
		int timeout = Integer.parseInt(args.argv(0)) ;
		if (timeout <= 0) {
			return "Error, pool timeout should be greater then 0 ";
		}
		_poolTimeout = timeout;
		return "set pool timeout to "+timeout+ " seconds";
	}

	public final static String hh_set_pool_manager_timeout = "<#seconds>";
	public String ac_set_pool_manager_timeout_$_1(Args args)
        {
		int timeout = Integer.parseInt(args.argv(0)) ;
		if (timeout <= 0) {
			return "Error, pool manger timeout should be greater then 0 ";
		}
		_poolManagerTimeout = timeout;
		return "set pool manager timeout to "+timeout+ " seconds";
	}

	public final static String hh_set_pnfs_manager_timeout = "<#seconds>";
	public String ac_set_pnfs_manager_timeout_$_1(Args args)
        {
		int timeout = Integer.parseInt(args.argv(0)) ;
		if (timeout <= 0) {
			return "Error, pnfs manger timeout should be greater then 0 ";
		}
		_pnfsManagerTimeout = timeout;
		return "set pnfs manager timeout to "+timeout+ " seconds";
	}

	public final static String hh_ls = "[-l] [<#transferId>]";
	public String ac_ls_$_0_1(Args args)
        {
		boolean long_format = args.hasOption("l");
		if (args.argc() >0) {
			long id = Long.parseLong(args.argv(0));
                        TransferManagerHandler handler = _activeTransfers.get(id);
                        if (handler == null) {
                                return "ID not found : " + id;
                        }
                        return " transfer id=" + id + " : " + handler.toString(long_format);
		}
		StringBuilder sb =  new StringBuilder();
                if (_activeTransfers.isEmpty()) {
                        return "No Active Transfers";
                }
                sb.append("  Active Transfers ");
                for (Map.Entry<Long,TransferManagerHandler> e: _activeTransfers.entrySet()) {
                        sb.append("\n#").append(e.getKey());
                        sb.append(" ").append(e.getValue().toString(long_format));
                }
		return sb.toString();
	}

	public final static String hh_kill = " id";
	public String ac_kill_$_1(Args args)
        {
		long id = Long.parseLong(args.argv(0));
		TransferManagerHandler handler = _activeTransfers.get(id);
                if (handler == null) {
                        return "ID not found : "+id;
                }
		handler.cancel(null);
		return "this will kill the running mover or the mover queued on the pool!!!\n"+
			"killing the Transfer:\n"+ handler.toString(true);
	}

	public final static String hh_killall = " [-p pool] pattern [pool] \n"+
	" for example killall .* ketchup will kill all transfers with movers on the ketchup pool" ;
	public String ac_killall_$_1_2(Args args)
        {
		try {
			Pattern p = Pattern.compile(args.argv(0));
			String pool = null;
			if (args.argc() >1) {
				pool = args.argv(1);
			}
			List<TransferManagerHandler> handlersToKill =
                                new ArrayList<>();
                        for (Map.Entry<Long,TransferManagerHandler> e: _activeTransfers.entrySet()) {
                                long id = e.getKey();
                                TransferManagerHandler handler = e.getValue();
                                Matcher m = p.matcher(String.valueOf(id));
                                if (m.matches()) {
                                        log.debug("pattern: \"{}\" matches id=\"{}\"", args.argv(0), id);
                                        if (pool != null && pool.equals(handler.getPool())) {
                                                handlersToKill.add(handler);
                                        } else if (pool == null) {
                                                handlersToKill.add(handler);
                                        }
                                } else {
                                        log.debug("pattern: \"{}\" does not match id=\"{}\"", args.argv(0), id);
                                }
			}
			if (handlersToKill.isEmpty()) {
				return "no active transfers match the pattern and the pool";
			}
			StringBuilder sb = new StringBuilder("Killing these transfers: \n");
                        for (TransferManagerHandler handler: handlersToKill) {
				handler.cancel();
				sb.append(handler.toString(true)).append('\n');
			}
			return sb.toString();
		} catch (Exception e) {
			log.error(e.toString());
			return e.toString();
		}
	}

	public final static String hh_set_io_queue = "<io-queue name >";
	public String ac_set_io_queue_$_1(Args args)
        {
		String newIoQueueName = args.argv(0) ;
		if (newIoQueueName.equals("null")) {
			_ioQueueName = null;
			return "io-queue is set to null";
		}
		_ioQueueName=newIoQueueName;
		return "io_queue was set to "+_ioQueueName;
	}

	public void messageArrived(CellMessage envelope, DoorTransferFinishedMessage message)
        {
                long id = message.getId();
                TransferManagerHandler h = getHandler(id);
                if (h != null) {
                        h.poolDoorMessageArrived(message);
                } else {
                        log.error("can not find handler with id={}", id);
                }
        }

	public CancelTransferMessage messageArrived(CellMessage envelope, CancelTransferMessage message)
        {
                long id = message.getId();
                TransferManagerHandler h = getHandler(id);
                if (h != null) {
                        h.cancel(message);
                } else {
                        log.error("can not find handler with id={}", id);
                }
                return message;
        }

	public TransferManagerMessage messageArrived(CellMessage envelope, TransferManagerMessage message)
                throws CacheException
        {
                if (!newTransfer()) {
                        throw new CacheException(TransferManagerMessage.TOO_MANY_TRANSFERS, "too many transfers!");
                }
		new TransferManagerHandler(this, message, envelope.getSourcePath()).handle();
                return message;
        }

	public int getMaxTransfers()
        {
		return _maxTransfers;
	}

	public void setMaxTransfers(int max_transfers)
        {
		_maxTransfers = max_transfers;
	}

	private synchronized boolean newTransfer()
        {
                log.debug("newTransfer() num_transfers = {} max_transfers={}",
                          _numTransfers, _maxTransfers);
                if(_numTransfers == _maxTransfers) {
                        log.debug("newTransfer() returns false");
                        return false;
                }
                log.debug("newTransfer() INCREMENT and return true");
                _numTransfers++;
                return true;
        }

	synchronized void finishTransfer()
        {
		log.debug("finishTransfer() num_transfers = {} DECREMENT", _numTransfers);
		_numTransfers--;
	}

	public synchronized long getNextMessageID()
        {
		if (idGenerator!=null) {
			try {
				nextMessageID=idGenerator.nextLong();
			} catch (Exception e)  {
				log.error("Having trouble getting getNextMessageID from DB");
				log.error(e.toString());
				log.error("will nullify requestsPropertyStorage");
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

	protected abstract IpProtocolInfo getProtocolInfo(TransferManagerMessage transferRequest)
                ;

	protected TransferManagerHandler getHandler(long handlerId)
        {
                return _activeTransfers.get(handlerId);
	}

	public void startTimer(final long id)
        {
		TimerTask task = new TimerTask()
                        {
                                @Override
				public void run()
                                {
					log.error("timer for handler " + id + " has expired, killing");
					Object o = _moverTimeoutTimerTasks.remove(id);
					if (o == null) {
						log.error("TimerTask.run(): timer task for handler Id={} not found in moverTimoutTimerTasks hashtable", id);
						return;
					}
					TransferManagerHandler handler = getHandler(id);
					if (handler == null) {
						log.error("TimerTask.run(): timer task for handler Id={} could not find handler !!!", id);
						return;
					}
					handler.timeout();
				}
			};

		_moverTimeoutTimerTasks.put(id, task);

		// this is very approximate
		// but we do not need hard real time
		_moverTimeoutTimer.schedule(task, _moverTimeout);
	}

	public void stopTimer(long id)
        {
		TimerTask tt = _moverTimeoutTimerTasks.remove(id);
		if (tt == null) {
			log.error("stopTimer(): timer not found for Id={}", id);
			return;
		}
		log.debug("canceling the mover timer for handler id {}", id);
		tt.cancel();
	}

	public void addActiveTransfer(long id, TransferManagerHandler handler)
        {
		_activeTransfers.put(id, handler);

                // pm is not final, so better make a final local copy
                // before we synchronize on it and use it
                final PersistenceManager persistanceManager = _pm;
		if (doDbLogging() && persistanceManager !=null) {
			synchronized(persistanceManager) {
				Transaction tx = persistanceManager.currentTransaction();
				try {
					tx.begin();
					persistanceManager.makePersistent(handler);
					// Detach the handler for use
					// working_handler = (TransferManagerHandler)pm.detachCopy(handler);
					tx.commit();
					log.debug("Recording new handler into database.");
				} catch (Exception e) {
					log.error(e.toString());
				} finally {
					rollbackIfActive(tx);
				}
			}
		}
	}

	public void removeActiveTransfer(long id)
        {
		TransferManagerHandler handler = _activeTransfers.remove(id);
                // pm is not final, so better make a final local copy
                // before we synchronize on it and use it
                final PersistenceManager persistanceManager = _pm;
		if (doDbLogging() && persistanceManager!=null) {
			synchronized(persistanceManager) {
				TransferManagerHandlerBackup handlerBackup = new TransferManagerHandlerBackup(handler);
				Transaction tx = persistanceManager.currentTransaction();
				try {
					tx.begin();
					persistanceManager.makePersistent(handler);
					persistanceManager.deletePersistent(handler);
					persistanceManager.makePersistent(handlerBackup);
					tx.commit();
					log.debug("handler removed from db");
				} catch (Exception e) {
					log.error(e.toString());
				} finally {
					rollbackIfActive(tx);
				}
			}
		}
	}

	public int getNumberOfTranfers()
        {
		return _numTransfers;
	}

	public long getPoolTimeout()
        {
		return _poolTimeout;
	}

	public long getPoolManagerTimeout()
        {
		return _poolManagerTimeout;
	}

	public long getPnfsManagerTimeout()
        {
		return _pnfsManagerTimeout;
	}

	public long getMoverTimeout()
        {
		return _moverTimeout;
	}

	public String getLogRootName()
        {
		return _tLogRoot;
	}

	public boolean isOverwrite()
        {
		return _overwrite;
	}

	public CellPath getPoolManagerPath()
        {
		return _poolMgrPath;
	}

	public String getPoolManagerName()
        {
		return _poolManager;
	}

	public String getPnfsManagerName()
        {
		return _pnfsManager;
	}

	public String getIoQueueName()
        {
		return _ioQueueName;
	}

	public synchronized PersistenceManager getPersistenceManager()
        {
		return _pm;
	}

	public static void rollbackIfActive(Transaction tx) {
		if (tx != null && tx.isActive()) {
			tx.rollback();
		}
	}

	public boolean doDbLogging()
        {
		return _doDatabaseLogging;
	}

	public void  setDbLogging(boolean yes)
        {
		_doDatabaseLogging = yes;
	}

	public void setMaxNumberOfDeleteRetries(int nretries)
        {
		_maxNumberOfDeleteRetries = nretries;
	}

	public int getMaxNumberOfDeleteRetries()
        {
		return _maxNumberOfDeleteRetries;
	}

	public void persist(Object o) {
        // pm is not final, so better make a final local copy
        // before we synchronize on it and use it
                final PersistenceManager persistanceManager = _pm;
		if (doDbLogging() && persistanceManager!=null) {
			synchronized(persistanceManager) {
				Transaction tx = persistanceManager.currentTransaction();
				try {
					tx.begin();
					persistanceManager.makePersistent(o);
					tx.commit();
					log.debug("["+o.toString()+"]: Recording new state of handler into database.");
				} catch (Exception e) {
					log.error("["+o.toString()+"]: failed to persist obhject "+o.toString());
					log.error(e.toString());
				} finally {
					rollbackIfActive(tx);
				}
			}
		}
	}

        public String getPoolProxy()
        {
                return _poolProxy;
        }

        public void setPoolProxy(String poolProxy)
        {
                _poolProxy = poolProxy;
        }
}
