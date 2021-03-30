package diskCacheV111.poolManager ;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.CostException;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SourceCostException;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolManagerGetRestoreHandlerInfo;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.poolmanager.SelectedPool;
import org.dcache.util.Args;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;

import static dmg.util.CommandException.checkCommand;
import static java.util.stream.Collectors.toList;

public class RequestContainerV5
    extends AbstractCellComponent
    implements Runnable, CellCommandListener, CellMessageReceiver, CellSetupProvider, CellInfoProvider
{
    private static final Logger _log =
        LoggerFactory.getLogger(RequestContainerV5.class);

    public enum RequestState { ST_INIT, ST_DONE, ST_POOL_2_POOL,
            ST_STAGE, ST_WAITING, ST_WAITING_FOR_STAGING,
            ST_WAITING_FOR_POOL_2_POOL, ST_SUSPENDED }

    private static final String POOL_UNKNOWN_STRING  = "<unknown>" ;

    private static final String STRING_NEVER      = "never" ;
    private static final String STRING_BESTEFFORT = "besteffort" ;
    private static final String STRING_NOTCHECKED = "notchecked" ;

    /** value in milliseconds */
    private static final int DEFAULT_TICKER_INTERVAL = 60000;

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("MM.dd HH:mm:ss");

    private final Map<UOID, PoolRequestHandler>     _messageHash   = new HashMap<>() ;
    private final Map<String, PoolRequestHandler>   _handlerHash   = new HashMap<>() ;

    private CellStub _billing;
    private CellStub _poolStub;
    private long        _retryTimer    = 15 * 60 * 1000 ;

    private static final int MAX_REQUEST_CLUMPING = 20;

    private String      _onError       = "suspend" ;
    private int         _maxRetries    = 3 ;
    private int         _maxRestore    = -1 ;

    private CheckStagePermission _stagePolicyDecisionPoint;
    private boolean _allowAnonymousStaging;

    private boolean     _sendHitInfo;

    private int         _restoreExceeded;
    private boolean     _suspendIncoming;
    private boolean     _suspendStaging;

    private PoolSelectionUnit  _selectionUnit;
    private PoolMonitorV5      _poolMonitor;
    private PnfsHandler        _pnfsHandler;

    private Executor _executor;
    private final Map<PnfsId, CacheException>            _selections       = new HashMap<>() ;
    private PartitionManager   _partitionManager ;
    private volatile long               _checkFilePingTimer = 10 * 60 * 1000 ;
    /** value in milliseconds */
    private final long _ticketInterval;

    private Thread _tickerThread;

    private PoolPingThread _poolPingThread;

    /**
     * Tape Protection.
     * allStates defines that all states are allowed.
     * allStatesExceptStage defines that all states except STAGE are allowed.
     */
    public static final EnumSet<RequestState> allStates =
        EnumSet.allOf(RequestState.class);

    public static final EnumSet<RequestState> allStatesExceptStage =
        EnumSet.complementOf(EnumSet.of(RequestState.ST_STAGE));

    /**
     * RC state machine states sufficient to access online files.
     */
    public static final EnumSet<RequestState> ONLINE_FILES_ONLY
            = EnumSet.of(RequestState.ST_INIT, RequestState.ST_DONE);

    public RequestContainerV5(long tickerInterval) {
        _ticketInterval = tickerInterval;
    }

    public RequestContainerV5()
    {
        this(DEFAULT_TICKER_INTERVAL);
    }

    @Override
    public CellSetupProvider mock()
    {
        RequestContainerV5 mock = new RequestContainerV5();
        mock.setPartitionManager(new PartitionManager());
        return mock;
    }


    public void start()
    {
        _tickerThread = new Thread(this, "Container-ticker");
        _tickerThread.start();
        _poolPingThread = new PoolPingThread();
        _poolPingThread.start();
    }

    public void shutdown()
    {
        if (_tickerThread != null) {
            _tickerThread.interrupt();
        }
        if (_poolPingThread != null) {
            _poolPingThread.interrupt();
        }
    }

    @Required
    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    @Required
    public void setPoolMonitor(PoolMonitorV5 poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfsHandler)
    {
        _pnfsHandler = pnfsHandler;
    }

    @Required
    public void setPartitionManager(PartitionManager partitionManager)
    {
        _partitionManager = partitionManager;
    }

    @Required
    public void setExecutor(Executor executor)
    {
        _executor = executor;
    }

    public void setHitInfoMessages(boolean sendHitInfo)
    {
        _sendHitInfo = sendHitInfo;
    }

    @Required
    public void setBilling(CellStub billing)
    {
        _billing = billing;
    }

    @Required
    public void setPoolStub(CellStub poolStub)
    {
        _poolStub = poolStub;
    }

    public void messageArrived(CellMessage envelope, Object message)
    {
        UOID uoid = envelope.getLastUOID();
        PoolRequestHandler handler;

        synchronized (_messageHash) {
            handler = _messageHash.remove(uoid);
            if (handler == null) {
                return;
            }
        }

        handler.mailForYou(message);
    }

    @Override
    public void run()
    {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(_ticketInterval) ;

                List<PoolRequestHandler> list;
                synchronized (_handlerHash) {
                    list = new ArrayList<>(_handlerHash.values());
                }
                for (PoolRequestHandler handler: list) {
                    if (handler != null) {
                        handler.alive();
                    }
                }
            } catch (InterruptedException e) {
                break;
            } catch (Throwable t) {
                Thread thisThread = Thread.currentThread();
                UncaughtExceptionHandler ueh =
                    thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException(thisThread, t);
            }
        }
        _log.debug("Container-ticker done");
    }

    public void poolStatusChanged(String poolName, int poolStatus) {
        _log.info("Restore Manager : got 'poolRestarted' for {}", poolName);
        try {
            List<PoolRequestHandler> list;
            synchronized (_handlerHash) {
                list = new ArrayList<>(_handlerHash.values());
            }

            for (PoolRequestHandler rph : list) {

                if (rph == null) {
                    continue;
                }


                switch( poolStatus ) {
                    case PoolStatusChangedMessage.UP:
                        /*
                         * if pool is up, re-try all request scheduled to this pool
                         * and all requests, which do not have any pool candidates
                         *
                         * in this construction we will fall down to next case
                         */
                        if (rph.getPoolCandidate().equals(POOL_UNKNOWN_STRING) ) {
                            _log.info("Restore Manager : retrying : {}", rph);
                            rph.retry();
                        }
                    case PoolStatusChangedMessage.DOWN:
                        /*
                         * if pool is down, re-try all request scheduled to this
                         * pool
                         */
                        if (rph.getPoolCandidate().equals(poolName) ) {
                            _log.info("Restore Manager : retrying : {}", rph);
                            rph.retry();
                        }
                }
            }
        } catch (RuntimeException e) {
            _log.error("Problem retrying pool " + poolName, e);
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
       Partition def = _partitionManager.getDefaultPartition();

       pw.println( "      Retry Timeout : "+(_retryTimer/1000)+" seconds" ) ;
       pw.println( "  Thread Controller : "+_executor ) ;
       pw.println( "    Maximum Retries : "+_maxRetries ) ;
       pw.println( "    Pool Ping Timer : "+(_checkFilePingTimer/1000) + " seconds" ) ;
       pw.println( "           On Error : "+_onError ) ;
       pw.println( "          Allow p2p : "+( def._p2pAllowed ? "on" : "off" )+
                                          " oncost="+( def._p2pOnCost ? "on" : "off" )+
                                          " fortransfer="+( def._p2pForTransfer ? "on" : "off" ) );
       pw.println( "      Allow staging : "+(def._hasHsmBackend ? "on":"off") ) ;
       pw.println( "Allow stage on cost : "+(def._stageOnCost ? "on":"off") ) ;
       pw.println( "      Restore Limit : "+(_maxRestore<0?"unlimited":(String.valueOf(_maxRestore))));
       pw.println( "   Restore Exceeded : "+_restoreExceeded ) ;
       if( _suspendIncoming ) {
           pw.println("   Suspend Incoming : on (not persistent)");
       }
       if( _suspendStaging ) {
           pw.println("   Suspend Staging  : on (not persistent)");
       }
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.append("rc onerror ").println(_onError);
        pw.append("rc set max retries ").println(_maxRetries);
        pw.append("rc set retry ").println(_retryTimer/1000);
        pw.append("rc set poolpingtimer ").println(_checkFilePingTimer/1000);
        pw.append("rc set max restore ")
            .println(_maxRestore<0?"unlimited":(String.valueOf(_maxRestore)));
    }

    public static final String hh_rc_set_sameHostCopy =
        STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED;
    @AffectsSetup
    public String ac_rc_set_sameHostCopy_$_1(Args args)
    {
        _partitionManager.setProperties("default", ImmutableMap.of("sameHostCopy", args.argv(0)));
        return "";
    }

    public static final String hh_rc_set_sameHostRetry =
        STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED;
    @AffectsSetup
    public String ac_rc_set_sameHostRetry_$_1(Args args)
    {
        _partitionManager.setProperties("default", ImmutableMap.of("sameHostRetry", args.argv(0)));
        return "" ;
    }

    public static final String fh_rc_set_max_restore = "Limit total number of concurrent restores.  If the total number of\n" +
                                          "restores reaches this limit then any additional restores will fail;\n" +
                                          "when the total number of restores drops below limit then additional\n" +
                                          "restores will be accepted.  Setting the limit to \"0\" will result in\n" +
                                          "all restores failing; setting the limit to \"unlimited\" will remove\n" +
                                          "the limit.";
    public static final String hh_rc_set_max_restore = "<maxNumberOfRestores>" ;
    @AffectsSetup
    public String ac_rc_set_max_restore_$_1( Args args ){
       if( args.argv(0).equals("unlimited") ){
          _maxRestore = -1 ;
          return "" ;
       }
       int n = Integer.parseInt(args.argv(0));
       if( n < 0 ) {
           throw new
                   IllegalArgumentException("must be >=0");
       }
       _maxRestore = n ;
       return "" ;
    }
    public static final String hh_rc_select = "[<pnfsId> [<errorNumber> [<errorMessage>]] [-remove]]" ;
    public String ac_rc_select_$_0_3( Args args ){

       synchronized( _selections ){
          if( args.argc() == 0 ){
             StringBuilder sb = new StringBuilder() ;
             for( Map.Entry<PnfsId, CacheException > entry: _selections.entrySet() ){

                sb.append(entry.getKey().toString()).
                   append("  ").
                   append(entry.getValue().toString()).
                   append("\n");
             }
             return sb.toString() ;
          }
          boolean remove = args.hasOption("remove") ;
          PnfsId  pnfsId = new PnfsId(args.argv(0));

          if( remove ){
             _selections.remove( pnfsId ) ;
             return "" ;
          }
          int    errorNumber  = args.argc() > 1 ? Integer.parseInt(args.argv(1)) : 1 ;
          String errorMessage = args.argc() > 2 ? args.argv(2) : ("Failed-"+errorNumber);

          _selections.put( pnfsId , new CacheException(errorNumber,errorMessage) ) ;
       }
       return "" ;
    }
    public static final String hh_rc_set_warning_path = " # obsolete";
    public String ac_rc_set_warning_path_$_0_1( Args args ){
       return "";
    }
    public static final String fh_rc_set_poolpingtimer =
    " rc set poolpingtimer <timer/seconds> "+
    ""+
    "    If set to a nonzero value, the restore handler will frequently"+
    "    check the pool whether the request is still pending, failed"+
    "    or has been successful" +
    "";
    public static final String hh_rc_set_poolpingtimer = "<checkPoolFileTimer/seconds>" ;
    @AffectsSetup
    public String ac_rc_set_poolpingtimer_$_1(Args args ){
       _checkFilePingTimer = 1000L * Long.parseLong(args.argv(0));

        PoolPingThread poolPingThread = _poolPingThread;
        if (poolPingThread != null) {
            synchronized (poolPingThread) {
                poolPingThread.notify();
            }
        }
       return "" ;
    }
    public static final String hh_rc_set_retry = "<retryTimer/seconds>" ;
    @AffectsSetup
    public String ac_rc_set_retry_$_1(Args args ){
       _retryTimer = 1000L * Long.parseLong(args.argv(0));
       return "" ;
    }
    public static final String hh_rc_set_max_retries = "<maxNumberOfRetries>" ;
    @AffectsSetup
    public String ac_rc_set_max_retries_$_1(Args args ){
       _maxRetries = Integer.parseInt(args.argv(0));
       return "" ;
    }
    public static final String hh_rc_suspend = "[on|off] -all" ;
    public String ac_rc_suspend_$_0_1( Args args ){
       boolean all = args.hasOption("all") ;
       if( args.argc() == 0 ){
          if(all) {
              _suspendIncoming = true;
          }
          _suspendStaging = true ;
       }else{

          String mode = args.argv(0) ;
           switch (mode) {
           case "on":
               if (all) {
                   _suspendIncoming = true;
               }
               _suspendStaging = true;
               break;
           case "off":
               if (all) {
                   _suspendIncoming = false;
               }
               _suspendStaging = false;
               break;
           default:
               throw new
                       IllegalArgumentException("Usage : rc suspend [on|off]");
           }

       }
       return "" ;
    }
    public static final String hh_rc_onerror = "suspend|fail" ;
    @AffectsSetup
    public String ac_rc_onerror_$_1(Args args ){
       String onerror = args.argv(0) ;
       if( ( ! onerror.equals("suspend") ) &&
           ( ! onerror.equals("fail") )  ) {
           throw new
                   IllegalArgumentException("Usage : rc onerror fail|suspend");
       }

       _onError = onerror ;
       return "onerror "+_onError ;
    }
    public static final String fh_rc_retry =
       "NAME\n"+
       "           rc retry\n\n"+
       "SYNOPSIS\n"+
       "           I)  rc retry <pnfsId> [OPTIONS]\n"+
       "           II) rc retry * -force-all [OPTIONS]\n\n"+
       "DESCRIPTION\n"+
       "           Forces a 'restore request' to be retried.\n"+
       "           While  using syntax I, a single request  is retried,\n"+
       "           syntax II retries all requests which reported an error.\n"+
       "           If the '-force-all' options is given, all requests are\n"+
       "           retried, regardless of their current status.\n";
    public static final String hh_rc_retry = "<pnfsId>|* -force-all";
    public String ac_rc_retry_$_1( Args args )
    {
       boolean forceAll = args.hasOption("force-all") ;
       if( args.argv(0).equals("*") ){
          List<PoolRequestHandler> all;
          //
          // Remember : we are not allowed to call 'retry' as long
          // as we  are holding the _handlerHash lock.
          //
          synchronized( _handlerHash ){
             all = new ArrayList<>( _handlerHash.values() ) ;
          }
          for (PoolRequestHandler rph : all) {
              if( forceAll || ( rph._currentRc != 0 ) ) {
                  rph.retry();
              }
          }
       }else{
          PoolRequestHandler rph;
          synchronized( _handlerHash ){
             rph = _handlerHash.get(args.argv(0));
             if( rph == null ) {
                 throw new
                         IllegalArgumentException("Not found : " + args
                         .argv(0));
             }
          }
          rph.retry() ;
       }
       return "";
    }

    @Command(name = "rc failed", hint = "abort a file read request",
            description = "Aborts all requests from clients to read a"
                    + " particular file.  This is typically done when"
                    + " the request is suspended and it is impossible for"
                    + " dCache to provide that file (for example, the disk"
                    + " server with that file has been decomissioned, the tape"
                    + " containing that file's data is broken).  Although this"
                    + " command is useful for recovering from these situations,"
                    + " its usage often points to misconfiguration elsewhere in"
                    + " dCache.")
    public class FailedCommand implements Callable<String>
    {
        @Argument(usage="The request ID, as shown in the first column of the 'rc ls' command.")
        String id;

        @Argument(index=1, required=false, usage="The error number to return to the door.")
        Integer errorNumber = 1;

        @Argument(index=2, required=false, usage="The error message explaining why the transfer failed.")
        String errorString = "Operator Intervention";

        @Override
        public String call() throws CommandException
        {
            checkCommand(errorNumber >= 0, "Error number must be >= 0");

            PoolRequestHandler rph;
            synchronized (_handlerHash) {
                rph = _handlerHash.get(id);
            }

            checkCommand(rph != null, "Not found : %s", id);

            rph.failed(errorNumber, errorString);
            return "";
        }
    }

    public static final String hh_rc_ls = " [<regularExpression>] [-w] [-l] # lists pending requests" ;
    public String ac_rc_ls_$_0_1( Args args ){
       StringBuilder sb  = new StringBuilder() ;

       Pattern  pattern = args.argc() > 0 ? Pattern.compile(args.argv(0)) : null ;
       boolean isLongListing = args.hasOption("l");

       if( !args.hasOption("w") ){
          List<PoolRequestHandler>    allRequestHandlers;
          synchronized( _handlerHash ){
              allRequestHandlers = new ArrayList<>( _handlerHash.values() ) ;
          }

          for( PoolRequestHandler h : allRequestHandlers ){

              if( h == null ) {
                  continue;
              }
              String line = h.toString() ;
              if( ( pattern == null ) || pattern.matcher(line).matches() ) {
                  sb.append(line).append("\n");
                  if (isLongListing) {
                      for(CellMessage m: h.getMessages()) {
                          PoolMgrSelectReadPoolMsg request =
                                  (PoolMgrSelectReadPoolMsg) m.getMessageObject();
                          sb.append("    ").append(request.getProtocolInfo()).append('\n');
                      }
                  }
              }
          }
       }else{

           Map<UOID, PoolRequestHandler>  allPendingRequestHandlers   = new HashMap<>() ;
          synchronized(_messageHash){
              allPendingRequestHandlers.putAll( _messageHash ) ;
          }

          for (Map.Entry<UOID, PoolRequestHandler> requestHandler : allPendingRequestHandlers.entrySet()) {

                UOID uoid = requestHandler.getKey();
                PoolRequestHandler h = requestHandler.getValue();

                if (h == null) {
                    continue;
                }
                String line = uoid.toString() + " " + h.toString();
                if ((pattern == null) || pattern.matcher(line).matches()) {
                    sb.append(line).append("\n");
                }

            }
        }
       return sb.toString();
    }

    public PoolManagerGetRestoreHandlerInfo messageArrived(PoolManagerGetRestoreHandlerInfo msg) {
        msg.setResult(getRestoreHandlerInfo());
        return msg;
    }

    public List<RestoreHandlerInfo> getRestoreHandlerInfo() {
        List<RestoreHandlerInfo> requests;
        synchronized (_handlerHash) {
            requests = _handlerHash.values().stream().filter(Objects::nonNull).map(
                            PoolRequestHandler::getRestoreHandlerInfo).collect(toList());
        }
        return requests;
    }

    public static final String hh_xrc_ls = " # lists pending requests (binary)" ;
    public Object ac_xrc_ls( Args args ){

       List<PoolRequestHandler> all;
       synchronized( _handlerHash ){
          all = new ArrayList<>( _handlerHash.values() ) ;
       }

       List<RestoreHandlerInfo>          list = new ArrayList<>() ;

       for( PoolRequestHandler h: all  ){
          if( h  == null ) {
              continue;
          }
          list.add( h.getRestoreHandlerInfo() ) ;
       }
       return list.toArray(RestoreHandlerInfo[]::new) ;
    }

    public void messageArrived(CellMessage envelope,
                               PoolMgrSelectReadPoolMsg request)
        throws PatternSyntaxException, IOException
    {
        boolean enforceP2P = false ;

        PnfsId       pnfsId       = request.getPnfsId() ;
        String       poolGroup    = request.getPoolGroup();
        ProtocolInfo protocolInfo = request.getProtocolInfo() ;
        EnumSet<RequestState> allowedStates = request.getAllowedStates();

        String hostName;
        if (protocolInfo instanceof IpProtocolInfo) {
            InetSocketAddress target = ((IpProtocolInfo)protocolInfo).getSocketAddress();
            hostName = target.isUnresolved() ? target.getHostString() : target.getAddress().getHostAddress();
        } else {
            hostName = "NoSuchHost";
        }

        String netName      = _selectionUnit.getNetIdentifier(hostName);
        String protocolNameFromInfo = protocolInfo.getProtocol()+"/"+protocolInfo.getMajorVersion() ;

        String protocolName = _selectionUnit.getProtocolUnit( protocolNameFromInfo ) ;
        if( protocolName == null ) {
          throw new
            IllegalArgumentException("Protocol not found : "+protocolNameFromInfo);
        }

        if( request instanceof PoolMgrReplicateFileMsg ){
           if( request.isReply() ){
               _log.warn("Unexpected PoolMgrReplicateFileMsg arrived (is a reply)");
               return ;
           }else{
               enforceP2P = true ;
           }
        }

        String canonicalName = pnfsId +"@"+netName+"-"+protocolName+(enforceP2P?"-p2p":"")
                        +(poolGroup == null ? "" : ("-pg-" + poolGroup));

        PoolRequestHandler handler;

        _log.info( "Adding request for : {}", canonicalName ) ;
        synchronized( _handlerHash ){
           handler = _handlerHash.computeIfAbsent(canonicalName, n ->
                           new PoolRequestHandler(pnfsId,
                                                  poolGroup,
                                                  n,
                                                  allowedStates));
           handler.addRequest(envelope) ;
        }
    }

    // replicate a file
    public static final String hh_replicate = " <pnfsid> <client IP>";
    public String ac_replicate_$_2(Args args) {

        String commandReply = "Replication initiated...";

        try {

            FileAttributes fileAttributes =
                _pnfsHandler.getFileAttributes(new PnfsId(args.argv(0)),
                                               PoolMgrReplicateFileMsg.getRequiredAttributes());

            // TODO: call p2p direct
            // send message to yourself
            PoolMgrReplicateFileMsg req =
                new PoolMgrReplicateFileMsg(fileAttributes,
                                            new DCapProtocolInfo("DCap", 3, 0,
                                                                 new InetSocketAddress(args.argv(1),
                                                                 2222)));

            sendMessage( new CellMessage(new CellAddressCore("PoolManager"), req) );

        } catch (CacheException e) {
            commandReply = "P2P failed : " + e.getMessage();
        }

        return commandReply;
    }

    /**
     * Return Codes used in PoolRequestHandler
     */
    public enum RequestStatusCode {
        OK,
        FOUND,
        NOT_FOUND,
        ERROR,
        OUT_OF_RESOURCES,
        COST_EXCEEDED,
        NOT_PERMITTED,
        S_COST_EXCEEDED,
        DELAY
    }
    ///////////////////////////////////////////////////////////////
    //
    // the read io request handler
    //
    private class PoolRequestHandler  {

        protected final PnfsId       _pnfsId;
        protected final String       _poolGroup;
        protected final List<CellMessage>    _messages = new ArrayList<>() ;
        protected int _retryCounter;
        private final CDC _cdc = new CDC();


        private   UOID         _waitingFor;

        private   String       _status        = "[<idle>]";
        private   volatile RequestState _state         = RequestState.ST_INIT;
        private   final Collection<RequestState> _allowedStates;
        private   boolean      _stagingDenied;
        private   int          _currentRc;
        private   String       _currentRm     = "" ;

        /**
         * The best pool found by askIfAvailable(). In contrast to
         * _poolCandidateInfo, _bestPool may be set even when
         * askIfAvailable() returns with an error. Eg when the best
         * pool is too expensive.
         */
        private SelectedPool _bestPool;

        /**
         * The pool from which to read the file or the pool to which
         * to stage the file. Set by askIfAvailable() when it returns
         * RequestStatusCode.FOUND, by exercisePool2PoolReply() when it returns
         * RequestStatusCode.OK, and by askForStaging(). Also set in the
         * stateEngine() at various points.
         */
        private   volatile SelectedPool _poolCandidate;

        /**
         * The pool used for staging.
         *
         * Serves a critical role when retrying staging to avoid that
         * the same pool is chosen twice in a row.
         */
        private Optional<SelectedPool> _stageCandidate = Optional.empty();

        /**
         * The destination of a pool to pool transfer. Set by
         * askForPoolToPool() when it returns RequestStatusCode.FOUND.
         */
        private   volatile SelectedPool _p2pDestinationPool;

        /**
         * The source of a pool to pool transfer. Set by
         * askForPoolToPool() when it return RequestStatusCode.FOUND.
         */
        private SelectedPool _p2pSourcePool;

        private   final long   _started       = System.currentTimeMillis() ;
        private   final String       _name;

        private   FileAttributes _fileAttributes;
        private   StorageInfo  _storageInfo;
        private   ProtocolInfo _protocolInfo;
        private   String       _linkGroup;
        private   String _billingPath;
        private   String _transferPath;

        private   boolean _enforceP2P;
        private   int     _destinationFileStatus = Pool2PoolTransferMsg.UNDETERMINED ;

        private PoolSelector _poolSelector;
        private Partition _parameter = _partitionManager.getDefaultPartition();

        /**
         * Indicates the next time a TTL of a request message will be
         * exceeded.
         */
        private long    _nextTtlTimeout = Long.MAX_VALUE;
        private boolean _failOnExcluded;

        public PoolRequestHandler(PnfsId pnfsId,
                                  String poolGroup,
                                  String canonicalName,
                                  Collection<RequestState> allowedStates)
        {
	    _pnfsId  = pnfsId ;
	    _poolGroup = poolGroup;
	    _name    = canonicalName ;
	    _allowedStates = allowedStates ;
	}
        //...........................................................
        //
        // the following methods can be called from outside
        // at any time.
        //...........................................................
        //
        // add request is assumed to be synchronized by a higher level.
        //
        public void addRequest( CellMessage message ){

           PoolMgrSelectReadPoolMsg request =
                (PoolMgrSelectReadPoolMsg)message.getMessageObject() ;

            // fail-fast if state is not allowed
            if (!request.getAllowedStates().contains(_state)) {
                request.setFailed(CacheException.PERMISSION_DENIED, "Pool manager state not allowed");

                message.revertDirection();
                sendMessage(message);
                return;
            }

           _messages.add(message);
           _stagingDenied = false;

           long ttl = message.getTtl();
           if (ttl < Long.MAX_VALUE) {
               long timeout = System.currentTimeMillis() + ttl;
               _nextTtlTimeout = Math.min(_nextTtlTimeout, timeout);
           }

           if (_poolSelector != null) {
               return;
           }

           _linkGroup = request.getLinkGroup();
           _protocolInfo = request.getProtocolInfo();
           _fileAttributes = request.getFileAttributes();
           _storageInfo = _fileAttributes.getStorageInfo();
           _billingPath = request.getBillingPath();
           _transferPath = request.getTransferPath();

           _retryCounter = request.getContext().getRetryCounter();
           _stageCandidate = Optional.ofNullable(request.getContext().getPreviousStagePool());

           if( request instanceof PoolMgrReplicateFileMsg ){
              _enforceP2P            = true ;
              _destinationFileStatus = ((PoolMgrReplicateFileMsg)request).getDestinationFileStatus() ;
           }

           Set<String> excluded = request.getExcludedHosts();
           _failOnExcluded = excluded != null && !excluded.isEmpty();

           _poolSelector =
               _poolMonitor.getPoolSelector(_fileAttributes,
                       _protocolInfo,
                       _linkGroup,
                       excluded);
           //
           //
           //
           add(null) ;
        }

        public List<CellMessage> getMessages() {
            synchronized( _handlerHash ){
                return new ArrayList<>(_messages);
            }
        }

        public String getPoolCandidate()
        {
            if (_poolCandidate != null) {
                return _poolCandidate.name();
            } else if (_p2pDestinationPool != null) {
                return _p2pDestinationPool.name();
            } else {
                return POOL_UNKNOWN_STRING;
            }
        }

        private String getPoolCandidateState()
        {
            if (_poolCandidate != null) {
                return _poolCandidate.name();
            } else if (_p2pDestinationPool != null) {
                return (_p2pSourcePool == null ? POOL_UNKNOWN_STRING : _p2pSourcePool.name())
                    + "->" + _p2pDestinationPool.name();
            } else {
                return POOL_UNKNOWN_STRING;
            }
        }

	public RestoreHandlerInfo getRestoreHandlerInfo(){
	   return new RestoreHandlerInfo(
	          _name,
		  _messages.size(),
		  _retryCounter ,
                  _started ,
		  getPoolCandidateState() ,
		  _status ,
		  _currentRc ,
		  _currentRm ) ;
	}
        @Override
        public String toString(){
           return _name+" m="+_messages.size()+" r="+
                  _retryCounter+" ["+getPoolCandidateState()+"] ["+_status+"] "+
                  "{"+_currentRc+","+_currentRm+"}" ;
        }
        //
        //
        private void mailForYou( Object message ){
           //
           // !!!!!!!!! remove this
           //
           //if( message instanceof PoolFetchFileMessage ){
           //    _log.info("mailForYou !!!!! reply ignored ") ;
           //    return ;
           //}
           add( message ) ;
        }
        private void alive(){

           Object [] command = new Object[1] ;
           command[0] = "alive" ;

           add( command ) ;

        }
        private void retry()
        {
           Object [] command = new Object[1];
           command[0] = "retry" ;
           add(command);
        }
        private void failed( int errorNumber , String errorMessage )
        {

           if( errorNumber > 0 ){
              Object [] command = new Object[3] ;
              command[0] = "failed" ;
              command[1] = errorNumber;
              command[2] = errorMessage == null ?
                           ( "Error-"+_currentRc ) :
                           errorMessage ;


              add( command ) ;
              return ;
           }
           throw new
           IllegalArgumentException("Error number must be > 0");

        }

        //...................................................................
        //
        // from now on, methods can only be called from within
        // the state mechanism. (which is thread save because
        // we only allow to run a single thread at a time.
        //
        private void clearSteering() {
            if (_waitingFor != null) {
                synchronized (_messageHash) {
                  _messageHash.remove(_waitingFor);
                }
                _waitingFor = null;
            }
        }
        private void setError( int errorCode , String errorMessage ){
           _currentRc = errorCode ;
           _currentRm = errorMessage ;
        }

        private boolean sendFetchRequest(SelectedPool pool)
        {
            // TODO: Include assumption in request
            CellMessage cellMessage = new CellMessage(
                    new CellPath(pool.address()),
                    new PoolFetchFileMessage(pool.name(), _fileAttributes)
            );
            synchronized (_messageHash) {
                if (_maxRestore >= 0 && _messageHash.size() >= _maxRestore) {
                    return false;
                }
                if (_waitingFor != null) {
                    _messageHash.remove(_waitingFor);
                }
                _waitingFor = cellMessage.getUOID();
                _messageHash.put(_waitingFor, this);
                sendMessage(cellMessage);
                _status = "Staging " + LocalDateTime.now().format(DATE_TIME_FORMAT);
            }
            return true;
        }

        private void sendPool2PoolRequest(SelectedPool sourcePool, SelectedPool destPool)
        {
            // TOOD: Include assumptions in request
            Pool2PoolTransferMsg pool2pool =
                    new Pool2PoolTransferMsg(sourcePool.name(), destPool.name(), _fileAttributes);
            pool2pool.setDestinationFileStatus(_destinationFileStatus);
            _log.info("[p2p] Sending transfer request: {}", pool2pool);
            CellMessage cellMessage =
                    new CellMessage(new CellPath(destPool.address()), pool2pool);

            synchronized (_messageHash) {
                if (_waitingFor != null) {
                    _messageHash.remove(_waitingFor);
                }
                _waitingFor = cellMessage.getUOID();
                _messageHash.put(_waitingFor, this);
                sendMessage(cellMessage);
                _status = "[P2P " + LocalDateTime.now().format(DATE_TIME_FORMAT) + "]";
            }
        }

        /**
         * Removes request messages who's time to live has been
         * exceeded. Messages are dropped; no reply is sent to the
         * requestor, as we assume it is no longer waiting for the
         * reply.
         */
        private void expireRequests()
        {
            /* Access to _messages is controlled by a lock on
             * _handlerHash.
             */
            synchronized (_handlerHash) {
                long now = System.currentTimeMillis();
                _nextTtlTimeout = Long.MAX_VALUE;

                Iterator<CellMessage> i = _messages.iterator();
                while (i.hasNext()) {
                    CellMessage message = i.next();
                    long ttl = message.getTtl();
                    if (message.getLocalAge() >= ttl) {
                        _log.info("Discarding request from {} because its time to live has been exceeded.", message.getSourcePath().getCellName());
                        i.remove();
                    } else if (ttl < Long.MAX_VALUE) {
                        _nextTtlTimeout = Math.min(_nextTtlTimeout, now + ttl);
                    }
                }
            }
        }

        private boolean answerRequest(int count) {
            //
            // if there is an error we won't continue ;
            //
            if (_currentRc != 0) {
                count = 100000;
            }
            //

            Iterator<CellMessage> messages = _messages.iterator();
            for (int i = 0; (i < count) && messages.hasNext(); i++) {
                CellMessage m =  messages.next();
                PoolMgrSelectReadPoolMsg rpm =
                    (PoolMgrSelectReadPoolMsg) m.getMessageObject();
                rpm.setContext(_retryCounter + 1, _stageCandidate.orElse(null));
                if (_currentRc == 0) {
                    rpm.setPool( new diskCacheV111.vehicles.Pool(_poolCandidate.name(), _poolCandidate.info().getAddress(), _poolCandidate.assumption()));
                    rpm.setSucceeded();
                } else {
                    rpm.setFailed(_currentRc, _currentRm);
                }
                m.revertDirection();
                sendMessage(m);
                messages.remove();
            }
            return messages.hasNext();
        }
        //
        // and the heart ...
        //


        private static final int CONTINUE        = 0 ;
        private static final int WAIT            = 1 ;

        private final Deque<Object> _fifo              = new LinkedList<>() ;
        private boolean    _stateEngineActive;
        private boolean    _forceContinue;
        private boolean    _overwriteCost;

        public class RunEngine implements Runnable {
           @Override
           public void run(){
              try (CDC ignored = _cdc.restore()) {
                 stateLoop() ;
              }finally{
                 synchronized( _fifo ){
                   _stateEngineActive = false ;
                 }
              }
           }

           @Override
           public String toString() {
              return PoolRequestHandler.this.toString();
           }
        }
        private void add( Object obj ){
           synchronized( _fifo ){
               _log.info( "Adding Object : {}", obj ) ;
               _fifo.addFirst(obj) ;
               if( _stateEngineActive ) {
                   return;
               }
               _log.info( "Starting Engine" ) ;
               _stateEngineActive = true ;
               try {
                   _executor.execute(new FireAndForgetTask(new RunEngine()));
               } catch (RuntimeException e) {
                   _stateEngineActive = false;
                   throw e;
               }
           }
        }
        private void stateLoop(){

           Object inputObject ;
           _log.info( "ACTIVATING STATE ENGINE {} {}", _pnfsId, (System.currentTimeMillis()-_started)) ;

           while( ! Thread.interrupted() ){

              if( ! _forceContinue ){

                 synchronized( _fifo ){
                    if(_fifo.isEmpty()){
                       _stateEngineActive = false ;
                       return ;
                    }
                    inputObject = _fifo.removeLast() ;
                 }
              }else{
                 inputObject = null ;
              }
              _forceContinue = false ;
              try{
                 _log.info("StageEngine called in mode {}  with object {}", _state,
                         (
                         inputObject == null ? "(NULL)":
                                 (
                                         inputObject instanceof Object [] ?
                                                 ((Object[])inputObject)[0].toString()
                                                 : inputObject.getClass().getName()
                                 )
                         )
                 );

                 stateEngine( inputObject ) ;

                 _log.info("StageEngine left with: {} ({})",
                           _state, (_forceContinue ? "Continue" : "Wait"));
              } catch (RuntimeException e) {
                  _log.error("Unexpected Exception in state loop for " + _pnfsId, e);
              }
           }
        }

        private boolean canStage()
        {
            /* If the result is cached or the door disabled staging,
             * then we don't check the permissions.
             */
            if (_stagingDenied || !_allowedStates.contains(RequestState.ST_STAGE)) {
                return false;
            }

            /* Staging is allowed if just one of the requests has
             * permission to stage.
             */
            for (CellMessage envelope: _messages) {
                try {
                    PoolMgrSelectReadPoolMsg msg =
                        (PoolMgrSelectReadPoolMsg) envelope.getMessageObject();
                    if (_stagePolicyDecisionPoint.canPerformStaging(msg.getSubject(),
                                                                    msg.getFileAttributes(),
                                                                    msg.getProtocolInfo())) {
                        return true;
                    }
                } catch (IOException | PatternSyntaxException e) {
                    _log.error("Failed to verify stage permissions: {}", e.getMessage());
                }
            }

            /* None of the requests had the necessary credentials to
             * stage. This result is cached.
             */
            _stagingDenied = true;
            return false;
        }

        private void nextStep(RequestState state, int shouldContinue ){
            if (_currentRc == CacheException.NOT_IN_TRASH ||
                _currentRc == CacheException.FILE_NOT_FOUND) {
                _state = RequestState.ST_DONE;
                _forceContinue = true;
                _status = "Failed";
                sendInfoMessage(
                        _currentRc , "Failed "+_currentRm);
            } else {
                if (state == RequestState.ST_STAGE && !canStage()) {
                    _state = RequestState.ST_DONE;
                    _forceContinue = true;
                    _status = "Failed";
                    _log.debug("Subject is not authorized to stage");
                    _currentRc = CacheException.PERMISSION_DENIED;
                    _currentRm = "File not online. Staging not allowed.";
                    sendInfoMessage(
                            _currentRc , "Permission denied." + _currentRm);
                } else if (!_allowedStates.contains(state)) {
                    _state = RequestState.ST_DONE;
                    _forceContinue = true;
                    _status = "Failed";
                    _log.debug("No permission to perform {}", state);
                    _currentRc = CacheException.PERMISSION_DENIED;
                    _currentRm = "Permission denied.";
                    sendInfoMessage(_currentRc,
                                    "Permission denied for " + state);
                } else {
                    _state = state;
                    _forceContinue = shouldContinue == CONTINUE ;
                    if( _state != RequestState.ST_DONE ){
                        _currentRc = 0 ;
                        _currentRm = "" ;
                    }
                }
            }
        }

        //
        //  askIfAvailable :
        //
        //      default : (bestPool=set,overwriteCost=false) otherwise mentioned
        //
        //      RequestStatusCode.FOUND :
        //
        //         Because : file is on pool which is allowed and has reasonable cost.
        //
        //         -> DONE
        //
        //      RequestStatusCode.NOT_FOUND :
        //
        //         Because : file is not in cache at all
        //
        //         (bestPool=0)
        //
        //         -> _hasHsmBackend : STAGE
        //              else         : Suspended (CacheException.POOL_UNAVAILABLE, pool unavailable)
        //
        //      RequestStatusCode.NOT_PERMITTED :
        //
        //         Because : file not in an permitted pool but somewhere else
        //
        //         (bestPool=0,overwriteCost=true)
        //
        //         -> _p2pAllowed ||
        //            ! _hasHsmBackend  : P2P
        //            else              : STAGE
        //
        //      RequestStatusCode.COST_EXCEEDED :
        //
        //         Because : file is in permitted pools but cost is too high.
        //
        //         -> _p2pOnCost          : P2P
        //            _hasHsmBackend &&
        //            _stageOnCost        : STAGE
        //            else                : 127 , "Cost exceeded (st,p2p not allowed)"
        //
        //      RequestStatusCode.ERROR :
        //
        //         Because : - No entry in configuration Permission Matrix
        //                   - Code Exception
        //
        //         (bestPool=0)
        //
        //         -> STAGE
        //
        //
        //
        //  askForPoolToPool( overwriteCost ) :
        //
        //      RequestStatusCode.FOUND :
        //
        //         Because : source and destination pool found and cost ok.
        //
        //         -> DONE
        //
        //      RequestStatusCode.NOT_PERMITTED :
        //
        //         Because : - already too many copies (_maxPnfsFileCopies)
        //                   - file already everywhere (no destination found)
        //                   - SAME_HOST_NEVER : but no valid combination found
        //
        //         -> DONE 'using bestPool'
        //
        //      RequestStatusCode.S_COST_EXCEEDED (only if ! overwriteCost ) :
        //
        //         Because : best source pool exceeds 'alert' cost.
        //
        //         -> _hasHsmBackend &&
        //            _stageOnCost    : STAGE
        //            bestPool == 0   : 194,"File not present in any reasonable pool"
        //            else            : DONE 'using bestPool'
        //
        //      RequestStatusCode.COST_EXCEEDED (only if ! overwriteCost )  :
        //
        //         Because : file is in permitted pools but cost of
        //                   best destination pool exceeds cost of best
        //                   source pool (resp. slope * source).
        //
        //         -> _bestPool == 0 : 192,"File not present in any reasonable pool"
        //            else           : DONE 'using bestPool'
        //
        //      RequestStatusCode.ERROR :
        //
        //         Because : - no source pool (code problem)
        //                   - Code Exception
        //
        //         -> 132,"PANIC : Tried to do p2p, but source was empty"
        //                or exception text.
        //
        //  askForStaging :
        //
        //      RequestStatusCode.FOUND :
        //
        //         Because : destination pool found and cost ok.
        //
        //         -> DONE
        //
        //      RequestStatusCode.NOT_FOUND :
        //
        //         -> 149 , "No pool candidates available or configured for 'staging'"
        //         -> 150 , "No cheap candidates available for 'staging'"
        //
        //      RequestStatusCode.ERROR :
        //
        //         Because : - Code Exception
        //
        private void stateEngine( Object inputObject ) {
           RequestStatusCode rc;
           switch( _state ){

              case ST_INIT :
                 _log.debug( "stateEngine: case ST_INIT");
                 synchronized( _selections ){

                    CacheException ce = _selections.get(_pnfsId) ;
                    if( ce != null ){
                       setError(ce.getRc(),ce.getMessage());
                       nextStep(RequestState.ST_DONE , CONTINUE ) ;
                       return ;
                    }

                 }


                 if( inputObject == null ){


                    if( _suspendIncoming ){
                        setError(1005, "Suspend enforced");
                        suspend("Suspended (forced)");
                        return ;
                    }

                    //
                    //
                    if( _enforceP2P ){
                        setError(0,"");
                        nextStep(RequestState.ST_POOL_2_POOL , CONTINUE) ;
                        return ;
                    }

                    if( ( rc = askIfAvailable() ) == RequestStatusCode.FOUND ){

                       setError(0,"");
                       nextStep(RequestState.ST_DONE , CONTINUE ) ;
                       _log.info("AskIfAvailable found the object");
                       if (_sendHitInfo) {
                           sendHitMsg(_bestPool.info(), true);
                       }

                    }else if( rc == RequestStatusCode.NOT_FOUND ){
                       //
                       //
                        _log.debug(" stateEngine: RequestStatusCode.NOT_FOUND ");
                       if( _parameter._hasHsmBackend && _storageInfo.isStored()){
                           _log.debug(" stateEngine: parameter has HSM backend and the file is stored on tape ");
                          nextStep(RequestState.ST_STAGE , CONTINUE ) ;
                       }else{
                          _log.debug(" stateEngine: case 1: parameter has NO HSM backend or case 2: the HSM backend exists but the file isn't stored on it.");
                          _poolCandidate = null ;
                          setError(CacheException.POOL_UNAVAILABLE, "Pool unavailable");
                          suspendIfEnabled("Suspended (pool unavailable)");
                       }
                       if (_sendHitInfo && _poolCandidate == null) {
                           sendHitMsg(_bestPool == null ? null : _bestPool.info(), false);   //VP
                       }
                       //
                    }else if( rc == RequestStatusCode.NOT_PERMITTED ){
                       //
                       //  if we can't read the file because 'read is prohibited'
                       //  we at least must give dCache the chance to copy it
                       //  to another pool (not regarding the cost).
                       //
                       _overwriteCost = true ;
                       //
                       //  if we don't have an hsm we overwrite the p2pAllowed
                       //
                       nextStep( _parameter._p2pAllowed || ! _parameter._hasHsmBackend
                                ? RequestState.ST_POOL_2_POOL : RequestState.ST_STAGE , CONTINUE ) ;

                    }else if( rc == RequestStatusCode.COST_EXCEEDED ){

                       if( _parameter._p2pOnCost ){

                           nextStep(RequestState.ST_POOL_2_POOL , CONTINUE ) ;

                       }else if( _parameter._hasHsmBackend &&  _parameter._stageOnCost ){

                           nextStep(RequestState.ST_STAGE , CONTINUE ) ;

                       }else{

                           setError( 127 , "Cost exceeded (st,p2p not allowed)" ) ;
                           nextStep(RequestState.ST_DONE , CONTINUE ) ;

                       }
                    }else if( rc == RequestStatusCode.ERROR ){
                       _log.debug( " stateEngine: RequestStatusCode.ERROR");
                       nextStep(RequestState.ST_STAGE , CONTINUE ) ;
                       _log.info("AskIfAvailable returned an error, will continue with Staging");

                    }

                 }else if( inputObject instanceof Object [] ){

                      handleCommandObject( (Object [] ) inputObject ) ;

                 }

              break ;

              case ST_POOL_2_POOL :
              {
                  _log.debug( "stateEngine: case ST_POOL_2_POOL");
                 if( inputObject == null ){

                    if( ( rc = askForPoolToPool( _overwriteCost ) ) == RequestStatusCode.FOUND ){

                       nextStep(RequestState.ST_WAITING_FOR_POOL_2_POOL , WAIT ) ;
                       _status = "Pool2Pool "+ LocalDateTime.now().format(DATE_TIME_FORMAT);
                       setError(0, "");

                       if (_sendHitInfo) {
                           sendHitMsg(_p2pSourcePool.info(), true);   //VP
                       }

                    }else if( rc == RequestStatusCode.NOT_PERMITTED ){

                        if( _bestPool == null) {
                            if( _enforceP2P ){
                               nextStep(RequestState.ST_DONE , CONTINUE ) ;
                            }else if( _parameter._hasHsmBackend && _storageInfo.isStored() ){
                               _log.info("ST_POOL_2_POOL : Pool to pool not permitted, trying to stage the file");
                               nextStep(RequestState.ST_STAGE , CONTINUE ) ;
                            }else{
                               setError(265, "Pool to pool not permitted");
                               suspendIfEnabled("Suspended");
                            }
                        }else{
                            _poolCandidate = _bestPool;
                            _log.info("ST_POOL_2_POOL : Choosing high cost pool {}", _poolCandidate.info());

                          setError(0,"");
                          nextStep(RequestState.ST_DONE , CONTINUE ) ;
                        }

                    }else if( rc == RequestStatusCode.S_COST_EXCEEDED ){

                       _log.info("ST_POOL_2_POOL : RequestStatusCode.S_COST_EXCEEDED");

                       if( _parameter._hasHsmBackend && _parameter._stageOnCost && _storageInfo.isStored() ){

                           if( _enforceP2P ){
                              nextStep(RequestState.ST_DONE , CONTINUE ) ;
                           }else{
                              _log.info("ST_POOL_2_POOL : staging");
                              nextStep(RequestState.ST_STAGE , CONTINUE ) ;
                           }
                       }else{

                          if( _bestPool != null ){

                              _poolCandidate = _bestPool;
                              _log.info("ST_POOL_2_POOL : Choosing high cost pool {}", _poolCandidate.info());

                             setError(0,"");
                             nextStep(RequestState.ST_DONE , CONTINUE ) ;
                          }else{
                             //
                             // this can't possibly happen
                             //
                             setError(194,"PANIC : File not present in any reasonable pool");
                             nextStep(RequestState.ST_DONE , CONTINUE ) ;
                          }

                       }
                    }else if( rc == RequestStatusCode.COST_EXCEEDED ){
                       //
                       //
                       if( _bestPool == null ){
                          //
                          // this can't possibly happen
                          //
                          if( _enforceP2P ){
                             nextStep(RequestState.ST_DONE , CONTINUE ) ;
                          }else{
                             setError(192,"PANIC : File not present in any reasonable pool");
                             nextStep(RequestState.ST_DONE , CONTINUE ) ;
                          }

                       }else{

                           _poolCandidate = _bestPool;

                          _log.info(" found high cost object");

                          setError(0,"");
                          nextStep(RequestState.ST_DONE , CONTINUE ) ;

                       }


                    }else{

                       if( _enforceP2P ){
                          nextStep(RequestState.ST_DONE , CONTINUE ) ;
                       }else if( _parameter._hasHsmBackend && _storageInfo.isStored() ){
                          nextStep(RequestState.ST_STAGE , CONTINUE ) ;
                       }else{
                          suspendIfEnabled("Suspended");
                       }
                    }
                 }
              }
              break ;

              case ST_STAGE :

                  _log.debug("stateEngine: case ST_STAGE");

                 if( inputObject == null ){

                    if( _suspendStaging ){
                         setError(1005, "Suspend enforced");
                         suspend("Suspended Stage (forced)");
                         return ;
                    }

                    if( ( rc = askForStaging() ) == RequestStatusCode.FOUND ){

                       nextStep(RequestState.ST_WAITING_FOR_STAGING , WAIT ) ;
                       _status = "Staging "+ LocalDateTime.now().format(DATE_TIME_FORMAT);
                       setError(0, "");

                    }else if( rc == RequestStatusCode.OUT_OF_RESOURCES ){

                       _restoreExceeded ++ ;
                       outOfResources("Restore") ;

                    }else{
                       //
                       // we couldn't find a pool for staging
                       //
                       errorHandler() ;
                    }
                 }

              break ;
              case ST_WAITING_FOR_POOL_2_POOL :
                 _log.debug( "stateEngine: case ST_WAITING_FOR_POOL_2_POOL");
                 if( inputObject instanceof Pool2PoolTransferMsg ){

                    if( ( rc =  exercisePool2PoolReply((Pool2PoolTransferMsg)inputObject) ) == RequestStatusCode.OK ){
                        if (_parameter._p2pForTransfer && ! _enforceP2P) {
                            setError(CacheException.OUT_OF_DATE,
                                     "Pool locations changed due to p2p transfer");
                            nextStep(RequestState.ST_DONE, CONTINUE);
                        } else {
                            nextStep(RequestState.ST_DONE, CONTINUE);
                        }
                    }else{
                        _log.info("ST_POOL_2_POOL : Pool to pool reported a problem");
                        if( _parameter._hasHsmBackend && _storageInfo.isStored() ){

                            _log.info("ST_POOL_2_POOL : trying to stage the file");
                            nextStep(RequestState.ST_STAGE , CONTINUE ) ;

                        }else{
                            errorHandler() ;
                        }

                    }

                 }else if( inputObject instanceof Object [] ){

                    handleCommandObject( (Object []) inputObject ) ;

                } else if (inputObject instanceof PingFailure &&
                        _p2pDestinationPool.address().equals(((PingFailure) inputObject).getPool())) {
                    _log.info("Ping reported that request died.");
                    setError(CacheException.TIMEOUT, "Replication timed out");
                    errorHandler();
                } else if (inputObject != null) {
                    _log.error("Unexpected message type: {}. Possibly a bug.", inputObject.getClass());
                    setError(102,"Unexpected message type " + inputObject.getClass());
                    errorHandler() ;
                }

              break ;
              case ST_WAITING_FOR_STAGING :
                 _log.debug( "stateEngine: case ST_WAITING_FOR_STAGING" );
                 if( inputObject instanceof PoolFetchFileMessage ){

                    if( ( rc =  exerciseStageReply( (PoolFetchFileMessage)inputObject ) ) == RequestStatusCode.OK ){
                        if (_parameter._p2pForTransfer) {
                            setError(CacheException.OUT_OF_DATE,
                                     "Pool locations changed due to stage");
                            nextStep(RequestState.ST_DONE, CONTINUE);
                        } else {
                            nextStep(RequestState.ST_DONE, CONTINUE);
                        }
                    }else if( rc == RequestStatusCode.DELAY ){
                        suspend("Suspended By HSM request");
                    }else{

                       errorHandler() ;

                    }
                 }else if( inputObject instanceof Object [] ){

                    handleCommandObject( (Object []) inputObject ) ;

                } else if (inputObject instanceof PingFailure &&
                        _poolCandidate.address().equals(((PingFailure) inputObject).getPool())) {
                    _log.info("Ping reported that request died.");
                    setError(CacheException.TIMEOUT, "Staging timed out");
                    errorHandler();
                } else if (inputObject != null) {
                     _log.error("Unexpected message type: {}. Possibly a bug.", inputObject.getClass());
                     setError(102,"Unexpected message type " + inputObject.getClass());
                     errorHandler() ;
                }
                break;
            case ST_SUSPENDED:
                _log.debug("stateEngine: case ST_SUSPENDED");
                if (inputObject instanceof Object[]) {

                    handleCommandObject( (Object []) inputObject ) ;

                 }
              return ;

              case ST_DONE :
                 _log.debug( "stateEngine: case ST_DONE" );
                 if( inputObject == null ){

                    clearSteering();
                    //
                    // it is essential that we are not within any other
                    // lock when trying to get the handlerHash lock.
                    //
                    synchronized (_handlerHash) {
                        _handlerHash.remove(_name);
                    }
                    while (answerRequest(MAX_REQUEST_CLUMPING)) {
                        setError(CacheException.OUT_OF_DATE,
                                 "Request clumping limit reached");
                    }
                 }

           }
        }
        private void handleCommandObject( Object [] c ){

           String command = c[0].toString() ;
            switch (command) {
            case "failed":

                clearSteering();
                setError((Integer) c[1], c[2].toString());
                nextStep(RequestState.ST_DONE, CONTINUE);

                break;
            case "retry":

                _status = "Retry enforced";
                _retryCounter = -1;
                clearSteering();
                setError(CacheException.OUT_OF_DATE, "Operator asked for retry");
                nextStep(RequestState.ST_DONE, CONTINUE);
                break;

            case "alive":
                long now = System.currentTimeMillis();
                if (now > _nextTtlTimeout) {
                    expireRequests();
                }
                break;
            }

        }

        private void outOfResources( String detail ){

           clearSteering();
           setError(5,"Resource temporarily unavailable : "+detail);
           nextStep(RequestState.ST_DONE , CONTINUE ) ;
           _status = "Failed" ;
           sendInfoMessage(
                   _currentRc , "Failed "+_currentRm );
        }

        private void fail()
        {
            if (_currentRc == 0) {
                _log.error("Error handler called without an error");
                setError(CacheException.DEFAULT_ERROR_CODE,
                        "Pool selection failed");
            }
            nextStep(RequestState.ST_DONE, CONTINUE);
        }

        private void suspend(String status)
        {
            _log.debug(" stateEngine: SUSPENDED/WAIT ");
            _status = status + " " + LocalDateTime.now().format(DATE_TIME_FORMAT);
            nextStep(RequestState.ST_SUSPENDED, WAIT);
            sendInfoMessage(
                    _currentRc, "Suspended (" + _currentRm + ")");
        }

        private void suspendIfEnabled(String status)
        {
            if (_onError.equals("suspend") && !_failOnExcluded) {
                suspend(status);
            } else {
                fail();
            }
        }

        private void errorHandler()
        {
            if (_retryCounter >= _maxRetries) {
                suspendIfEnabled("Suspended");
            } else {
                fail();
            }
        }

        private RequestStatusCode exerciseStageReply(PoolFetchFileMessage reply) {

            _currentRc = reply.getReturnCode();

            switch (_currentRc) {
                case 0:
                    // best candidate is the right one
                    return RequestStatusCode.OK;
                case CacheException.HSM_DELAY_ERROR:
                    String explanation = reply.getErrorObject() == null
                                    ? "No info" : reply.getErrorObject().toString();
                    _currentRm = "Suspend by HSM request : " + explanation;
                    return RequestStatusCode.DELAY;
                default:
                    _currentRm = reply.getErrorObject() == null
                            ? ("Error=" + _currentRc) : reply.getErrorObject().toString();
                    return RequestStatusCode.ERROR;
            }
        }

        private RequestStatusCode exercisePool2PoolReply(Pool2PoolTransferMsg reply) {

            _log.info("Pool2PoolTransferMsg replied with : {}", reply);
            if ((_currentRc = reply.getReturnCode()) == 0) {
                _poolCandidate = _p2pDestinationPool;
                return RequestStatusCode.OK;

            } else {
                _currentRm = reply.getErrorObject() == null
                        ? ("Error=" + _currentRc) : reply.getErrorObject().toString();

                return RequestStatusCode.ERROR;
            }
        }
        //
        //  calculate :
        //       matrix = list of list of active
        //                pools with file available (sorted)
        //
        //  if empty :
        //        bestPool = 0 , return NOT_FOUND
        //
        //  else
        //        determine best pool by
        //
        //        if allowFallback :
        //           first row for which cost < costCut or
        //           if not found, pool with lowest cost.
        //        else
        //           leftmost pool of first nonzero row
        //
        //  if bestPool > costCut :
        //        return COST_EXCEEDED
        //
        //  chose best pool from row selected above by :
        //     if ( minCostCut > 0 ) :
        //         take all pools of the selected row
        //         with cost < minCostCut and make hash selection.
        //     else
        //         take leftmost pool.
        //
        //  return FOUND
        //
        //  RESULT :
        //      RequestStatusCode.FOUND :
        //         file is on pool which is allowed and has reasonable cost.
        //      RequestStatusCode.NOT_FOUND :
        //         file is not in cache at all
        //      RequestStatusCode.NOT_PERMITTED :
        //         file not in an permitted pool but somewhere else
        //      RequestStatusCode.COST_EXCEEDED :
        //         file is in permitted pools but cost is too high.
        //      RequestStatusCode.ERROR :
        //         - No entry in configuration Permission Matrix
        //         - Code Exception
        //
        private RequestStatusCode askIfAvailable()
        {
           try {
               _bestPool = _poolSelector.selectReadPool();
               _parameter = _poolSelector.getCurrentPartition();
           } catch (FileNotInCacheException e) {
               _log.info("[read] {}", e.getMessage());
               return RequestStatusCode.NOT_FOUND;
           } catch (PermissionDeniedCacheException e) {
               _log.info("[read] {}", e.getMessage());
               return RequestStatusCode.NOT_PERMITTED;
           } catch (CostException e) {
               if (e.getPool() == null) {
                   _log.info("[read] {}", e.getMessage());
                   setError(125, e.getMessage());
                   return RequestStatusCode.ERROR;
               }

               _bestPool = e.getPool();
               _parameter = _poolSelector.getCurrentPartition();
               if (e.shouldTryAlternatives()) {
                   _log.info("[read] {} ({})", e.getMessage(), _bestPool.name());
                   return RequestStatusCode.COST_EXCEEDED;
               }
           } catch (CacheException e) {
               String err = "Read pool selection failed: " + e.getMessage();
               _log.warn(err);
               setError(130, err);
               return RequestStatusCode.ERROR;
           } catch (IllegalArgumentException e) {
               String err = "Read pool selection failed:" + e.getMessage();
               _log.error(err);
               setError(130, err);
               return RequestStatusCode.ERROR;
           } catch (RuntimeException e) {
               _log.error("Read pool selection failed", e);
               setError(130, "Read pool selection failed: " + e.toString());
               return RequestStatusCode.ERROR;
           } finally {
               _log.info("[read] Took  {} ms",
                         (System.currentTimeMillis() - _started));
           }

           _poolCandidate = _bestPool;
           setError(0,"");
           return RequestStatusCode.FOUND;
        }
        //
        // Result :
        //    FOUND :
        //        valid source/destination pair found fitting all constraints.
        //    NOT_PERMITTED :
        //        - already too many copies (_maxPnfsFileCopies)
        //        - file already everywhere (no destination found)
        //        - SAME_HOST_NEVER : but no valid combination found
        //    COST_EXCEEDED :
        //        - slope == 0 : all destination pools > costCut (p2p)
        //          else       : (best destination) > ( slope * source )
        //    S_COST_EXCEEDED :
        //        - all source pools > alert
        //    ERROR
        //        - no source pool (code problem)
        //
        private RequestStatusCode askForPoolToPool(boolean overwriteCost)
        {
            try {
                Partition.P2pPair pools =
                    _poolSelector.selectPool2Pool(_poolGroup, overwriteCost);

                _p2pSourcePool = pools.source;
                _p2pDestinationPool = pools.destination;
                _log.info("[p2p] source={};dest={}",
                          _p2pSourcePool, _p2pDestinationPool);
                sendPool2PoolRequest(_p2pSourcePool, _p2pDestinationPool);

                return RequestStatusCode.FOUND;
            } catch (PermissionDeniedCacheException e) {
                setError(e.getRc(), e.getMessage());
                _log.info("[p2p] {}", e.toString());
                return RequestStatusCode.NOT_PERMITTED;
            } catch (SourceCostException e) {
                setError(e.getRc(), e.getMessage());
                _log.info("[p2p] {}", e.getMessage());
                return RequestStatusCode.S_COST_EXCEEDED;
            } catch (DestinationCostException e) {
                setError(e.getRc(), e.getMessage());
                _log.info("[p2p] {}", e.getMessage());
                return RequestStatusCode.COST_EXCEEDED;
            } catch (CacheException e) {
                setError(e.getRc(), e.getMessage());
                _log.warn("[p2p] {}", e.getMessage());
                return RequestStatusCode.ERROR;
            } catch (IllegalArgumentException e) {
                setError(128, e.getMessage());
                _log.error("[p2p] {}", e.getMessage());
                return RequestStatusCode.ERROR;
            } catch (RuntimeException e) {
                setError(128, e.getMessage());
                _log.error("[p2p] contact support@dcache.org", e);
                return RequestStatusCode.ERROR;
            } finally {
                _log.info("[p2p] Selection took {} ms",
                          (System.currentTimeMillis() - _started));
            }
        }

        //
        //   FOUND :
        //        - pool candidate found
        //   NOT_FOUND :
        //        - no pools configured
        //        - pools configured but not active
        //        - no pools left after subtracting primary candidate.
        //   OUT_OF_RESOURCES :
        //        - too many requests queued
        //
        private RequestStatusCode askForStaging()
        {
            try {
                SelectedPool pool =  _poolSelector.selectStagePool(_stageCandidate.map(SelectedPool::info));
                _poolCandidate = pool;
                _stageCandidate = Optional.of(pool);

                _log.info("[staging] poolCandidate -> {}", _poolCandidate.info());
                if (!sendFetchRequest(_poolCandidate)) {
                    return RequestStatusCode.OUT_OF_RESOURCES;
                }

                setError(0,"");

                return RequestStatusCode.FOUND;
            } catch (CostException e) {
               if (e.getPool() != null) {
                   _poolCandidate = e.getPool();
                   _stageCandidate = Optional.of(_poolCandidate);
                   return RequestStatusCode.FOUND;
               }
               _log.info("[stage] {}", e.getMessage());
               setError(125, e.getMessage());
               return RequestStatusCode.ERROR;
            } catch (CacheException e) {
                setError(e.getRc(), e.getMessage());
                _log.warn("[stage] {}", e.getMessage());
                return RequestStatusCode.NOT_FOUND;
            } catch (IllegalArgumentException e) {
                setError(128, e.getMessage());
                _log.error("[stage] {}", e.getMessage());
                return RequestStatusCode.ERROR;
            } catch (RuntimeException e) {
                setError(128, e.getMessage());
                _log.error("[stage] contact support@dcache.org", e);
                return RequestStatusCode.ERROR;
            } finally {
                _log.info("[stage] Selection took {} ms",
                          (System.currentTimeMillis() - _started));
            }
        }

        private void sendInfoMessage(int rc, String infoMessage)
        {
            WarningPnfsFileInfoMessage info =
                    new WarningPnfsFileInfoMessage("PoolManager", getCellAddress(), _pnfsId, rc, infoMessage);
            info.setStorageInfo(_fileAttributes.getStorageInfo());
            info.setFileSize(_fileAttributes.getSize());
            info.setBillingPath(_billingPath);
            info.setTransferPath(_transferPath);
            _billing.notify(info);
        }

        private void sendHitMsg(PoolInfo pool, boolean cached)
        {
            PoolHitInfoMessage msg = new PoolHitInfoMessage(pool == null ? null : pool.getAddress(), _pnfsId);
            msg.setBillingPath(_billingPath);
            msg.setTransferPath(_transferPath);
            msg.setFileCached(cached);
            msg.setStorageInfo(_fileAttributes.getStorageInfo());
            msg.setFileSize(_fileAttributes.getSize());
            msg.setProtocolInfo(_protocolInfo);
            _billing.notify(msg);
        }
    }

    public void setStageConfigurationFile(String path)
    {
        _stagePolicyDecisionPoint = new CheckStagePermission(path);
        _stagePolicyDecisionPoint.setAllowAnonymousStaging(_allowAnonymousStaging);
    }

    public void setAllowAnonymousStaging(boolean isAllowed)
    {
        _allowAnonymousStaging = isAllowed;
        if (_stagePolicyDecisionPoint != null) {
            _stagePolicyDecisionPoint.setAllowAnonymousStaging(isAllowed);
        }
    }

    private class PoolPingThread extends Thread
    {
        private PoolPingThread()
        {
            super("Container-ping");
        }

        public void run()
        {
            try {
                while (!Thread.interrupted()) {
                    try {
                        synchronized (this) {
                            wait(_checkFilePingTimer);
                        }

                        long now = System.currentTimeMillis();

                        // Determine which pools to query
                        List<PoolRequestHandler> list;
                        synchronized (_handlerHash) {
                            list = new ArrayList<>(_handlerHash.values());
                        }
                        Multimap<CellAddressCore, PoolRequestHandler> p2pRequests = ArrayListMultimap.create();
                        Multimap<CellAddressCore, PoolRequestHandler> stageRequests = ArrayListMultimap.create();
                        for (PoolRequestHandler handler : list) {
                            if (handler._started < now - _checkFilePingTimer) {
                                SelectedPool pool;
                                switch (handler._state) {
                                case ST_WAITING_FOR_POOL_2_POOL:
                                    pool = handler._p2pDestinationPool;
                                    if (pool != null) {
                                        p2pRequests.put(pool.address(), handler);
                                    }
                                    break;
                                case ST_WAITING_FOR_STAGING:
                                    pool = handler._poolCandidate;
                                    if (pool != null) {
                                        stageRequests.put(pool.address(), handler);
                                    }
                                    break;
                                }
                            }
                        }

                        // Send query to all pools
                        Map<CellAddressCore, ListenableFuture<String>> futures = new HashMap<>();
                        for (CellAddressCore pool : p2pRequests.keySet()) {
                            futures.put(pool, _poolStub.send(new CellPath(pool), "pp ls", String.class));
                        }
                        for (CellAddressCore pool : stageRequests.keySet()) {
                            futures.put(pool, _poolStub.send(new CellPath(pool), "rh ls", String.class));
                        }

                        // Collect replies
                        for (Map.Entry<CellAddressCore, ListenableFuture<String>> entry : futures.entrySet()) {
                            String reply;
                            try {
                                reply = CellStub.get(entry.getValue());
                            } catch (NoRouteToCellException | CacheException ignored) {
                                reply = "";
                            }

                            CellAddressCore address = entry.getKey();
                            for (PoolRequestHandler handler : p2pRequests.get(address)) {
                                if (!reply.contains(handler._pnfsId.toString())) {
                                    handler.add(new PingFailure(address));
                                }
                            }
                            for (PoolRequestHandler handler : stageRequests.get(address)) {
                                if (!reply.contains(handler._pnfsId.toString())) {
                                    handler.add(new PingFailure(address));
                                }
                            }
                        }
                    } catch (RuntimeException e) {
                        _log.error("Pool ping failed", e);
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    private static class PingFailure
    {
        private final CellAddressCore pool;

        private PingFailure(CellAddressCore pool)
        {
            this.pool = pool;
        }

        public CellAddressCore getPool()
        {
            return pool;
        }
    }
}
