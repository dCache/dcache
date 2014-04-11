package diskCacheV111.poolManager ;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.CostException;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.ExtendedRunnable;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SourceCostException;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
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
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.util.Args;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;

public class RequestContainerV5
    extends AbstractCellComponent
    implements Runnable, CellCommandListener, CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(RequestContainerV5.class);

    /**
     * State of CheckFilePingHandler.
     */
    private enum PingState { STOPPED, WAITING, QUERYING }

    public enum RequestState { ST_INIT, ST_DONE, ST_POOL_2_POOL,
            ST_STAGE, ST_WAITING, ST_WAITING_FOR_STAGING,
            ST_WAITING_FOR_POOL_2_POOL, ST_SUSPENDED }

    private static final String POOL_UNKNOWN_STRING  = "<unknown>" ;

    private static final String STRING_NEVER      = "never" ;
    private static final String STRING_BESTEFFORT = "besteffort" ;
    private static final String STRING_NOTCHECKED = "notchecked" ;

    /** value in milliseconds */
    private static final int DEFAULT_RETRY_INTERVAL = 60000;

    private final Map<UOID, PoolRequestHandler>     _messageHash   = new HashMap<>() ;
    private final Map<String, PoolRequestHandler>   _handlerHash   = new HashMap<>() ;

    private CellStub _billing;
    private long        _retryTimer    = 15 * 60 * 1000 ;

    private int         _maxRequestClumping = 1 ;

    private String      _onError       = "suspend" ;
    private int         _maxRetries    = 3 ;
    private int         _maxRestore    = -1 ;

    private CheckStagePermission _stagePolicyDecisionPoint;

    private boolean     _sendHitInfo;

    private int         _restoreExceeded;
    private boolean     _suspendIncoming;
    private boolean     _suspendStaging;

    private PoolSelectionUnit  _selectionUnit;
    private PoolMonitorV5      _poolMonitor;
    private PnfsHandler        _pnfsHandler;
    private final SimpleDateFormat   _formatter        = new SimpleDateFormat ("MM.dd HH:mm:ss");
    private Executor _executor;
    private final Map<PnfsId, CacheException>            _selections       = new HashMap<>() ;
    private PartitionManager   _partitionManager ;
    private long               _checkFilePingTimer = 10 * 60 * 1000 ;
    /** value in milliseconds */
    private final int _stagingRetryInterval;

    private final Thread _tickerThread;

    /**
     * Tape Protection.
     * allStates defines that all states are allowed.
     * allStatesExceptStage defines that all states except STAGE are allowed.
     */
    public static final EnumSet<RequestState> allStates =
        EnumSet.allOf(RequestState.class);

    public static final EnumSet<RequestState> allStatesExceptStage =
        EnumSet.complementOf(EnumSet.of(RequestState.ST_STAGE));

    public RequestContainerV5( int stagingRetryInterval) {
        _stagingRetryInterval = stagingRetryInterval;
        _tickerThread = new Thread(this, "Container-ticker");
        _tickerThread.start();
    }

    public RequestContainerV5()
    {
        this( DEFAULT_RETRY_INTERVAL);
    }

    public void shutdown()
    {
        _tickerThread.interrupt();
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
                Thread.sleep(_stagingRetryInterval) ;

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
        _log.info("Restore Manager : got 'poolRestarted' for " + poolName);
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
                            _log.info("Restore Manager : retrying : " + rph);
                            rph.retry();
                        }
                    case PoolStatusChangedMessage.DOWN:
                        /*
                         * if pool is down, re-try all request scheduled to this
                         * pool
                         */
                        if (rph.getPoolCandidate().equals(poolName) ) {
                            _log.info("Restore Manager : retrying : " + rph);
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
       pw.println( "      Restore Limit : "+(_maxRestore<0?"unlimited":(""+_maxRestore)));
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
            .println(_maxRestore<0?"unlimited":(""+_maxRestore));
    }

    public final static String hh_rc_set_sameHostCopy =
        STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED;
    public String ac_rc_set_sameHostCopy_$_1(Args args)
    {
        _partitionManager.setProperties("default", ImmutableMap.of("sameHostCopy", args.argv(0)));
        return "";
    }

    public final static String hh_rc_set_sameHostRetry =
        STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED;
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
    public String ac_rc_set_poolpingtimer_$_1(Args args ){
       _checkFilePingTimer = 1000L * Long.parseLong(args.argv(0));
       return "" ;
    }
    public static final String hh_rc_set_retry = "<retryTimer/seconds>" ;
    public String ac_rc_set_retry_$_1(Args args ){
       _retryTimer = 1000L * Long.parseLong(args.argv(0));
       return "" ;
    }
    public static final String hh_rc_set_max_retries = "<maxNumberOfRetries>" ;
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
    public static final String hh_rc_failed = "<pnfsId> [<errorNumber> [<errorMessage>]]" ;
    public String ac_rc_failed_$_1_3( Args args )
    {
       int    errorNumber = args.argc() > 1 ? Integer.parseInt(args.argv(1)) : 1;
       String errorString = args.argc() > 2 ? args.argv(2) : "Operator Intervention" ;

       PoolRequestHandler rph;

       synchronized( _handlerHash ){
          rph = _handlerHash.get(args.argv(0));
          if( rph == null ) {
              throw new
                      IllegalArgumentException("Not found : " + args.argv(0));
          }
       }
       rph.failed(errorNumber,errorString) ;
       return "" ;
    }
    public static final String hh_rc_destroy = "<pnfsId> # !!!  use with care" ;
    public String ac_rc_destroy_$_1( Args args )
    {

       PoolRequestHandler rph;

       synchronized( _handlerHash ){
          rph = _handlerHash.get(args.argv(0));
          if( rph == null ) {
              throw new
                      IllegalArgumentException("Not found : " + args.argv(0));
          }

          _handlerHash.remove( args.argv(0) ) ;
       }
       return "" ;
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
       return list.toArray( new RestoreHandlerInfo[list.size()] ) ;
    }

    public void messageArrived(CellMessage envelope,
                               PoolMgrSelectReadPoolMsg request)
        throws PatternSyntaxException, IOException
    {
        boolean enforceP2P = false ;

        PnfsId       pnfsId       = request.getPnfsId() ;
        ProtocolInfo protocolInfo = request.getProtocolInfo() ;
        EnumSet<RequestState> allowedStates = request.getAllowedStates();

        String  hostName    =
               protocolInfo instanceof IpProtocolInfo ?
               ((IpProtocolInfo)protocolInfo).getSocketAddress().getAddress().getHostAddress() :
               "NoSuchHost" ;

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
        String canonicalName = pnfsId +"@"+netName+"-"+protocolName+(enforceP2P?"-p2p":"")  ;
        //
        //
        PoolRequestHandler handler;
        _log.info( "Adding request for : "+canonicalName ) ;
        synchronized( _handlerHash ){
           //
           handler = _handlerHash.get(canonicalName);
           if( handler == null ){
              _handlerHash.put(
                     canonicalName ,
                     handler = new PoolRequestHandler( pnfsId , canonicalName, allowedStates ) ) ;
           }
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

            sendMessage( new CellMessage(new CellPath("PoolManager"), req) );

        } catch (NoRouteToCellException e) {
            commandReply = "P2P failed : " + e.getMessage();
        } catch (CacheException e) {
            commandReply = "P2P failed : " + e.getMessage();
        }

        return commandReply;
    }

    ///////////////////////////////////////////////////////////////
    //
    // the read io request handler
    //
    private class PoolRequestHandler  {

        protected PnfsId       _pnfsId;
        protected final List<CellMessage>    _messages = new ArrayList<>() ;
        protected int _retryCounter;
        private final CDC _cdc = new CDC();


        private   UOID         _waitingFor;
        private   long         _waitUntil;

        private   String       _status        = "[<idle>]";
        private   RequestState _state         = RequestState.ST_INIT;
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
        private   PoolInfo    _bestPool;

        /**
         * The pool from which to read the file or the pool to which
         * to stage the file. Set by askIfAvailable() when it returns
         * RT_FOUND, by exercisePool2PoolReply() when it returns
         * RT_OK, and by askForStaging(). Also set in the
         * stateEngine() at various points.
         */
        private   PoolInfo    _poolCandidate;

        /**
         * The host name of the pool used for staging.
         *
         * Serves a critical role when retrying staging to avoid that
         * the same stage host is chosen twice in a row.
         */
        private   String     _stageCandidateHost;

        /**
         * The name of the pool used for staging.
         *
         * Serves a critical role when retrying staging to avoid that
         * the same pool is chosen twice in a row.
         */
        private   String     _stageCandidatePool;

        /**
         * The destination of a pool to pool transfer. Set by
         * askForPoolToPool() when it returns RT_FOUND.
         */
        private   PoolInfo   _p2pDestinationPool;

        /**
         * The source of a pool to pool transfer. Set by
         * askForPoolToPool() when it return RT_FOUND.
         */
        private   PoolInfo   _p2pSourcePool;

        private   final long   _started       = System.currentTimeMillis() ;
        private   String       _name;

        private   FileAttributes _fileAttributes;
        private   StorageInfo  _storageInfo;
        private   ProtocolInfo _protocolInfo;
        private   String       _linkGroup;
        private   FsPath _path;

        private   boolean _enforceP2P;
        private   int     _destinationFileStatus = Pool2PoolTransferMsg.UNDETERMINED ;

        private CheckFilePingHandler  _pingHandler = new CheckFilePingHandler(_checkFilePingTimer) ;

        private PoolSelector _poolSelector;
        private Partition _parameter = _partitionManager.getDefaultPartition();

        /**
         * Indicates the next time a TTL of a request message will be
         * exceeded.
         */
        private long _nextTtlTimeout = Long.MAX_VALUE;

        private class CheckFilePingHandler {
            private long _timeInterval;
            private long _timer;
            private PoolInfo _candidate;
            private PingState _state = PingState.STOPPED;
            private String _query;

            private CheckFilePingHandler(long timerInterval)
            {
                _timeInterval = timerInterval;
            }

            private void startP2P(PoolInfo candidate)
            {
                if (_timeInterval <= 0L || candidate == null) {
                    return;
                }
                _candidate = candidate;
                _timer = _timeInterval + System.currentTimeMillis();
                _state = PingState.WAITING;
                _query = "pp ls";
            }

            private void startStage(PoolInfo candidate)
            {
                if (_timeInterval <= 0L || candidate == null) {
                    return;
                }
                _candidate = candidate;
                _timer = _timeInterval + System.currentTimeMillis();
                _state = PingState.WAITING;
                _query = "rh ls";
            }

            private void stop()
            {
                _candidate = null;
                _state = PingState.STOPPED;
                synchronized (_messageHash) {
                    if (_waitingFor != null) {
                        _messageHash.remove(_waitingFor);
                    }
                }
            }

            private void alive()
            {
                if ((_candidate == null) || (_timer == 0L)) {
                    return;
                }

                long now = System.currentTimeMillis();
                if (now > _timer) {
                    switch (_state) {
                    case WAITING:
                        _log.info("CheckFilePingHandler : sending " + _query + " to " + _candidate);
                        sendQuery();
                        _state = PingState.QUERYING;
                        break;

                    case QUERYING:
                        /* No reply since last query.
                         */
                        _log.info("CheckFilePingHandler : request died");
                        stop();
                        setError(CacheException.TIMEOUT,
                                 "Replication/staging timed out");
                        errorHandler();
                        break;

                    case STOPPED:
                        return;
                    }
                    _timer = _timeInterval + now;
                }
            }

            private void gotReply(Object object)
            {
                if (_state == PingState.QUERYING && object instanceof String) {
                    String s = (String) object;
                    if (s.contains(_pnfsId.toString())) {
                        _log.info("CheckFilePingHandler : request is alive");
                        _state = PingState.WAITING;
                    }
                }
            }

            private void sendQuery()
            {
                CellMessage envelope =
                    new CellMessage(new CellPath(_candidate.getAddress()), _query);
                synchronized (_messageHash) {
                    try {
                        sendMessage(envelope);
                        _waitingFor = envelope.getUOID();
                        _messageHash.put(_waitingFor, PoolRequestHandler.this);
                    } catch (NoRouteToCellException e) {
                        _log.warn("Can't send pool ping to {}: {}",
                                  _candidate, e.toString());
                    }
                }
            }
        }

        public PoolRequestHandler(PnfsId pnfsId, String canonicalName,
                                  Collection<RequestState> allowedStates)
        {
	    _pnfsId  = pnfsId ;
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

           PoolMgrSelectReadPoolMsg request =
                (PoolMgrSelectReadPoolMsg)message.getMessageObject() ;

           _linkGroup = request.getLinkGroup();
           _protocolInfo = request.getProtocolInfo();
           _fileAttributes = request.getFileAttributes();
           _storageInfo = _fileAttributes.getStorageInfo();
           _path = request.getPnfsPath();

           _retryCounter = request.getContext().getRetryCounter();
           _stageCandidateHost = request.getContext().getPreviousStageHost();
           _stageCandidatePool = request.getContext().getPreviousStagePool();

           if( request instanceof PoolMgrReplicateFileMsg ){
              _enforceP2P            = true ;
              _destinationFileStatus = ((PoolMgrReplicateFileMsg)request).getDestinationFileStatus() ;
           }

           _poolSelector =
               _poolMonitor.getPoolSelector(_fileAttributes,
                       _protocolInfo,
                       _linkGroup);
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
                return _poolCandidate.getName();
            } else if (_p2pDestinationPool != null) {
                return _p2pDestinationPool.getName();
            } else {
                return POOL_UNKNOWN_STRING;
            }
        }

        private String getPoolCandidateState()
        {
            if (_poolCandidate != null) {
                return _poolCandidate.getName();
            } else if (_p2pDestinationPool != null) {
                return (_p2pSourcePool == null ? POOL_UNKNOWN_STRING : _p2pSourcePool)
                    + "->" + _p2pDestinationPool;
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
        private void waitFor( long millis ){
           _waitUntil = System.currentTimeMillis() + millis ;
        }
        private void clearSteering(){
           synchronized( _messageHash ){

              if( _waitingFor != null ) {
                  _messageHash.remove(_waitingFor);
              }
           }
           _waitingFor = null ;
           _waitUntil  = 0L ;

           //
           // and the ping handler
           //
           _pingHandler.stop() ;


        }
        private void setError( int errorCode , String errorMessage ){
           _currentRc = errorCode ;
           _currentRm = errorMessage ;
        }

	private boolean sendFetchRequest(PoolInfo pool)
            throws NoRouteToCellException
        {
	    CellMessage cellMessage = new CellMessage(
                                new CellPath(pool.getAddress()),
	                        new PoolFetchFileMessage(
                                        pool.getName(),
                                        _fileAttributes)
                                );
            synchronized( _messageHash ){
                if( ( _maxRestore >=0 ) &&
                    ( _messageHash.size() >= _maxRestore ) ) {
                    return false;
                }
                sendMessage( cellMessage );
                _poolMonitor.messageToCostModule( cellMessage ) ;
                _messageHash.put( _waitingFor = cellMessage.getUOID() , this ) ;
                _status = "Staging "+_formatter.format(new Date()) ;
            }
            return true ;
	}
	private void sendPool2PoolRequest(PoolInfo sourcePool, PoolInfo destPool)
            throws NoRouteToCellException
        {
            Pool2PoolTransferMsg pool2pool =
                  new Pool2PoolTransferMsg(sourcePool.getName(), destPool.getName(), _fileAttributes);
            pool2pool.setDestinationFileStatus( _destinationFileStatus ) ;
            _log.info("[p2p] Sending transfer request: "+pool2pool);
	    CellMessage cellMessage =
                new CellMessage(new CellPath(destPool.getAddress()), pool2pool);

            synchronized( _messageHash ){
                sendMessage( cellMessage );
                _poolMonitor.messageToCostModule( cellMessage ) ;
                if( _waitingFor != null ) {
                    _messageHash.remove(_waitingFor);
                }
                _messageHash.put( _waitingFor = cellMessage.getUOID() , this ) ;
                _status = "[P2P "+_formatter.format(new Date())+"]" ;
            }
	}

        /**
         * Removes request messages whos time to live has been
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
                        _log.info("Discarding request from "
                                  + message.getSourcePath().getCellName()
                                  + " because its time to live has been exceeded.");
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
                rpm.setContext(_retryCounter + 1, _stageCandidateHost, _stageCandidatePool);
                if (_currentRc == 0) {
                    rpm.setPoolName(_poolCandidate.getName());
                    rpm.setPoolAddress(_poolCandidate.getAddress());
                    rpm.setSucceeded();
                } else {
                    rpm.setFailed(_currentRc, _currentRm);
                }
                try {
                    m.revertDirection();
                    sendMessage(m);
                    _poolMonitor.messageToCostModule(m);
                    if (!rpm.getSkipCostUpdate()) {
                        _poolMonitor.messageToCostModule(m);
                    }
                } catch (NoRouteToCellException e) {
                    _log.warn("Exception answering request: {}", e.toString());
                }
                messages.remove();
            }
            return messages.hasNext();
        }
        //
        // and the heart ...
        //
        private static final int RT_OK         = 1 ;
        private static final int RT_FOUND      = 2 ;
        private static final int RT_NOT_FOUND  = 3 ;
        private static final int RT_ERROR      = 4 ;
        private static final int RT_OUT_OF_RESOURCES = 5 ;
        private static final int RT_COST_EXCEEDED    = 7 ;
        private static final int RT_NOT_PERMITTED    = 8 ;
        private static final int RT_S_COST_EXCEEDED  = 9 ;
        private static final int RT_DELAY  = 10 ;

        private static final int CONTINUE        = 0 ;
        private static final int WAIT            = 1 ;

        private final Deque<Object> _fifo              = new LinkedList<>() ;
        private boolean    _stateEngineActive;
        private boolean    _forceContinue;
        private boolean    _overwriteCost;

        public class RunEngine implements ExtendedRunnable {
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
           public void runFailed(){
              synchronized( _fifo ){
                   _stateEngineActive = false ;
              }
           }

           @Override
           public String toString() {
              return PoolRequestHandler.this.toString();
           }
        }
        private void add( Object obj ){
           synchronized( _fifo ){
               _log.info( "Adding Object : "+obj ) ;
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
           _log.info( "ACTIVATING STATE ENGINE "+_pnfsId+" "+(System.currentTimeMillis()-_started)) ;

           while( ! Thread.interrupted() ){

              if( ! _forceContinue ){

                 synchronized( _fifo ){
                    if( _fifo.size() == 0 ){
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
                 _log.info("StageEngine called in mode " +
                           _state + " with object " +
                        (  inputObject == null ?
                             "(NULL)":
                            (  inputObject instanceof Object [] ?
                                 ((Object[])inputObject)[0].toString() :
                                 inputObject.getClass().getName()
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
                    if (_stagePolicyDecisionPoint.canPerformStaging(msg.getSubject(), msg.getStorageInfo())) {
                        return true;
                    }
                } catch (IOException | PatternSyntaxException e) {
                    _log.error("Failed to verify stage permissions: " + e.getMessage());
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
                sendInfoMessage(_pnfsId , _path, _fileAttributes,
                                _currentRc , "Failed "+_currentRm);
            } else {
                if (state == RequestState.ST_STAGE && !canStage()) {
                    _state = RequestState.ST_DONE;
                    _forceContinue = true;
                    _status = "Failed";
                    _log.debug("Subject is not authorized to stage");
                    _currentRc = CacheException.FILE_NOT_ONLINE;
                    _currentRm = "File not online. Staging not allowed.";
                    sendInfoMessage(_pnfsId , _path, _fileAttributes,
                                    _currentRc , "Permission denied." + _currentRm);
                } else if (!_allowedStates.contains(state)) {
                    _state = RequestState.ST_DONE;
                    _forceContinue = true;
                    _status = "Failed";
                    _log.debug("No permission to perform {}", state);
                    _currentRc = CacheException.PERMISSION_DENIED;
                    _currentRm = "Permission denied.";
                    sendInfoMessage(_pnfsId, _path, _fileAttributes, _currentRc,
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
        //      RT_FOUND :
        //
        //         Because : file is on pool which is allowed and has reasonable cost.
        //
        //         -> DONE
        //
        //      RT_NOT_FOUND :
        //
        //         Because : file is not in cache at all
        //
        //         (bestPool=0)
        //
        //         -> _hasHsmBackend : STAGE
        //              else         : Suspended (1010, pool unavailable)
        //
        //      RT_NOT_PERMITTED :
        //
        //         Because : file not in an permitted pool but somewhere else
        //
        //         (bestPool=0,overwriteCost=true)
        //
        //         -> _p2pAllowed ||
        //            ! _hasHsmBackend  : P2P
        //            else              : STAGE
        //
        //      RT_COST_EXCEEDED :
        //
        //         Because : file is in permitted pools but cost is too high.
        //
        //         -> _p2pOnCost          : P2P
        //            _hasHsmBackend &&
        //            _stageOnCost        : STAGE
        //            else                : 127 , "Cost exceeded (st,p2p not allowed)"
        //
        //      RT_ERROR :
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
        //      RT_FOUND :
        //
        //         Because : source and destination pool found and cost ok.
        //
        //         -> DONE
        //
        //      RT_NOT_PERMITTED :
        //
        //         Because : - already too many copies (_maxPnfsFileCopies)
        //                   - file already everywhere (no destination found)
        //                   - SAME_HOST_NEVER : but no valid combination found
        //
        //         -> DONE 'using bestPool'
        //
        //      RT_S_COST_EXCEEDED (only if ! overwriteCost ) :
        //
        //         Because : best source pool exceeds 'alert' cost.
        //
        //         -> _hasHsmBackend &&
        //            _stageOnCost    : STAGE
        //            bestPool == 0   : 194,"File not present in any reasonable pool"
        //            else            : DONE 'using bestPool'
        //
        //      RT_COST_EXCEEDED (only if ! overwriteCost )  :
        //
        //         Because : file is in permitted pools but cost of
        //                   best destination pool exceeds cost of best
        //                   source pool (resp. slope * source).
        //
        //         -> _bestPool == 0 : 192,"File not present in any reasonable pool"
        //            else           : DONE 'using bestPool'
        //
        //      RT_ERROR :
        //
        //         Because : - no source pool (code problem)
        //                   - Code Exception
        //
        //         -> 132,"PANIC : Tried to do p2p, but source was empty"
        //                or exception text.
        //
        //  askForStaging :
        //
        //      RT_FOUND :
        //
        //         Because : destination pool found and cost ok.
        //
        //         -> DONE
        //
        //      RT_NOT_FOUND :
        //
        //         -> 149 , "No pool candidates available or configured for 'staging'"
        //         -> 150 , "No cheap candidates available for 'staging'"
        //
        //      RT_ERROR :
        //
        //         Because : - Code Exception
        //
        private void stateEngine( Object inputObject ) {
           int rc;
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

                    if( ( rc = askIfAvailable() ) == RT_FOUND ){

                       setError(0,"");
                       nextStep(RequestState.ST_DONE , CONTINUE ) ;
                       _log.info("AskIfAvailable found the object");
                       if (_sendHitInfo ) {
                           sendHitMsg(_pnfsId, _path, (_bestPool != null) ? _bestPool.getName() : "<UNKNOWN>",
                                      _fileAttributes, _protocolInfo, true);   //VP
                       }

                    }else if( rc == RT_NOT_FOUND ){
                       //
                       //
                        _log.debug(" stateEngine: RT_NOT_FOUND ");
                       if( _parameter._hasHsmBackend && _storageInfo.isStored()){
                           _log.debug(" stateEngine: parameter has HSM backend and the file is stored on tape ");
                          nextStep(RequestState.ST_STAGE , CONTINUE ) ;
                       }else{
                          _log.debug(" stateEngine: case 1: parameter has NO HSM backend or case 2: the HSM backend exists but the file isn't stored on it.");
                          _poolCandidate = null ;
                          setError(1010, "Pool unavailable");
                          suspendIfEnabled("Suspended (pool unavailable)");
                       }
                       if (_sendHitInfo && _poolCandidate == null) {
                           sendHitMsg(  _pnfsId, _path, (_bestPool!=null)?_bestPool.getName():"<UNKNOWN>",
                                        _fileAttributes, _protocolInfo, false );   //VP
                       }
                       //
                    }else if( rc == RT_NOT_PERMITTED ){
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

                    }else if( rc == RT_COST_EXCEEDED ){

                       if( _parameter._p2pOnCost ){

                           nextStep(RequestState.ST_POOL_2_POOL , CONTINUE ) ;

                       }else if( _parameter._hasHsmBackend &&  _parameter._stageOnCost ){

                           nextStep(RequestState.ST_STAGE , CONTINUE ) ;

                       }else{

                           setError( 127 , "Cost exceeded (st,p2p not allowed)" ) ;
                           nextStep(RequestState.ST_DONE , CONTINUE ) ;

                       }
                    }else if( rc == RT_ERROR ){
                       _log.debug( " stateEngine: RT_ERROR");
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

                    if( ( rc = askForPoolToPool( _overwriteCost ) ) == RT_FOUND ){

                       nextStep(RequestState.ST_WAITING_FOR_POOL_2_POOL , WAIT ) ;
                       _status = "Pool2Pool "+_formatter.format(new Date()) ;
                       setError(0,"");
                       _pingHandler.startP2P(_p2pDestinationPool) ;

                       if (_sendHitInfo ) {
                           sendHitMsg(_pnfsId, _path,
                                   (_p2pSourcePool != null) ? _p2pSourcePool.getName() : "<UNKNOWN>",
                                   _fileAttributes, _protocolInfo, true);   //VP
                       }

                    }else if( rc == RT_NOT_PERMITTED ){

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
                            _log.info("ST_POOL_2_POOL : Choosing high cost pool "+_poolCandidate);

                          setError(0,"");
                          nextStep(RequestState.ST_DONE , CONTINUE ) ;
                        }

                    }else if( rc == RT_S_COST_EXCEEDED ){

                       _log.info("ST_POOL_2_POOL : RT_S_COST_EXCEEDED");

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
                              _log.info("ST_POOL_2_POOL : Choosing high cost pool "+_poolCandidate);

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
                    }else if( rc == RT_COST_EXCEEDED ){
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

                    if( ( rc = askForStaging() ) == RT_FOUND ){

                       nextStep(RequestState.ST_WAITING_FOR_STAGING , WAIT ) ;
                       _status = "Staging "+_formatter.format(new Date()) ;
                       setError(0,"");
                       _pingHandler.startStage(_poolCandidate) ;

                    }else if( rc == RT_OUT_OF_RESOURCES ){

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
                 if( inputObject instanceof Message ){

                    if( ( rc =  exercisePool2PoolReply((Message)inputObject) ) == RT_OK ){
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

                 }else{
                     _pingHandler.gotReply(inputObject);
                 }

              break ;
              case ST_WAITING_FOR_STAGING :
                 _log.debug( "stateEngine: case ST_WAITING_FOR_STAGING" );
                 if( inputObject instanceof Message ){

                    if( ( rc =  exerciseStageReply( (Message)inputObject ) ) == RT_OK ){
                        if (_parameter._p2pForTransfer) {
                            setError(CacheException.OUT_OF_DATE,
                                     "Pool locations changed due to stage");
                            nextStep(RequestState.ST_DONE, CONTINUE);
                        } else {
                            nextStep(RequestState.ST_DONE, CONTINUE);
                        }
                    }else if( rc == RT_DELAY ){
                        suspend("Suspended By HSM request");
                    }else{

                       errorHandler() ;

                    }
                 }else if( inputObject instanceof Object [] ){

                    handleCommandObject( (Object []) inputObject ) ;

                 }else{
                     _pingHandler.gotReply(inputObject);
                 }
              break ;
              case ST_SUSPENDED :
                 _log.debug( "stateEngine: case ST_SUSPENDED" );
                 if( inputObject instanceof Object [] ){

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
                    synchronized( _handlerHash ){
                       if( answerRequest( _maxRequestClumping ) ){
                            setError(CacheException.RESOURCE,
                                     "Request clumping limit reached");
                            nextStep(RequestState.ST_DONE, CONTINUE);
                       }else{
                           _handlerHash.remove( _name ) ;
                       }
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

                if ((_waitUntil > 0L) && (now > _waitUntil)) {
                    clearSteering();
                    nextStep(_state, CONTINUE);
                } else {
                    _pingHandler.alive();
                }

                break;
            }

        }

        private void outOfResources( String detail ){

           clearSteering();
           setError(5,"Resource temporarily unavailable : "+detail);
           nextStep(RequestState.ST_DONE , CONTINUE ) ;
           _status = "Failed" ;
           sendInfoMessage( _pnfsId , _path, _fileAttributes,
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
            _status = status + " " + _formatter.format(new Date());
            nextStep(RequestState.ST_SUSPENDED, WAIT);
            sendInfoMessage(_pnfsId, _path, _fileAttributes,
                    _currentRc, "Suspended (" + _currentRm + ")");
        }

        private void suspendIfEnabled(String status)
        {
            if (_onError.equals("suspend")) {
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

        private int exerciseStageReply( Message messageArrived ){
           try{

              if( messageArrived instanceof PoolFetchFileMessage ){
                 PoolFetchFileMessage reply = (PoolFetchFileMessage)messageArrived ;

                 int rc;
                 _currentRc = reply.getReturnCode();

                 switch(_currentRc) {
                     case 0:
                         // best candidate is the right one
                         rc = RT_OK;
                         break;
                     case CacheException.HSM_DELAY_ERROR:
                         _currentRm = "Suspend by HSM request : " + reply.getErrorObject() == null ?
                                 "No info" : reply.getErrorObject().toString() ;
                         rc = RT_DELAY;
                         break;
                     default:
                         _currentRm = reply.getErrorObject() == null ?
                                 ( "Error="+_currentRc ) : reply.getErrorObject().toString() ;

                         rc =  RT_ERROR ;
                 }

                 return rc;

              }else{
                 throw new
                 CacheException(204,"Invalid message arrived : "+
                                messageArrived.getClass().getName());

              }
           } catch (CacheException e) {
              _currentRc = e.getRc();
              _currentRm = e.getMessage();
              _log.warn("exerciseStageReply: {} ", e.toString());
              return RT_ERROR;
           } catch (RuntimeException e) {
              _currentRc = 102;
              _currentRm = e.getMessage();
              _log.error("exerciseStageReply", e) ;
              return RT_ERROR;
           }
        }

        private int exercisePool2PoolReply( Message messageArrived ){
           try{

              if( messageArrived instanceof Pool2PoolTransferMsg ){
                 Pool2PoolTransferMsg reply = (Pool2PoolTransferMsg)messageArrived ;
                 _log.info("Pool2PoolTransferMsg replied with : "+reply);
                 if( ( _currentRc = reply.getReturnCode() ) == 0 ){
                     _poolCandidate = _p2pDestinationPool;
                    return RT_OK ;

                 }else{

                    _currentRm = reply.getErrorObject() == null ?
                                 ( "Error="+_currentRc ) : reply.getErrorObject().toString() ;

                    return RT_ERROR ;

                 }
              }else{

                 throw new
                 CacheException(205,"Invalid message arrived : "+
                                messageArrived.getClass().getName());

              }
           } catch (CacheException e) {
               _currentRc = e.getRc();
               _currentRm = e.getMessage();
               _log.warn("exercisePool2PoolReply: {}", e.toString());
               return RT_ERROR;
           } catch (RuntimeException e) {
               _currentRc = 102;
               _currentRm = e.getMessage();
               _log.error("exercisePool2PoolReply", e);
               return RT_ERROR;
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
        //      RT_FOUND :
        //         file is on pool which is allowed and has reasonable cost.
        //      RT_NOT_FOUND :
        //         file is not in cache at all
        //      RT_NOT_PERMITTED :
        //         file not in an permitted pool but somewhere else
        //      RT_COST_EXCEEDED :
        //         file is in permitted pools but cost is too high.
        //      RT_ERROR :
        //         - No entry in configuration Permission Matrix
        //         - Code Exception
        //
        private int askIfAvailable()
        {
           try {
               _bestPool = _poolSelector.selectReadPool();
               _parameter = _poolSelector.getCurrentPartition();
           } catch (FileNotInCacheException e) {
               _log.info("[read] {}", e.getMessage());
               return RT_NOT_FOUND;
           } catch (PermissionDeniedCacheException e) {
               _log.info("[read] {}", e.getMessage());
               return RT_NOT_PERMITTED;
           } catch (CostException e) {
               if (e.getPool() == null) {
                   _log.info("[read] {}", e.getMessage());
                   setError(125, e.getMessage());
                   return RT_ERROR;
               }

               _bestPool = e.getPool();
               _parameter = _poolSelector.getCurrentPartition();
               if (e.shouldTryAlternatives()) {
                   _log.info("[read] {} ({})",
                             e.getMessage(), _bestPool);
                   return RT_COST_EXCEEDED;
               }
           } catch (CacheException e) {
               String err = "Read pool selection failed: " + e.getMessage();
               _log.warn(err);
               setError(130, err);
               return RT_ERROR;
           } catch (RuntimeException e) {
               _log.error("Read pool selection failed", e);
               setError(130, "Read pool selection failed: " + e.toString());
               return RT_ERROR;
           } finally {
               _log.info("[read] Took  {} ms",
                         (System.currentTimeMillis() - _started));
           }

           _poolCandidate = _bestPool;
           setError(0,"");
           return RT_FOUND;
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
        private int askForPoolToPool(boolean overwriteCost)
        {
            try {
                Partition.P2pPair pools =
                    _poolSelector.selectPool2Pool(overwriteCost);

                _p2pSourcePool = pools.source;
                _p2pDestinationPool = pools.destination;
                _log.info("[p2p] source={};dest={}",
                          _p2pSourcePool, _p2pDestinationPool);
                sendPool2PoolRequest(_p2pSourcePool, _p2pDestinationPool);

                return RT_FOUND;
            } catch (PermissionDeniedCacheException e) {
                setError(e.getRc(), e.getMessage());
                _log.warn("[p2p] {}", e.toString());
                return RT_NOT_PERMITTED;
            } catch (SourceCostException e) {
                setError(e.getRc(), e.getMessage());
                _log.info("[p2p] {}", e.getMessage());
                return RT_S_COST_EXCEEDED;
            } catch (DestinationCostException e) {
                setError(e.getRc(), e.getMessage());
                _log.info("[p2p] {}", e.getMessage());
                return RT_COST_EXCEEDED;
            } catch (CacheException e) {
                setError(e.getRc(), e.getMessage());
                _log.warn("[p2p] {}", e.getMessage());
                return RT_ERROR;
            } catch (NoRouteToCellException e) {
                setError(128, e.getMessage());
                _log.error("[p2p] {}", e.toString());
                return RT_ERROR;
            } catch (RuntimeException e) {
                setError(128, e.getMessage());
                _log.error("[p2p] contact support@dcache.org", e);
                return RT_ERROR;
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
        private int askForStaging()
        {
            try {
                PoolInfo pool =
                    _poolSelector.selectStagePool(_stageCandidatePool, _stageCandidateHost);
                _poolCandidate = pool;
                _stageCandidatePool = pool.getName();
                _stageCandidateHost = pool.getHostName();

                _log.info("[staging] poolCandidate -> {}", _poolCandidate);
                if (!sendFetchRequest(_poolCandidate)) {
                    return RT_OUT_OF_RESOURCES;
                }

                setError(0,"");

                return RT_FOUND;
            } catch (CostException e) {
               if (e.getPool() != null) {
                   _poolCandidate = e.getPool();
                   _stageCandidatePool = e.getPool().getName();
                   _stageCandidateHost = e.getPool().getHostName();
                   return RT_FOUND;
               }
               _log.info("[stage] {}", e.getMessage());
               setError(125, e.getMessage());
               return RT_ERROR;
            } catch (CacheException e) {
                setError(e.getRc(), e.getMessage());
                _log.warn("[stage] {}", e.getMessage());
                return RT_NOT_FOUND;
            } catch (NoRouteToCellException e) {
                setError(128, e.getMessage());
                _log.error("[stage] {}", e.toString());
                return RT_ERROR;
            } catch (RuntimeException e) {
                setError(128, e.getMessage());
                _log.error("[stage] contact support@dcache.org", e);
                return RT_ERROR;
            } finally {
                _log.info("[stage] Selection took {} ms",
                          (System.currentTimeMillis() - _started));
            }
        }
    }

    private void sendInfoMessage( PnfsId pnfsId , FsPath path,
                                  FileAttributes fileAttributes,
                                  int rc , String infoMessage ){
      try{
        WarningPnfsFileInfoMessage info =
            new WarningPnfsFileInfoMessage(
                                    "PoolManager","PoolManager",pnfsId ,
                                    rc , infoMessage )  ;
        info.setStorageInfo(fileAttributes.getStorageInfo());
        info.setFileSize(fileAttributes.getSize());
        info.setPath(path);
        _billing.notify(info);
      } catch (NoRouteToCellException e) {
          _log.warn("Couldn't send WarningInfoMessage: {}", e.toString());
      }
    }

    private void sendHitMsg(PnfsId pnfsId, FsPath path, String poolName,
                            FileAttributes fileAttributes, ProtocolInfo protocolInfo, boolean cached)
    {
        try {
            PoolHitInfoMessage msg = new PoolHitInfoMessage(poolName, pnfsId);
            msg.setPath(path);
            msg.setFileCached(cached);
            msg.setStorageInfo(fileAttributes.getStorageInfo());
            msg.setFileSize(fileAttributes.getSize());
            msg.setProtocolInfo(protocolInfo);
            _billing.notify(msg);
        } catch (NoRouteToCellException e) {
            _log.warn("Couldn't report hit info for {}: {}",
                      pnfsId, e.toString());
        }
    }

    public void setStageConfigurationFile(String path)
    {
        _stagePolicyDecisionPoint = new CheckStagePermission(path);
    }
}
