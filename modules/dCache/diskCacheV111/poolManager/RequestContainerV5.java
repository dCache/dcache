// $Id: RequestContainerV5.java,v 1.62 2007-09-02 17:51:31 tigran Exp $

package diskCacheV111.poolManager ;

import java.io.PrintWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.ExtendedRunnable;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.ThreadPool;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;
import dmg.util.Args;
import java.util.NavigableMap;

public class RequestContainerV5
    extends AbstractCellComponent
    implements Runnable, CellCommandListener, CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(RequestContainerV5.class);

    /**
     * State of CheckFilePingHandler.
     */
    private enum PingState { STOPPED, WAITING, QUERYING };

    private static final String POOL_UNKNOWN_STRING  = "<unknown>" ;

    private static final String STRING_NEVER      = "never" ;
    private static final String STRING_BESTEFFORT = "besteffort" ;
    private static final String STRING_NOTCHECKED = "notchecked" ;

    private static final int SAME_HOST_RETRY_NEVER      = 0 ;
    private static final int SAME_HOST_RETRY_BESTEFFORT = 1 ;
    private static final int SAME_HOST_RETRY_NOTCHECKED = 2 ;
    /** value in milliseconds */
    private static final int DEFAULT_RETRY_INTERVAL = 60000;

    private final Map<UOID, PoolRequestHandler>     _messageHash   = new HashMap<UOID, PoolRequestHandler>() ;
    private final Map<String, PoolRequestHandler>   _handlerHash   = new HashMap<String, PoolRequestHandler>() ;

    private String      _warningPath   = "billing" ;
    private long        _retryTimer    = 15 * 60 * 1000 ;

    private int         _maxRequestClumping = 1 ;

    private String      _onError       = "suspend" ;
    private int         _maxRetries    = 3 ;
    private int         _maxRestore    = -1 ;

    private CheckStagePermission _stagePolicyDecisionPoint;

    private boolean     _sendHitInfo   = false ;

    private int         _restoreExceeded = 0 ;
    private boolean     _suspendIncoming = false ;
    private boolean     _suspendStaging  = false ;

    private PoolSelectionUnit  _selectionUnit;
    private PoolMonitorV5      _poolMonitor;
    private final SimpleDateFormat   _formatter        = new SimpleDateFormat ("MM.dd HH:mm:ss");
    private ThreadPool         _threadPool ;
    private final Map<PnfsId, CacheException>            _selections       = new HashMap<PnfsId, CacheException>() ;
    private PartitionManager   _partitionManager ;
    private long               _checkFilePingTimer = 10 * 60 * 1000 ;
    /** value in milliseconds */
    private final int _stagingRetryInterval;

    /**
     * define host selection behavior on restore retry
     */
    private int _sameHostRetry = SAME_HOST_RETRY_NOTCHECKED ;

    /**
     * Tape Protection.
     * allStates defines that all states are allowed.
     * allStatesExceptStage defines that all states except STAGE are allowed.
     */
    public static final int allStates = PoolRequestHandler.ST_STAGE | PoolRequestHandler.ST_INIT | PoolRequestHandler.ST_DONE
            | PoolRequestHandler.ST_POOL_2_POOL | PoolRequestHandler.ST_WAITING | PoolRequestHandler.ST_WAITING_FOR_STAGING
            | PoolRequestHandler.ST_WAITING_FOR_POOL_2_POOL | PoolRequestHandler.ST_SUSPENDED;

    public static final int allStatesExceptStage = PoolRequestHandler.ST_INIT | PoolRequestHandler.ST_DONE | PoolRequestHandler.ST_POOL_2_POOL
            | PoolRequestHandler.ST_WAITING | PoolRequestHandler.ST_WAITING_FOR_STAGING | PoolRequestHandler.ST_WAITING_FOR_POOL_2_POOL
            | PoolRequestHandler.ST_SUSPENDED;

    private static String modeToString(int mode){
        switch (mode) {
        case 1:
            return "ST_INIT";
        case 2:
            return "ST_DONE";
        case 4:
            return "ST_POOL_2_POOL";
        case 8:
            return "ST_STAGE";
        case 16:
            return "ST_WAITING";
        case 32:
            return "ST_WAITING_FOR_STAGING";
        case 64:
            return "ST_WAITING_FOR_POOL_2_POOL";
        case 128:
            return "ST_SUSPENDED";
       default:
            throw new IllegalArgumentException("Invalid mode : " + mode);
       }
    }

    public RequestContainerV5( int stagingRetryInterval) {
        _stagingRetryInterval = stagingRetryInterval;
        new Thread(this,"Container-ticker").start();
    }

    public RequestContainerV5()
    {
        this( DEFAULT_RETRY_INTERVAL);
    }

    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    public void setPoolMonitor(PoolMonitorV5 poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    public void setPartitionManager(PartitionManager partitionManager)
    {
        _partitionManager = partitionManager;
    }

    public void setThreadPool(ThreadPool threadPool)
    {
        _threadPool = threadPool;
    }

    public void setHitInfoMessages(boolean sendHitInfo)
    {
        _sendHitInfo = sendHitInfo;
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

    public void run(){
       try{
          while( ! Thread.interrupted() ){

             Thread.sleep(_stagingRetryInterval) ;

             List<PoolRequestHandler> list = null ;
             synchronized( _handlerHash ){
                list = new ArrayList<PoolRequestHandler>( _handlerHash.values() ) ;
             }
             try{
                for( PoolRequestHandler handler: list ){
                   if( handler == null )continue ;
                   handler.alive() ;
                }
             }catch(Throwable t){
                _log.warn(t.toString()) ;
                continue ;
             }
          }
       }catch(InterruptedException ie ){
          _log.warn("Container-ticker done");
       }
    }
    public void poolStatusChanged(String poolName, int poolStatus) {
        _log.info("Restore Manager : got 'poolRestarted' for " + poolName);
        try {
            List<PoolRequestHandler> list = null;
            synchronized (_handlerHash) {
                list = new ArrayList<PoolRequestHandler>(_handlerHash.values());
            }

            for (PoolRequestHandler rph : list) {

                if (rph == null)
                    continue;


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
                            rph.retry(false);
                        }
                    case PoolStatusChangedMessage.DOWN:
                        /*
                         * if pool is down, re-try all request scheduled to this
                         * pool
                         */
                        if (rph.getPoolCandidate().equals(poolName) ) {
                            _log.info("Restore Manager : retrying : " + rph);
                            rph.retry(false);
                        }
                }

            }

        } catch (Exception ee) {
            _log.warn("Problem retrying pool " + poolName + " (" + ee + ")");
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
       PoolManagerParameter def = _partitionManager.getParameterCopyOf() ;

       pw.println("Restore Controller [$Revision$]\n") ;
       pw.println( "      Retry Timeout : "+(_retryTimer/1000)+" seconds" ) ;
       pw.println( "  Thread Controller : "+_threadPool ) ;
       pw.println( "    Maximum Retries : "+_maxRetries ) ;
       pw.println( "    Pool Ping Timer : "+(_checkFilePingTimer/1000) + " seconds" ) ;
       pw.println( "           On Error : "+_onError ) ;
       pw.println( "       Warning Path : "+_warningPath ) ;
       pw.println( "          Allow p2p : "+( def._p2pAllowed ? "on" : "off" )+
                                          " oncost="+( def._p2pOnCost ? "on" : "off" )+
                                          " fortransfer="+( def._p2pForTransfer ? "on" : "off" ) );
       pw.println( "      Allow staging : "+(def._hasHsmBackend ? "on":"off") ) ;
       pw.println( "Allow stage on cost : "+(def._stageOnCost ? "on":"off") ) ;
       pw.println( "          P2p Slope : "+(float)def._slope ) ;
       pw.println( "     P2p Max Copies : "+def._maxPnfsFileCopies) ;
       pw.println( "          Cost Cuts : idle="+def._minCostCut+",p2p="+def.getCostCutString()+
                                       ",alert="+def._alertCostCut+",halt="+def._panicCostCut+
                                       ",fallback="+def._fallbackCostCut) ;
       pw.println( "      Restore Limit : "+(_maxRestore<0?"unlimited":(""+_maxRestore)));
       pw.println( "   Restore Exceeded : "+_restoreExceeded ) ;
       pw.println( "Allow same host p2p : "+getSameHostCopyMode() ) ;
       pw.println( "    Same host retry : "+getSameHostRetryMode() ) ;
       if( _suspendIncoming )
            pw.println( "   Suspend Incoming : on (not persistent)");
       if( _suspendStaging )
            pw.println( "   Suspend Staging  : on (not persistent)");
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.append("#\n# Submodule [rc] : ").append(this.getClass().toString()).append("\n#\n");
        pw.append("rc onerror ").println(_onError);
        pw.append("rc set max retries ").println(_maxRetries);
        pw.append("rc set retry ").println(_retryTimer/1000);
        pw.append("rc set warning path ").println(_warningPath);
        pw.append("rc set poolpingtimer ").println(_checkFilePingTimer/1000);
        pw.append("rc set max restore ")
            .println(_maxRestore<0?"unlimited":(""+_maxRestore));
        pw.append("rc set sameHostCopy ")
            .println(getSameHostCopyMode());
        pw.append("rc set sameHostRetry ")
            .println(getSameHostRetryMode());
        pw.append("rc set max threads ")
            .println(_threadPool.getMaxThreadCount());
    }

    private String getSameHostRetryMode(){
        return _sameHostRetry == SAME_HOST_RETRY_NEVER      ? STRING_NEVER :
               _sameHostRetry == SAME_HOST_RETRY_BESTEFFORT ? STRING_BESTEFFORT :
               _sameHostRetry == SAME_HOST_RETRY_NOTCHECKED ? STRING_NOTCHECKED :
               "UNDEFINED" ;

     }

     private String getSameHostCopyMode(){
       PoolManagerParameter def = _partitionManager.getParameterCopyOf() ;
       return def._allowSameHostCopy == PoolManagerParameter.P2P_SAME_HOST_BEST_EFFORT ? STRING_BESTEFFORT :
              def._allowSameHostCopy == PoolManagerParameter.P2P_SAME_HOST_NEVER       ? STRING_NEVER :
              STRING_NOTCHECKED ;
     }

    public String hh_rc_set_max_threads = "<threadCount> # 0 : no limits" ;
    public String ac_rc_set_max_threads_$_1( Args args ){
       int n = Integer.parseInt(args.argv(0));
       _threadPool.setMaxThreadCount(n);
       return "New max thread count : "+n;
    }

    public String hh_rc_set_sameHostCopy = STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED ;
    public String ac_rc_set_sameHostCopy_$_1( Args args ){
        String type = args.argv(0) ;
        PoolManagerParameter para = _partitionManager.getDefaultPartitionInfo().getParameter() ;
        synchronized( para ){
           if( type.equals(STRING_NEVER) ){
              para._allowSameHostCopy = PoolManagerParameter.P2P_SAME_HOST_NEVER ;
           }else if( type.equals(STRING_BESTEFFORT) ){
              para._allowSameHostCopy = PoolManagerParameter.P2P_SAME_HOST_BEST_EFFORT ;
           }else if( type.equals(STRING_NOTCHECKED) ){
              para._allowSameHostCopy = PoolManagerParameter.P2P_SAME_HOST_NOT_CHECKED ;
           }else{
              throw new
              IllegalArgumentException("Value not supported : "+type) ;
           }
        }
        return "" ;
     }

    public String hh_rc_set_sameHostRetry = STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED ;
    public String ac_rc_set_sameHostRetry_$_1( Args args ){

       String value = args.argv(0) ;
       if( value.equals(STRING_NEVER) ){
           _sameHostRetry = SAME_HOST_RETRY_NEVER ;
       }else if( value.equals( STRING_BESTEFFORT ) ){
           _sameHostRetry = SAME_HOST_RETRY_BESTEFFORT ;
       }else if( value.equals( STRING_NOTCHECKED) ){
           _sameHostRetry = SAME_HOST_RETRY_NOTCHECKED ;
       }else
          throw new
          IllegalArgumentException("Value not supported for \"set sameHostRetry\" : "+value ) ;
       return "" ;
    }

    public String fh_rc_set_max_restore = "Limit total number of concurrent restores.  If the total number of\n" +
                                          "restores reaches this limit then any additional restores will fail;\n" +
                                          "when the total number of restores drops below limit then additional\n" +
                                          "restores will be accepted.  Setting the limit to \"0\" will result in\n" +
                                          "all restores failing; setting the limit to \"unlimited\" will remove\n" +
                                          "the limit.";
    public String hh_rc_set_max_restore = "<maxNumberOfRestores>" ;
    public String ac_rc_set_max_restore_$_1( Args args ){
       if( args.argv(0).equals("unlimited") ){
          _maxRestore = -1 ;
          return "" ;
       }
       int n = Integer.parseInt(args.argv(0));
       if( n < 0 )
         throw new
         IllegalArgumentException("must be >=0") ;
       _maxRestore = n ;
       return "" ;
    }
    public String hh_rc_select = "[<pnfsId> [<errorNumber> [<errorMessage>]] [-remove]]" ;
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
          boolean remove = args.getOpt("remove") != null ;
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
    public String hh_rc_set_warning_path = " # where to send the warnings to" ;
    public String ac_rc_set_warning_path_$_0_1( Args args ){
       if( args.argc() > 0 ){
          _warningPath = args.argv(0) ;
       }
       return _warningPath ;
    }
    public String fh_rc_set_poolpingtimer =
    " rc set poolpingtimer <timer/seconds> "+
    ""+
    "    If set to a nonzero value, the restore handler will frequently"+
    "    check the pool whether the request is still pending, failed"+
    "    or has been successful" +
    "";
    public String hh_rc_set_poolpingtimer = "<checkPoolFileTimer/seconds>" ;
    public String ac_rc_set_poolpingtimer_$_1(Args args ){
       _checkFilePingTimer = 1000L * Long.parseLong(args.argv(0));
       return "" ;
    }
    public String hh_rc_set_retry = "<retryTimer/seconds>" ;
    public String ac_rc_set_retry_$_1(Args args ){
       _retryTimer = 1000L * Long.parseLong(args.argv(0));
       return "" ;
    }
    public String hh_rc_set_max_retries = "<maxNumberOfRetries>" ;
    public String ac_rc_set_max_retries_$_1(Args args ){
       _maxRetries = Integer.parseInt(args.argv(0));
       return "" ;
    }
    public String hh_rc_suspend = "[on|off] -all" ;
    public String ac_rc_suspend_$_0_1( Args args ){
       boolean all = args.getOpt("all") != null ;
       if( args.argc() == 0 ){
          if(all)_suspendIncoming = true ;
          _suspendStaging = true ;
       }else{

          String mode = args.argv(0) ;
          if( mode.equals("on") ){
              if(all)_suspendIncoming = true ;
              _suspendStaging = true ;
          }else if( mode.equals("off") ){
              if(all)_suspendIncoming = false ;
              _suspendStaging = false ;
          }else{
              throw new
              IllegalArgumentException("Usage : rc suspend [on|off]");
          }

       }
       return "" ;
    }
    public String hh_rc_onerror = "suspend|fail" ;
    public String ac_rc_onerror_$_1(Args args ){
       String onerror = args.argv(0) ;
       if( ( ! onerror.equals("suspend") ) &&
           ( ! onerror.equals("fail") )  )
             throw new
             IllegalArgumentException("Usage : rc onerror fail|suspend") ;

       _onError = onerror ;
       return "onerror "+_onError ;
    }
    public String fh_rc_retry =
       "NAME\n"+
       "           rc retry\n\n"+
       "SYNOPSIS\n"+
       "           I)  rc retry <pnfsId> [OPTIONS]\n"+
       "           II) rc retry * -force-all [OPTIONS]\n\n"+
       "DESCRIPTION\n"+
       "           Forces a 'restore request' to be retried.\n"+
       "           While  using syntax I , a single request  is retried,\n"+
       "           syntax II retries all requests which reported an error.\n"+
       "           If the '-force-all' options is given, all requests are\n"+
       "           retried, regardless of their current status.\n\n"+
       "           -update-si\n"+
       "                   fetch the storage info again before performing\n"+
       "                   the retry. \n"+
       "\n" ;
    public String hh_rc_retry = "<pnfsId>|* -force-all -update-si" ;
    public String ac_rc_retry_$_1( Args args ) throws CacheException {
       StringBuffer sb = new StringBuffer() ;
       boolean forceAll = args.getOpt("force-all") != null ;
       boolean updateSi = args.getOpt("update-si") != null ;
       if( args.argv(0).equals("*") ){
          List<PoolRequestHandler> all;
          //
          // Remember : we are not allowed to call 'retry' as long
          // as we  are holding the _handlerHash lock.
          //
          synchronized( _handlerHash ){
             all = new ArrayList<PoolRequestHandler>( _handlerHash.values() ) ;
          }
          for (PoolRequestHandler rph : all) {
             try{
                if( forceAll || ( rph._currentRc != 0 ) )rph.retry(updateSi) ;
             }catch(Exception ee){
                sb.append(ee.getMessage()).append("\n");
             }
          }
       }else{
          PoolRequestHandler rph;
          synchronized( _handlerHash ){
             rph = _handlerHash.get(args.argv(0));
             if( rph == null )
                throw new
                IllegalArgumentException("Not found : "+args.argv(0) ) ;
          }
          rph.retry(updateSi) ;
       }
       return sb.toString() ;
    }
    public String hh_rc_failed = "<pnfsId> [<errorNumber> [<errorMessage>]]" ;
    public String ac_rc_failed_$_1_3( Args args ) throws CacheException {
       int    errorNumber = args.argc() > 1 ? Integer.parseInt(args.argv(1)) : 1;
       String errorString = args.argc() > 2 ? args.argv(2) : "Operator Intervention" ;

       PoolRequestHandler rph = null ;

       synchronized( _handlerHash ){
          rph = _handlerHash.get(args.argv(0));
          if( rph == null )
             throw new
             IllegalArgumentException("Not found : "+args.argv(0) ) ;
       }
       rph.failed(errorNumber,errorString) ;
       return "" ;
    }
    public String hh_rc_destroy = "<pnfsId> # !!!  use with care" ;
    public String ac_rc_destroy_$_1( Args args ) throws CacheException {

       PoolRequestHandler rph = null ;

       synchronized( _handlerHash ){
          rph = _handlerHash.get(args.argv(0));
          if( rph == null )
             throw new
             IllegalArgumentException("Not found : "+args.argv(0) ) ;

          _handlerHash.remove( args.argv(0) ) ;
       }
       return "" ;
    }
    public String hh_rc_ls = " [<regularExpression>] [-w] # lists pending requests" ;
    public String ac_rc_ls_$_0_1( Args args ){
       StringBuilder sb  = new StringBuilder() ;

       Pattern  pattern = args.argc() > 0 ? Pattern.compile(args.argv(0)) : null ;

       if( args.getOpt("w") == null ){
          List<PoolRequestHandler>    allRequestHandlers = null ;
          synchronized( _handlerHash ){
              allRequestHandlers = new ArrayList<PoolRequestHandler>( _handlerHash.values() ) ;
          }

          for( PoolRequestHandler h : allRequestHandlers ){

              if( h == null )continue ;
              String line = h.toString() ;
              if( ( pattern == null ) || pattern.matcher(line).matches() )
                 sb.append(line).append("\n");
          }
       }else{

           Map<UOID, PoolRequestHandler>  allPendingRequestHandlers   = new HashMap<UOID, PoolRequestHandler>() ;
          synchronized(_messageHash){
              allPendingRequestHandlers.putAll( _messageHash ) ;
          }

          for (Map.Entry<UOID, PoolRequestHandler> requestHandler : allPendingRequestHandlers.entrySet()) {

                UOID uoid = requestHandler.getKey();
                PoolRequestHandler h = requestHandler.getValue();

                if (h == null)
                    continue;
                String line = uoid.toString() + " " + h.toString();
                if ((pattern == null) || pattern.matcher(line).matches())
                    sb.append(line).append("\n");

            }
        }
       return sb.toString();
    }
    public String hh_xrc_ls = " # lists pending requests (binary)" ;
    public Object ac_xrc_ls( Args args ){

       List<PoolRequestHandler> all  = null ;
       synchronized( _handlerHash ){
          all = new ArrayList<PoolRequestHandler>( _handlerHash.values() ) ;
       }

       List<RestoreHandlerInfo>          list = new ArrayList<RestoreHandlerInfo>() ;

       for( PoolRequestHandler h: all  ){
          if( h  == null )continue ;
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
        int allowedStates = request.getAllowedStates();

        String  hostName    =
               protocolInfo instanceof IpProtocolInfo ?
               ((IpProtocolInfo)protocolInfo).getHosts()[0] :
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
        PoolRequestHandler handler = null ;
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
    public String hh_replicate = " <pnfsid> <client IP>";
    public String ac_replicate_$_2(Args args) {

        String commandReply = "Replication initiated...";

        try {

            PnfsId pnfsId = new PnfsId(args.argv(0));
            PnfsGetStorageInfoMessage getStorageInfo = new PnfsGetStorageInfoMessage(
                    pnfsId);

            CellMessage request = new CellMessage(new CellPath("PnfsManager"),
                    getStorageInfo);

            request = sendAndWait(request, 30000);
            if (request == null) {
                throw new Exception(
                        "Timeout : PnfsManager request for storageInfo of "
                                + pnfsId);
            }

            getStorageInfo = (PnfsGetStorageInfoMessage) request
                    .getMessageObject();
            StorageInfo storageInfo = getStorageInfo.getStorageInfo();

            // TODO: call p2p direct
            // send message to yourself
            PoolMgrReplicateFileMsg req = new PoolMgrReplicateFileMsg(pnfsId,
                    storageInfo, new DCapProtocolInfo("DCap", 3, 0,
                            args.argv(1), 2222), storageInfo.getFileSize());

            sendMessage( new CellMessage(new CellPath("PoolManager"), req) );

        } catch (Exception ee) {
            commandReply = "P2P failed : " + ee.getMessage();
        }

        return commandReply;
    }

    private static final String [] ST_STRINGS = {

        "Init" , "Done" , "Pool2Pool" , "Staging" , "Waiting" ,
        "WaitingForStaging" , "WaitingForP2P" , "Suspended"
    } ;

    ///////////////////////////////////////////////////////////////
    //
    // the read io request handler
    //
    private class PoolRequestHandler  {

        protected PnfsId       _pnfsId;
        protected final List<CellMessage>    _messages = new ArrayList<CellMessage>() ;
        protected int          _retryCounter = -1 ;
        private final CDC _cdc = new CDC();


        private   UOID         _waitingFor    = null ;
        private   long         _waitUntil     = 0 ;

        private   String       _state         = "[<idle>]";
        private   int          _mode          = ST_INIT ;
        private   final int    _allowedStates;
        private   boolean      _stagingDenied = false;
        private   int          _currentRc     = 0 ;
        private   String       _currentRm     = "" ;

        private   PoolCostCheckable _bestPool = null ;
        private   PoolCheckable     _poolCandidateInfo    = null ;
        private   PoolCheckable     _p2pPoolCandidateInfo = null ;
        private   PoolCheckable     _p2pSourcePoolInfo    = null ;

        private   final long   _started       = System.currentTimeMillis() ;
        private   String       _name          = null ;

        private   StorageInfo  _storageInfo   = null ;
        private   ProtocolInfo _protocolInfo  = null ;

        private   boolean _enforceP2P            = false ;
        private   int     _destinationFileStatus = Pool2PoolTransferMsg.UNDETERMINED ;

        private CheckFilePingHandler  _pingHandler = new CheckFilePingHandler(_checkFilePingTimer) ;

        private PoolMonitorV5.PnfsFileLocation _pnfsFileLocation  = null ;
        private PoolManagerParameter           _parameter         = _partitionManager.getParameterCopyOf() ;

        /**
         * Indicates the next time a TTL of a request message will be
         * exceeded.
         */
        private long _nextTtlTimeout = Long.MAX_VALUE;

        private class CheckFilePingHandler {
            private long _timeInterval = 0;
            private long _timer = 0;
            private String _candidate;
            private PingState _state = PingState.STOPPED;
            private String _query;

            private CheckFilePingHandler(long timerInterval)
            {
                _timeInterval = timerInterval;
            }

            private void startP2P(String candidate)
            {
                if (_timeInterval <= 0L || candidate == null)
                    return;
                _candidate = candidate;
                _timer = _timeInterval + System.currentTimeMillis();
                _state = PingState.WAITING;
                _query = "pp ls";
            }

            private void startStage(String candidate)
            {
                if (_timeInterval <= 0L || candidate == null)
                    return;
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
                    if (_waitingFor != null)
                        _messageHash.remove(_waitingFor);
                }
            }

            private void alive()
            {
                if ((_candidate == null) || (_timer == 0L))
                    return;

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
                    new CellMessage(new CellPath(_candidate), _query);
                synchronized (_messageHash) {
                    try {
                        sendMessage(envelope);
                        _waitingFor = envelope.getUOID();
                        _messageHash.put(_waitingFor, PoolRequestHandler.this);
                    } catch (Exception e) {
                        _log.warn("Can't send pool ping to " + _candidate + " :", e);
                    }
                }
            }
        }

        public PoolRequestHandler( PnfsId pnfsId , String canonicalName, int allowedStates ){

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

           if (_pnfsFileLocation != null)return ;

           PoolMgrSelectReadPoolMsg request =
                (PoolMgrSelectReadPoolMsg)message.getMessageObject() ;

           _storageInfo  = request.getStorageInfo() ;
           _protocolInfo = request.getProtocolInfo() ;

           if( request instanceof PoolMgrReplicateFileMsg ){
              _enforceP2P            = true ;
              _destinationFileStatus = ((PoolMgrReplicateFileMsg)request).getDestinationFileStatus() ;
           }

           _pnfsFileLocation =
                _poolMonitor.getPnfsFileLocation( _pnfsId ,
                                                  _storageInfo ,
                                                  _protocolInfo, request.getLinkGroup()) ;

           //
           //
           //
           add(null) ;
        }
        public String getPoolCandidate() {
            return _poolCandidateInfo == null ? (_p2pPoolCandidateInfo == null ? POOL_UNKNOWN_STRING
                    : _p2pPoolCandidateInfo.getPoolName())
                    : _poolCandidateInfo.getPoolName();
        }

        private String getPoolCandidateState() {
            return _poolCandidateInfo != null ? _poolCandidateInfo
                    .getPoolName()
                    : _p2pPoolCandidateInfo != null ? ((_p2pSourcePoolInfo == null ? POOL_UNKNOWN_STRING
                            : _p2pSourcePoolInfo.getPoolName())
                            + "->" + _p2pPoolCandidateInfo.getPoolName())
                            : POOL_UNKNOWN_STRING;
        }
	public RestoreHandlerInfo getRestoreHandlerInfo(){
	   return new RestoreHandlerInfo(
	          _name,
		  _messages.size(),
		  _retryCounter ,
                  _started ,
		  getPoolCandidateState() ,
		  _state ,
		  _currentRc ,
		  _currentRm ) ;
	}
        @Override
        public String toString(){
           return _name+" m="+_messages.size()+" r="+
                  _retryCounter+" ["+getPoolCandidateState()+"] ["+_state+"] "+
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
        private void retry(boolean updateSi) throws CacheException {
           Object [] command = new Object[2] ;
           command[0] = "retry" ;
           command[1] = updateSi ? "update" : "" ;
           _pnfsFileLocation.clear() ;
           add( command ) ;
        }
        private void failed( int errorNumber , String errorMessage )
                throws CacheException {

           if( errorNumber > 0 ){
              Object [] command = new Object[3] ;
              command[0] = "failed" ;
              command[1] = Integer.valueOf(errorNumber) ;
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

              if( _waitingFor != null )_messageHash.remove( _waitingFor ) ;
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
	private boolean sendFetchRequest( String poolName , StorageInfo storageInfo )
            throws NoRouteToCellException
        {

	    CellMessage cellMessage = new CellMessage(
                                new CellPath( poolName ),
	                        new PoolFetchFileMessage(
                                        poolName,
                                        storageInfo,
                                        _pnfsId          )
                                );
            synchronized( _messageHash ){
                if( ( _maxRestore >=0 ) &&
                    ( _messageHash.size() >= _maxRestore ) )return false ;
                sendMessage( cellMessage );
                _poolMonitor.messageToCostModule( cellMessage ) ;
                _messageHash.put( _waitingFor = cellMessage.getUOID() , this ) ;
                _state = "Staging "+_formatter.format(new Date()) ;
            }
            return true ;
	}
	private void sendPool2PoolRequest( String sourcePool , String destPool )
            throws NoRouteToCellException
        {

            Pool2PoolTransferMsg pool2pool =
                  new Pool2PoolTransferMsg(sourcePool,destPool,_pnfsId,_storageInfo) ;
            pool2pool.setDestinationFileStatus( _destinationFileStatus ) ;
            _log.info("Sending pool2pool request : "+pool2pool);
	    CellMessage cellMessage =
                new CellMessage(
                                  new CellPath( destPool ),
                                  pool2pool
                                );

            synchronized( _messageHash ){
                sendMessage( cellMessage );
                _poolMonitor.messageToCostModule( cellMessage ) ;
                if( _waitingFor != null )_messageHash.remove( _waitingFor ) ;
                _messageHash.put( _waitingFor = cellMessage.getUOID() , this ) ;
                _state = "[P2P "+_formatter.format(new Date())+"]" ;
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
                                  + message.getSourceAddress().getCellName()
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
            if (_currentRc != 0)
                count = 100000;
            //

            Iterator<CellMessage> messages = _messages.iterator();
            for (int i = 0; (i < count) && messages.hasNext(); i++) {
                CellMessage m =  messages.next();
                PoolMgrSelectPoolMsg rpm = (PoolMgrSelectPoolMsg) m.getMessageObject();
                if (_currentRc == 0) {
                    rpm.setPoolName(_poolCandidateInfo.getPoolName());
                    rpm.setSucceeded();
                } else {
                    rpm.setFailed(_currentRc, _currentRm);
                }
                try {
                    m.revertDirection();
                    sendMessage(m);
                    _poolMonitor.messageToCostModule(m);
                } catch (Exception e) {
                    _log.warn("Exception requestSucceeded : " + e);
                    _log.warn(e.toString());
                }
                messages.remove();
            }
            return messages.hasNext();
        }
        //
        // and the heart ...
        //
        private static final int RT_DONE       = 0 ;
        private static final int RT_OK         = 1 ;
        private static final int RT_FOUND      = 2 ;
        private static final int RT_NOT_FOUND  = 3 ;
        private static final int RT_ERROR      = 4 ;
        private static final int RT_OUT_OF_RESOURCES = 5 ;
        private static final int RT_CONTINUE         = 6 ;
        private static final int RT_COST_EXCEEDED    = 7 ;
        private static final int RT_NOT_PERMITTED    = 8 ;
        private static final int RT_S_COST_EXCEEDED  = 9 ;
        private static final int RT_DELAY  = 10 ;

        private static final int ST_INIT        = 1 ;
        private static final int ST_DONE        = 2 ;
        private static final int ST_POOL_2_POOL = 4 ;
        private static final int ST_STAGE       = 8 ;
        private static final int ST_WAITING     = 16 ;
        private static final int ST_WAITING_FOR_STAGING     = 32 ;
        private static final int ST_WAITING_FOR_POOL_2_POOL = 64 ;
        private static final int ST_SUSPENDED   = 128 ;

        private static final int CONTINUE        = 0 ;
        private static final int WAIT            = 1 ;

        private LinkedList _fifo              = new LinkedList() ;
        private boolean    _stateEngineActive = false ;
        private boolean    _forceContinue     = false ;
        private boolean    _overwriteCost     = false ;

        public class RunEngine implements ExtendedRunnable {
           public void run(){
               _cdc.restore();
              try{
                 stateLoop() ;
              }finally{
                  _cdc.clear();
                 synchronized( _fifo ){
                   _stateEngineActive = false ;
                 }
              }
           }
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
               if( _stateEngineActive )return ;
               _log.info( "Starting Engine" ) ;
               _stateEngineActive = true ;
               try {
                   _threadPool.invokeLater(new RunEngine(), "Read-"+_pnfsId);
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
                 _log.info("StageEngine called in mode "+
                   modeToString(_mode)+
                     " with object "+
                        (  inputObject == null ?
                             "(NULL)":
                            (  inputObject instanceof Object [] ?
                                 ((Object[])inputObject)[0].toString() :
                                 inputObject.getClass().getName()
                            )
                        )
                    );

                 stateEngine( inputObject ) ;

                 _log.info("StageEngine left with   : " + modeToString(_mode)+
                            "  ("+ ( _forceContinue?"Continue":"Wait")+")");

              }catch(Exception ee ){
                 _log.warn("Unexpected Exception in state loop for "+_pnfsId+" : "+ee) ;
                 _log.warn(ee.toString());
              }
           }
        }

        private boolean canStage()
        {
            /* If the result is cached or the door disabled staging,
             * then we don't check the permissions.
             */
            if (_stagingDenied || (_allowedStates & ST_STAGE) == 0) {
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
                } catch (IOException e) {
                    _log.error("Failed to verify stage permissions: " + e.getMessage());
                } catch (PatternSyntaxException e) {
                    _log.error("Failed to verify stage permissions: " + e.getMessage());
                }
            }

            /* None of the requests had the necessary credentials to
             * stage. This result is cached.
             */
            _stagingDenied = true;
            return false;
        }

        private void nextStep( int mode , int shouldContinue ){
            if (_currentRc == CacheException.NOT_IN_TRASH ||
                _currentRc == CacheException.FILE_NOT_FOUND) {
                _mode = ST_DONE;
                _forceContinue = true;
                _state = "Failed";
                sendInfoMessage(_pnfsId , _storageInfo ,
                                _currentRc , "Failed "+_currentRm);
            } else {
                if (mode == ST_STAGE && !canStage()) {
                    _mode = ST_DONE;
                    _forceContinue = true;
                    _state = "Failed";
                    _log.debug("Subject is not authorized to stage");
                    _currentRc = CacheException.FILE_NOT_ONLINE;
                    _currentRm = "File not online. Staging not allowed.";
                    sendInfoMessage(_pnfsId , _storageInfo ,
                                    _currentRc , "Permission denied." + _currentRm);
                } else if ((mode & _allowedStates) == 0) {
                    _mode = ST_DONE;
                    _forceContinue = true;
                    _state = "Failed";
                    _log.debug("No permission to perform " +
                               modeToString(mode));
                    _currentRc = CacheException.PERMISSION_DENIED;
                    _currentRm = "Permission denied.";
                    sendInfoMessage(_pnfsId, _storageInfo, _currentRc,
                                    "Permission denied for " +
                                    modeToString(mode));
                } else if ((mode & _allowedStates) == mode) {
                    _mode = mode ;
                    _forceContinue = shouldContinue == CONTINUE ;
                    if( _mode != ST_DONE ){
                        _currentRc = 0 ;
                        _currentRm = "" ;
                    }
                } else {
                    _log.error("Value of (mode & _allowedStates) is neither equal to '0' nor to 'mode'." +
                               " Current value of mode = " + Integer.toString(mode));
                    throw new IllegalStateException("Illegal value of 'mode'.");
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
           int rc = -1;
           switch( _mode ){

              case ST_INIT :
                 _log.debug( "stateEngine: case ST_INIT");
                 synchronized( _selections ){

                    CacheException ce = _selections.get(_pnfsId) ;
                    if( ce != null ){
                       setError(ce.getRc(),ce.getMessage());
                       nextStep( ST_DONE , CONTINUE ) ;
                       return ;
                    }

                 }


                 if( inputObject == null ){


                    if( _suspendIncoming ){
                          _state = "Suspended (forced) "+_formatter.format(new Date()) ;
                          _currentRc = 1005 ;
                          _currentRm = "Suspend enforced";
                          _log.debug( " stateEngine: SUSPENDED/WAIT ");
                         nextStep( ST_SUSPENDED , WAIT ) ;
                         sendInfoMessage( _pnfsId , _storageInfo ,
                                          _currentRc , "Suspended (forced) "+_currentRm );
                         return ;
                    }
                    _retryCounter ++ ;
                    _pnfsFileLocation.clear() ;
                    //
                    //
                    if( _enforceP2P ){
                        setError(0,"");
                        nextStep(ST_POOL_2_POOL , CONTINUE) ;
                        return ;
                    }

                    if( ( rc = askIfAvailable() ) == RT_FOUND ){

                       setError(0,"");
                       nextStep( ST_DONE , CONTINUE ) ;
                       _log.info("AskIfAvailable found the object");
                       if (_sendHitInfo ) sendHitMsg(  _pnfsId, (_bestPool!=null)?_bestPool.getPoolName():"<UNKNOWN>", true );   //VP

                    }else if( rc == RT_NOT_FOUND ){
                       //
                       //
                        _log.debug(" stateEngine: RT_NOT_FOUND ");
                       if( _parameter._hasHsmBackend ){
                           _log.debug(" stateEngine: parameter has HSM backend ");
                          nextStep( ST_STAGE , CONTINUE ) ;
                       }else{
                          _log.debug(" stateEngine: parameter has NO HSM backend ");
                          _state = "Suspended (pool unavailable) "+_formatter.format(new Date()) ;
                          _currentRc = 1010 ;
                          _currentRm = "Suspend";
                          _poolCandidateInfo = null ;
                          nextStep( ST_SUSPENDED , WAIT ) ;
                       }
                       if (_sendHitInfo && _poolCandidateInfo == null) {
                           sendHitMsg(  _pnfsId, (_bestPool!=null)?_bestPool.getPoolName():"<UNKNOWN>", false );   //VP
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
                                ? ST_POOL_2_POOL : ST_STAGE , CONTINUE ) ;

                    }else if( rc == RT_COST_EXCEEDED ){

                       if( _parameter._p2pOnCost ){

                           nextStep( ST_POOL_2_POOL , CONTINUE ) ;

                       }else if( _parameter._hasHsmBackend &&  _parameter._stageOnCost ){

                           nextStep( ST_STAGE , CONTINUE ) ;

                       }else{

                           setError( 127 , "Cost exceeded (st,p2p not allowed)" ) ;
                           nextStep( ST_DONE , CONTINUE ) ;

                       }
                    }else if( rc == RT_ERROR ){
                       _log.debug( " stateEngine: RT_ERROR");
                       nextStep( ST_STAGE , CONTINUE ) ;
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

                       nextStep( ST_WAITING_FOR_POOL_2_POOL , WAIT ) ;
                       _state = "Pool2Pool "+_formatter.format(new Date()) ;
                       setError(0,"");
                       _pingHandler.startP2P(_p2pPoolCandidateInfo.getPoolName()) ;

                       if (_sendHitInfo ) sendHitMsg(  _pnfsId,
                               (_p2pSourcePoolInfo!=null)?
                                   _p2pSourcePoolInfo.getPoolName():
                                   "<UNKNOWN>", true );   //VP

                    }else if( rc == RT_NOT_PERMITTED ){

                        if( _bestPool == null) {
                            if( _enforceP2P ){
                               nextStep( ST_DONE , CONTINUE ) ;
                            }else if( _parameter._hasHsmBackend && _storageInfo.isStored() ){
                               _log.info("ST_POOL_2_POOL : Pool to pool not permitted, trying to stage the file");
                               nextStep( ST_STAGE , CONTINUE ) ;
                            }else{
                               setError(265,"Pool to pool not permitted");
                               nextStep( ST_SUSPENDED , WAIT ) ;
                            }
                        }else{
                            _poolCandidateInfo = _bestPool ;
                            _log.info("ST_POOL_2_POOL : Choosing high cost pool "+_poolCandidateInfo.getPoolName());

                          setError(0,"");
                          nextStep( ST_DONE , CONTINUE ) ;
                        }

                    }else if( rc == RT_S_COST_EXCEEDED ){

                       _log.info("ST_POOL_2_POOL : RT_S_COST_EXCEEDED");

                       if( _parameter._hasHsmBackend && _parameter._stageOnCost && _storageInfo.isStored() ){

                           if( _enforceP2P ){
                              nextStep( ST_DONE , CONTINUE ) ;
                           }else{
                              _log.info("ST_POOL_2_POOL : staging");
                              nextStep( ST_STAGE , CONTINUE ) ;
                           }
                       }else{

                          if( _bestPool != null ){

                              _poolCandidateInfo = _bestPool;
                              _log.info("ST_POOL_2_POOL : Choosing high cost pool "+_poolCandidateInfo.getPoolName());

                             setError(0,"");
                             nextStep( ST_DONE , CONTINUE ) ;
                          }else{
                             //
                             // this can't possibly happen
                             //
                             setError(194,"PANIC : File not present in any reasonable pool");
                             nextStep( ST_DONE , CONTINUE ) ;
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
                             nextStep( ST_DONE , CONTINUE ) ;
                          }else{
                             setError(192,"PANIC : File not present in any reasonable pool");
                             nextStep( ST_DONE , CONTINUE ) ;
                          }

                       }else{

                           _poolCandidateInfo = _bestPool;

                          _log.info(" found high cost object");

                          setError(0,"");
                          nextStep( ST_DONE , CONTINUE ) ;

                       }


                    }else{

                       if( _enforceP2P ){
                          nextStep( ST_DONE , CONTINUE ) ;
                       }else if( _parameter._hasHsmBackend && _storageInfo.isStored() ){
                          nextStep( ST_STAGE , CONTINUE ) ;
                       }else{
                          nextStep( ST_SUSPENDED , WAIT ) ;
                       }

                    }

                 }
              }
              break ;

              case ST_STAGE :
                 _log.debug( "stateEngine: case ST_STAGE");
                 if( inputObject == null ){

                    if( _suspendStaging ){
                          _state = "Suspended Stage (forced) "+_formatter.format(new Date()) ;
                          _currentRc = 1005 ;
                          _currentRm = "Suspend enforced";
                         nextStep( ST_SUSPENDED , WAIT ) ;
                         sendInfoMessage( _pnfsId , _storageInfo ,
                                          _currentRc , "Suspended Stage (forced) "+_currentRm );
                         return ;
                    }

                    if( ( rc = askForStaging() ) == RT_FOUND ){

                       nextStep( ST_WAITING_FOR_STAGING , WAIT ) ;
                       _state = "Staging "+_formatter.format(new Date()) ;
                       setError(0,"");
                       _pingHandler.startStage(_poolCandidateInfo.getPoolName()) ;

                    }else if( rc == RT_OUT_OF_RESOURCES ){

                       _restoreExceeded ++ ;
                       outOfResources("Restore") ;

                    }else{
                       //
                       // we coudn't find a pool for staging
                       //
                       errorHandler() ;
                    }

                 }

              break ;
              case ST_WAITING_FOR_POOL_2_POOL :
                 _log.debug( "stateEngine: case ST_WAITING_FOR_POOL_2_POOL");
                 if( inputObject instanceof Message ){

                    if( ( rc =  exercisePool2PoolReply((Message)inputObject) ) == RT_OK ){

                       nextStep( _parameter._p2pForTransfer && ! _enforceP2P ? ST_INIT : ST_DONE , CONTINUE ) ;

                    }else if( rc == RT_CONTINUE ){
                        //
                        //
                    }else{
                        _log.info("ST_POOL_2_POOL : Pool to pool reported a problem");
                        if( _parameter._hasHsmBackend && _storageInfo.isStored() ){

                            _log.info("ST_POOL_2_POOL : trying to stage the file");
                            nextStep( ST_STAGE , CONTINUE ) ;

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

                       nextStep( _parameter._p2pForTransfer ? ST_INIT : ST_DONE , CONTINUE ) ;

                    }else if( rc == RT_DELAY ){
                        _state = "Suspended By HSM request";
                        nextStep( ST_SUSPENDED , WAIT ) ;
                    }else if( rc == RT_CONTINUE ){

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
                           nextStep( ST_INIT , CONTINUE ) ;
                       }else{
                           _handlerHash.remove( _name ) ;
                       }
                    }
                 }

              return ;
           }
        }
        private void handleCommandObject( Object [] c ){

           String command = c[0].toString() ;
           if( command.equals("failed") ){

              clearSteering();
              setError(((Integer)c[1]).intValue(),c[2].toString());
              nextStep(ST_DONE,CONTINUE);

           }else if( command.equals("retry") ){

              _state = "Retry enforced" ;
              _retryCounter = 0 ;
              clearSteering() ;
              _pnfsFileLocation.clear() ;
              setError(0,"");
              if( ( c.length > 1 ) && c[1].toString().equals("update") )getStorageInfo();
              nextStep(ST_INIT,CONTINUE);

           }else if( command.equals("alive") ){

              long now = System.currentTimeMillis() ;

              if (now > _nextTtlTimeout) {
                  expireRequests();
              }

              if( ( _waitUntil > 0L ) && ( now > _waitUntil ) ){
                 nextStep(ST_INIT,CONTINUE);
                 clearSteering() ;
              }else{
                 _pingHandler.alive() ;
              }

           }

        }
        private void getStorageInfo(){
           try{
              PnfsGetStorageInfoMessage getStorageInfo = new PnfsGetStorageInfoMessage( _pnfsId ) ;

              CellMessage request = new CellMessage(
                                       new CellPath("PnfsManager") ,
                                       getStorageInfo ) ;

              request = sendAndWait( request , 30000 ) ;
              if( request == null )
                 throw new
                 Exception("Timeout : PnfsManager request for storageInfo of "+_pnfsId);

              getStorageInfo = (PnfsGetStorageInfoMessage)request.getMessageObject();
              switch (getStorageInfo.getReturnCode()) {
              case 0:
                  _storageInfo = getStorageInfo.getStorageInfo();
                  break;
              case CacheException.FILE_NOT_FOUND:
              case CacheException.NOT_IN_TRASH:
                  setError(getStorageInfo.getReturnCode(),
                           "File not found");
                  break;
              default:
                  _log.warn("Fetching storage info failed: " +
                            getStorageInfo.getErrorObject());
                  break;
              }
           }catch(Exception ee ){
               _log.warn("Fetching storage info failed : "+ee);
           }
        }
        private void outOfResources( String detail ){

           clearSteering();
           setError(5,"Resource temporarily unavailable : "+detail);
           nextStep( ST_DONE , CONTINUE ) ;
           _state = "Failed" ;
           sendInfoMessage( _pnfsId , _storageInfo ,
                            _currentRc , "Failed "+_currentRm );
        }
        private void errorHandler(){
           if(_retryCounter == 0 ){
              //
              // retry immediately (stager will take another pool)
              //
              _pnfsFileLocation.clear() ;
              getStorageInfo();
              nextStep( ST_INIT, CONTINUE ) ;
              //
           }else if( _retryCounter < _maxRetries ){
              //
              // now retry only after some time
              //
              _pnfsFileLocation.clear() ;
              getStorageInfo();
              waitFor( _retryTimer ) ;
              nextStep( ST_INIT , WAIT ) ;
              _state = "Waiting "+_formatter.format(new Date()) ;
              //
           }else{
              if( _onError.equals( "suspend" ) ){
                 _state = "Suspended "+_formatter.format(new Date()) ;
                 nextStep( ST_SUSPENDED , WAIT ) ;
                 sendInfoMessage( _pnfsId , _storageInfo ,
                                  _currentRc , "Suspended "+_currentRm );
              }else{
                 nextStep( ST_DONE , CONTINUE ) ;
                 _state = "Failed" ;
                 sendInfoMessage( _pnfsId , _storageInfo ,
                                  _currentRc , "Failed "+_currentRm );
              }
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
           }catch(Exception ee ){
              _currentRc = ee instanceof CacheException ? ((CacheException)ee).getRc() : 102 ;
              _currentRm = ee.getMessage();
              _log.warn("exerciseStageReply : "+ee ) ;
              _log.warn(ee.toString());
              return RT_ERROR ;
           }
        }
        private int exercisePool2PoolReply( Message messageArrived ){
           try{

              if( messageArrived instanceof Pool2PoolTransferMsg ){
                 Pool2PoolTransferMsg reply = (Pool2PoolTransferMsg)messageArrived ;
                 _log.info("Pool2PoolTransferMsg replied with : "+reply);
                 if( ( _currentRc = reply.getReturnCode() ) == 0 ){
                     _poolCandidateInfo = _p2pPoolCandidateInfo ;
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
           }catch(Exception ee ){
              _currentRc = ee instanceof CacheException ? ((CacheException)ee).getRc() : 102 ;
              _currentRm = ee.getMessage();
              _log.warn("exercisePool2PoolReply : "+ee ) ;
              _log.warn(ee.toString());
              return RT_ERROR ;
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
        private int askIfAvailable(){

           String err = null ;
           try{

              List<List<PoolCostCheckable>> avMatrix =
                  _pnfsFileLocation.getFileAvailableMatrix();
              int matrixSize = avMatrix.size() ;
              //
              // the DB matrix has no rows, which
              // means that there are no pools which are allowed
              // to serve this request.
              //
              if( ( matrixSize == 0 ) ||
                  ( _pnfsFileLocation.getAllowedPoolCount() == 0 ) ){

                  err="Configuration Error : No entries in Permission Matrix for this request" ;
                  setError(130,err) ;
                  _log.warn("askIfAvailable : "+err);
                  return RT_ERROR ;

              }
              //
              // we define the top row as the default parameter set for
              // cases where none of the pools hold the file.
              //
              List<PoolManagerParameter> paraList =
                  _pnfsFileLocation.getListOfParameter() ;
              _parameter = paraList.get(0);
              //
              // The file is not in the dCache at all.
              //
              if( _pnfsFileLocation.getAcknowledgedPnfsPools().size() == 0 ){
                  _log.info("askIfAvailable : file not in pool at all");
                  return RT_NOT_FOUND ;
              }
              //
              // The file is in the cache but not on a pool where
              // we would be allowed to read it from.
              //
              if( _pnfsFileLocation.getAvailablePoolCount()  == 0 ){
                  _log.info("askIfAvailable : file in cache but not in read-allowed pool");
                  return RT_NOT_PERMITTED ;
              }
              //
              // File is at least on one pool from which we could
              // get it. Now we have to find the pool with the
              // best performance cost.
              // Matrix is assumed to be sorted, so we
              // only have to check the leftmost entry
              // in the list (get(0)). Rows could be empty.
              //
              _bestPool       = null;
              List<PoolCostCheckable> bestAv = null;
              int  validCount = 0;
              List<PoolCostCheckable> tmpList = new ArrayList<PoolCostCheckable>();
              int  level      = 0;
              boolean allowFallbackOnPerformance = false;

              for( Iterator<List<PoolCostCheckable>> i = avMatrix.iterator() ; i.hasNext() ; level++ ){

                 List<PoolCostCheckable> av = i.next() ;

                 if( av.size() == 0 )continue ;

                 validCount++;
                 PoolCostCheckable cost = av.get(0);
                 tmpList.add(cost);

                 if( ( _bestPool == null ) ||
                     ( _bestPool.getPerformanceCost() > cost.getPerformanceCost() ) ){

                    _bestPool = cost ;
                    bestAv    = av ;

                 }
                 _parameter = paraList.get(level);
                 allowFallbackOnPerformance = _parameter._fallbackCostCut > 0.0 ;

                 if( ( ( ! allowFallbackOnPerformance ) &&
                       ( validCount == 1              )    ) ||
                     ( _bestPool.getPerformanceCost() < _parameter._fallbackCostCut ) )break ;
              }
              //
              // this can't happen because we already know that
              // there are pools which contain the files and which
              // are allowed for us.
              //
              if( _bestPool == null )return RT_NOT_FOUND ;

              double bestPoolPerformanceCost = _bestPool.getPerformanceCost() ;

              if(   (  _parameter.getCostCut()     > 0.0         ) &&
                    (  bestPoolPerformanceCost >= getCurrentCostCut( _parameter))    ){

                 if( allowFallbackOnPerformance ){
                    //
                    // if all costs are too high, the above list
                    // has been scanned up to the very end. But it
                    // could be that one of the first rows has a
                    // better cost than the last one, so we have
                    // to correct here.
                    //
                    _log.info("askIfAvailable : allowFallback , recalculation best cost");
                    _bestPool = Collections.min(
                                   tmpList ,
                                    _poolMonitor.getCostComparator(false,_parameter)
                                ) ;

                 }
                 _log.info("askIfAvailable : cost exceeded on all available pools, "+
                     "best pool would have been "+_bestPool);
                 return RT_COST_EXCEEDED ;
              }

              if( ( _parameter._panicCostCut > 0.0 ) && ( bestPoolPerformanceCost > _parameter._panicCostCut ) ){
                 _log.info("askIfAvailable : cost of best pool exceeds 'panic' level");
                 setError(125,"Cost of best pool exceeds panic level") ;
                 return RT_ERROR ;
              }
              _log.info("askIfAvailable : Found candidates : "+bestAv);
              //
              //  this part is intended to get rid of duplicates if the
              //  load is decreasing.
              //
              PoolCostCheckable cost = null ;
              NavigableMap<Integer,PoolCostCheckable> list =
                  new TreeMap<Integer,PoolCostCheckable>();

              if( _parameter._minCostCut > 0.0 ){

                 for( int i = 0 , n = bestAv.size() ; i < n ; i++ ){

                    cost = bestAv.get(i) ;

                    double costValue = cost.getPerformanceCost() ;

                    if( costValue < _parameter._minCostCut ){
                       //
                       // here we sort it arbitrary but reproducible
                       // (whatever that means)
                       //
                       String poolName = cost.getPoolName() ;
                       _log.info("askIfAvailable : "+poolName+" below "+_parameter._minCostCut+" : "+costValue);
                       list.put((_pnfsId.toString()+poolName).hashCode(), cost);
                    }
                 }

              }

              cost = list.isEmpty() ? bestAv.get(0) : list.firstEntry().getValue();

              _log.info( "askIfAvailable : candidate : "+cost ) ;


              _poolCandidateInfo = cost ;
              setError(0,"") ;

              return RT_FOUND ;

           }catch(Exception ee ){
              _log.warn(err="Exception in getFileAvailableList : "+ee ) ;
              _log.warn(ee.toString());
              setError(130,err) ;
              return RT_ERROR ;
           }finally{
              _log.info( "askIfAvailable : Took  "+(System.currentTimeMillis()-_started));
           }

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
		private int askForPoolToPool(boolean overwriteCost) {
			try {
				//
				//
				List<PoolCostCheckable> sources =
                                    _pnfsFileLocation.getCostSortedAvailable();
				//
				// Here we get the parameter set of the 'read'
				//
				PoolManagerParameter parameter = _pnfsFileLocation
						.getCurrentParameterSet();

				if (sources.size() == 0) {

					setError(132,
							"PANIC : Tried to do p2p, but source was empty");
					return RT_ERROR;

				} else if (sources.size() >= parameter._maxPnfsFileCopies) {
					//
					// already too many copies of the file
					//
					_log.info("askForPoolToPool : already too many copies : "
							+ sources.size());
					setError(133, "Not replicated : already too many copies : "
							+ sources.size());
					return RT_NOT_PERMITTED;

				} else if ((!overwriteCost)
						&& (parameter._alertCostCut > 0.0)
						&& ((sources.get(0))
								.getPerformanceCost() > parameter._alertCostCut)) {
					//
					// all source are too busy
					//
					_log.info("askForPoolToPool : all p2p source(s) are too busy (cost > "
							+ parameter._alertCostCut + ")");
					setError(134,
							"Not replicated : all p2p source(s) are too busy (cost > "
									+ parameter._alertCostCut + ")");
					return RT_S_COST_EXCEEDED;

				}
				//
				// make sure we are either below costCut or
				// 'slope' below the source pool.
				//
				double maxCost = parameter._slope > 0.01 ? (parameter._slope * sources.get(0).getPerformanceCost())
						: getCurrentCostCut( parameter);


				List<List<PoolCostCheckable>> matrix =
                                    _pnfsFileLocation.getFetchPoolMatrix(DirectionType.P2P,
						_storageInfo, _protocolInfo, _storageInfo
								.getFileSize());

				if (matrix.size() == 0) {
					setError(
							136,
							"Not replicated : No pool candidates available/configured/left for p2p or file already everywhere");
					_log.info("askForPoolToPool : No pool candidates available/configured/left for p2p or file already everywhere");
					return RT_NOT_PERMITTED;
				}
				//
				// Here we get the parameter set of the 'p2p'
				//
				parameter = _pnfsFileLocation.getCurrentParameterSet();

				List<PoolCostCheckable> destinations = null;

				for (Iterator<List<PoolCostCheckable>> it = matrix.iterator(); it.hasNext();) {
					destinations = it.next();
					if (destinations.size() > 0)
						break;
				}

//				subtract all source pools from the list of destination pools (those pools already have a copy)
				for (PoolCostCheckable dest : destinations) {
					for (PoolCheckable src : sources) {
						if (dest.getPoolName().equals( src.getPoolName()) ) {
							_log.info("removing pool "+dest.getPoolName()+" from dest pool list");
							destinations.remove(dest);
						}
					}
				}

				//
				if (destinations.size() == 0) {
					//
					// file already everywhere
					//
					_log.info("askForPoolToPool : file already everywhere");
					setError(137, "Not replicated : file already everywhere");
					return RT_NOT_PERMITTED;
				}

				if ((!overwriteCost) && (maxCost > 0.0)) {

					List<PoolCostCheckable> selected =
                                            new ArrayList<PoolCostCheckable>();
					for (PoolCostCheckable dest : destinations) {
						PoolCostCheckable cost = dest;
						if (cost.getPerformanceCost() < maxCost)
							selected.add(cost);
					}
					if (selected.size() == 0) {
						_log.info("askForPoolToPool : All destination pools exceed cost "
								+ maxCost);
						setError(137,
								"Not replicated : All destination pools exceed cost "
										+ maxCost);
						return RT_COST_EXCEEDED;
					}

					destinations = selected;
				}
				//
				// The 'performance cost' of all destination pools is below
				// maxCost,
				// and the destination list is sorted according to the
				// 'full cost'.
				//

				_pnfsFileLocation.sortByCost(destinations, true);

				//
				// loop over all source, destination combinations and find the
				// most appropriate for (source.hostname !=
				// destination.hostname)
				//
                PoolCheckable sourcePool                = null;
                PoolCheckable destinationPool           = null;
                PoolCheckable bestEffortSourcePool      = null;
                PoolCheckable bestEffortDestinationPool = null;

				Map<String, String> map = null;

				for (int s = 0, sMax = sources.size(); s < sMax; s++) {

					PoolCheckable sourceCost = sources.get(s);

					String sourceHost = ((map = sourceCost.getTagMap()) == null ? null : map.get("hostname"));

					for (int d = 0, dMax = destinations.size(); d < dMax; d++) {

						PoolCheckable destinationCost = destinations.get(d);

						if (parameter._allowSameHostCopy == PoolManagerParameter.P2P_SAME_HOST_NOT_CHECKED) {
							// we take the pair with the least cost without
							// further hostname checking
							sourcePool = sourceCost;
							destinationPool = destinationCost;
							break;
						}

						// save the pair with the least cost for later reuse
						if (bestEffortSourcePool == null) bestEffortSourcePool = sourceCost;
						if (bestEffortDestinationPool == null) bestEffortDestinationPool = destinationCost;

						_log.info("p2p same host checking : "
								+ sourceCost.getPoolName() + " "
								+ destinationCost.getPoolName());

						String destinationHost = ((map = destinationCost.getTagMap()) == null ? null : map.get("hostname"));

						if (sourceHost != null && !sourceHost.equals(destinationHost)) {
							// we take the first src/dest-pool pair not residing on the same host
							sourcePool = sourceCost;
							destinationPool = destinationCost;
							break;
						}
					}
					if (sourcePool != null && destinationPool != null)
						break;
				}

				if (sourcePool == null || destinationPool == null) {

//					ok, we could not find a pair on different hosts, what now?

					if (parameter._allowSameHostCopy == PoolManagerParameter.P2P_SAME_HOST_BEST_EFFORT) {
						_log.info("P2P : sameHostCopy=bestEffort : couldn't find a src/dest-pair on different hosts, choosing pair with the least cost");
						sourcePool = bestEffortSourcePool;
						destinationPool = bestEffortDestinationPool;

					} else if (parameter._allowSameHostCopy == PoolManagerParameter.P2P_SAME_HOST_NEVER) {
						_log.info("P2P : sameHostCopy=never : no matching pool found");
						setError(137,
								"Not replicated : sameHostCopy=never : no matching pool found");
						return RT_NOT_PERMITTED;

					} else {
						_log.info("P2P : coding error, bad state");
						setError(137,
								"Not replicated : coding error, bad state");
						return RT_NOT_PERMITTED;
					}
				}

                _log.info("P2P : source=" + sourcePool.getPoolName() + ";dest=" + destinationPool.getPoolName());

                sendPool2PoolRequest(
                      (_p2pSourcePoolInfo    = sourcePool).getPoolName(),
                      (_p2pPoolCandidateInfo = destinationPool ).getPoolName()
                                     );

				return RT_FOUND;

            } catch ( CacheException ce) {

                setError( ce.getRc() , ce.getMessage());
                _log.warn(ce.toString());

                return RT_ERROR;

            } catch (Exception ee) {

                setError( 128 , ee.getMessage());
                _log.warn(ee.toString());

                return RT_ERROR;

            } finally {
                _log.info("Selection pool 2 pool took : "+ (System.currentTimeMillis() - _started));
            }

		}

        private PoolCostCheckable askForFileStoreLocation( DirectionType mode  )
            throws CacheException, InterruptedException
        {

            //
            // matrix contains cost for original db matrix minus
            // the pools already containing the file.
            //
            List<List<PoolCostCheckable>> matrix =
                    _pnfsFileLocation.getFetchPoolMatrix (
                                mode ,
                                _storageInfo ,
                                _protocolInfo ,
                                _storageInfo.getFileSize() ) ;


            PoolManagerParameter parameter = _pnfsFileLocation.getCurrentParameterSet() ;

            if( matrix.size() == 0 )
                  throw new
                  CacheException( 149 , "No pool candidates available/configured/left for "+mode ) ;


            PoolCostCheckable cost = null ;
            if( _poolCandidateInfo == null ){
                int n = 0 ;
                for( Iterator<List<PoolCostCheckable>> i = matrix.iterator() ; i.hasNext() ; n++ ){

                    parameter = _pnfsFileLocation.getListOfParameter().get(n) ;
                    cost = i.next().get(0);
                    if( ( parameter._fallbackCostCut  == 0.0 ) ||
                        ( cost.getPerformanceCost() < parameter._fallbackCostCut ) )break ;

                 }
            }else{

                 //
                 // find a pool which is not identical to the first candidate
                 //
                //
                //    This prepares for the 'host name' comparison.
                //    (tmpMap is used to avoid calling getTagMap twice. The variable is used within the
                //     the for(for( loop for the same reason. The scope is limited to just two lines.)
                //
                Map<String, String> tmpMap = _poolCandidateInfo == null ? null : _poolCandidateInfo.getTagMap() ;
                String currentCandidateHostName = tmpMap == null ? null : (String)tmpMap.get("hostname") ;

                PoolCostCheckable rememberBest = null ;

                 for( Iterator<List<PoolCostCheckable>> i = matrix.iterator() ; i.hasNext() ; ){

                    for( Iterator<PoolCostCheckable> n = i.next().iterator() ; n.hasNext() ; ){

                       PoolCostCheckable c  = n.next() ;
                       //
                       // skip this one if we tried this last time
                       //
                       if( c.getPoolName().equals(_poolCandidateInfo.getPoolName()) &&  n.hasNext() ) {
                           _log.info("askFor "+mode+" : Second shot excluding : " + _poolCandidateInfo.getPoolName() ) ;
                           continue;
                       }

                       //
                       //  If the setting disallows 'sameHostRetry' and the hostname information
                       //  is present, we try to honor this.
                       //
                       if( ( _sameHostRetry != SAME_HOST_RETRY_NOTCHECKED ) && ( currentCandidateHostName != null )){
                           //
                           // Remember the best even if it is on the same host, in case of 'best effort'.
                           //
                           if( rememberBest == null )rememberBest = c  ;
                           //
                           // skip this if the hostname is available and identical to the first candidate.
                           //
                           String thisHostname = ( tmpMap = c.getTagMap() ) == null ? null : (String) tmpMap.get("hostname") ;
                           if( ( thisHostname != null ) && ( thisHostname.equals(currentCandidateHostName) ) )continue ;
                        }
                        //
                        // If the 'fallbackoncost' option is enabled and the cost of the smallest
                        // pool is still higher than the specified threshold, don't set the pool and
                        // step to the next level.
                        //
                        if( (  parameter._fallbackCostCut > 0.0 ) &&  ( c.getPerformanceCost() > parameter._fallbackCostCut ) ){
                            rememberBest = null ;
                            break ;
                        }
                        //
                        // now we can safely assign the best pool and break the loop.
                        //
                        cost = c ;

                       break ;
                    }
                    if( cost != null )break ;
                 }

                 //
                 // clear the pool candidate if this second shot didn't find a good pool. So we can try the first one
                 // again. If we don't, systems with a single pool for this request will never recover. (lionel bug 2132)
                 //

                 cost = ( cost == null ) && ( _sameHostRetry == SAME_HOST_RETRY_BESTEFFORT )  ? rememberBest : cost ;

           }
           _parameter = parameter ;

           if( cost == null )
              throw new
              CacheException( 150 , "No cheap candidates available for '"+mode+"'");

           return cost ;
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
        private int askForStaging(){

           try{

               _poolCandidateInfo = askForFileStoreLocation( DirectionType.CACHE ) ;

               //_poolCandidate     = _poolCandidateInfo.getPoolName() ;

               _log.info( "askForStaging : poolCandidate -> "+_poolCandidateInfo.getPoolName());

               if( ! sendFetchRequest( _poolCandidateInfo.getPoolName() , _storageInfo ) )return RT_OUT_OF_RESOURCES ;

               setError(0,"");

              return RT_FOUND ;

           }catch( CacheException ce ){

               setError( ce.getRc() , ce.getMessage() );
               _log.warn( ce.toString() );

               return RT_NOT_FOUND ;

           }catch( Exception ee ){

              setError( 128 , ee.getMessage() );
              _log.warn(ee.toString()) ;

              return RT_ERROR ;

           }finally{
              _log.info( "Selection cache took : "+(System.currentTimeMillis()-_started));
           }

       }

    }

    private void sendInfoMessage( PnfsId pnfsId ,
                                  StorageInfo storageInfo ,
                                  int rc , String infoMessage ){
      try{
        WarningPnfsFileInfoMessage info =
            new WarningPnfsFileInfoMessage(
                                    "PoolManager","PoolManager",pnfsId ,
                                    rc , infoMessage )  ;
            info.setStorageInfo( storageInfo ) ;

        sendMessage(
         new CellMessage( new CellPath(_warningPath), info )
                             ) ;

      }catch(Exception ee){
         _log.warn("Coudn't send WarningInfoMessage : "+ee ) ;
      }
    }

    private void sendHitMsg(PnfsId pnfsId, String poolName, boolean cached)
    {
        try {
            PoolHitInfoMessage msg = new PoolHitInfoMessage(poolName, pnfsId);
            msg.setFileCached(cached);
            sendMessage(new CellMessage( new CellPath(_warningPath), msg));
        } catch (Exception ee) {
            _log.warn("Couldn't report hit info for : "+pnfsId+" : "+ee);
        }
    }

    /**
     * Establish the costCut at the moment for the given set of parameters.
     * If the costCut was assigned a value from a string ending with a '%' then
     * that percentile cost is used.
     * @param parameter which set of parameters to consider.
     * @return the costCut, taking into account possible relative costCut.
     */
    private double getCurrentCostCut( PoolManagerParameter parameter) {
        return parameter.isCostCutPercentile()
            ? _poolMonitor.getPoolsPercentilePerformanceCost( parameter.getCostCut())
            : parameter.getCostCut();
    }
    //VP

    public void setStageConfigurationFile(String path)
    {
        _stagePolicyDecisionPoint = new CheckStagePermission(path);
    }
}