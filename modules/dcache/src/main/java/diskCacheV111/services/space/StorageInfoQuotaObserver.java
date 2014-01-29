// $Id: StorageInfoQuotaObserver.java,v 1.3 2006-08-12 21:07:48 patrick Exp $Cg

package diskCacheV111.services.space ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.vehicles.PoolLinkInfo;
import diskCacheV111.vehicles.PoolMgrGetPoolLinks;
import diskCacheV111.vehicles.QuotaMgrCheckQuotaMessage;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.Args;

/**
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.1, Aug 08, 2006
 *
 *  Collects space information from all pools as well as link information
 *  from the PoolManager. Merges those data which finally results in
 *  information about links, like space and storage classes for each link.
 */

public class StorageInfoQuotaObserver extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(StorageInfoQuotaObserver.class);

   private CellNucleus   _nucleus;
   private Args          _args;
   private File  _configFile;
   private final Map<String, PoolSpaceInfo> _poolHash        = new HashMap<>() ;
   private final Object        _linkMapLock     = new Object() ;
   private Map<String, LinkInfo> _linkMap;
   private String        _poolManagerName = "PoolManager" ;
   private int           _poolQuerySteps  = 10 ;
   private long          _poolQueryBreak  = 100L ;
   private boolean       _poolsUpdated    = true ;
   private boolean       _linksUpdated    = true ;
   private long          _poolQueryInterval         = 2L * 60L * 1000L ;
   private long          _poolManagerQueryInterval  = 5L * 60L * 1000L ;
   private long          _poolValidityTimeout       = 3L * _poolQueryInterval ;

   //-------------------------------------------------------------------------
   //
   //    Helper classes
   //
   /**
     *  General Class to hold spaceinfo , total, precious and sticky.
     *
     */
   private class SpaceInfo {

      private String _name;
      private long   _space;
      private long   _files;
      private long   _preciousSpace;
      private long   _preciousFiles;
      private long   _stickySpace;
      private long   _stickyFiles;
      private long   _removableSpace;
      private long   _removableFiles;
      private long   _poolTotalUsed;
      private long   _poolTotalFree;

      private SpaceInfo( String name ){
         _name = name ;
      }
      private SpaceInfo( String name , long space , long files ){
         _name  = name ;
         _space = space ;
         _files = files ;
      }
      public String toString(){
         return _name+
                "={space="+_space+";files="+_files+
                ";pSpace="+_preciousSpace+";pFiles="+_preciousFiles+
                ";sSpace="+_stickySpace+";sFiles="+_stickyFiles+
                ";rSpace="+_removableSpace+";rFiles="+_removableFiles+
                ";used="+_poolTotalUsed+";free="+_poolTotalFree+
                "}" ;
      }
      public void add( SpaceInfo sci ){
         if( sci == null ) {
             return;
         }
         _space += sci._space ;
         _files += sci._files ;
         _preciousSpace += sci._preciousSpace ;
         _preciousFiles += sci._preciousFiles ;
         _stickySpace += sci._stickySpace ;
         _stickyFiles += sci._stickyFiles ;
         _removableSpace += sci._removableSpace ;
         _removableFiles += sci._removableFiles ;
         _poolTotalUsed  += sci._poolTotalUsed ;
         _poolTotalFree  += sci._poolTotalFree ;
      }
   }
   /**
     * Helper pool class, holding total space as well as individual storage class
     * spaces. Declares itself invalid is it hasn't been updated recently.
     */
   private class PoolSpaceInfo {

      private String _name;
      private long   _time;
      private long   _poolSize;
      private long   _poolFreeSpace;
      private long   _poolRemovableSpace;
      private SpaceInfo []  _storageClassInfo;
      private SpaceInfo     _totalSpace;
      private final CellAddressCore _address;

      private PoolSpaceInfo(String name, CellAddressCore address){
         _name = name ;
         _address = address;
      }
      public String toString(){

        StringBuilder sb = new StringBuilder() ;
        sb.append(_name).append("={time=");
        if( ! isValid() ) {
            sb.append("Invalid");
        } else {
            sb.append(System.currentTimeMillis() - _time);
        }
        if( _storageClassInfo == null ){
           sb.append(";SciCount=NN") ;
        }else{
           sb.append(";SciCount=").append(_storageClassInfo.length);
           sb.append(";").append(_totalSpace.toString());
        }
        sb.append("}");
        return sb.toString();

      }
      public boolean isValid(){ return wasValidAt( System.currentTimeMillis() ) ; }
      public boolean wasValidAt( long atThatTime ){
         return ( _time > 0L ) && ( ( atThatTime - _time ) < _poolValidityTimeout ) ;
      }
   }
   /**
     * Helper for link info. Mainly storage class and total space for
     * SRM space manager.
     */
   private class LinkInfo {

       private String    _name;
       private List<?> _pools;
       private List<String> _storageClasses;
       private SpaceInfo _totalSpace;
       private long      _linkTotalSize;
       private long      _linkFreeSpace;
       private long      _linkRemovableSpace;

       private LinkInfo( String name ){
         _name = name ;
       }
   }
   ////////////////////////////////////////////////////////////////////////////////////
   //
   //  The CELL , constructor and interface
   // ---------------------------------------------
   //
   /**
     *  The actual Cell. (as usual)
     */
   public StorageInfoQuotaObserver( String name , String args )throws Exception {

      super( name ,StorageInfoQuotaObserver.class.getName(), args , false ) ;

      _args    = getArgs() ;
      _nucleus = getNucleus() ;

      try{
          String configName = _args.getOpt("config") ;
          if( ( configName != null ) && ( ! configName.equals("") ) ) {
              _configFile = new File(configName);
          }

          _log.info("Query Engine will be started a bit delayed" ) ;

          _nucleus.newThread( new DoDelayedOnStartup() , "init" ).start() ;

      }catch(Exception ee ){
          _log.warn( "<init> of WebCollector reports : "+ee.getMessage(), ee);
          start() ;
          kill() ;
          throw ee ;
      }
      start() ;
   }
   /**
     *   main message switchboard.
     */
   @Override
   public void messageArrived( CellMessage message ){

      CellPath source      = message.getSourcePath() ;
      String sourceCell    = source.getCellName() ;
      Object messageObject = message.getMessageObject() ;

      if( messageObject instanceof QuotaMgrCheckQuotaMessage ){

          queryQuotas( message , (QuotaMgrCheckQuotaMessage)messageObject ) ;

      }else if( messageObject instanceof PoolMgrGetPoolLinks ){

          queryPoolLinks( message , (PoolMgrGetPoolLinks)messageObject ) ;

      }else if( sourceCell.equals( _poolManagerName ) ){

          messageFromPoolManager( message ) ;

      }else{

          messageFromPool( sourceCell , message ) ;

      }
   }
   /**
     *  The getInfo
     */
   @Override
   public void getInfo( PrintWriter pw ){
       pw.println("   Cell Name "+getCellName());
       pw.println("  Cell Class "+this.getClass().getName());
       pw.println("         Pool Query Interval/sec : "+(_poolQueryInterval/1000L));
       pw.println("         Pool Query Break/millis : "+_poolQueryBreak);
       pw.println("                Pool Query Steps : "+_poolQuerySteps);
       pw.println("       Pool Validity Timeout/sec : "+(_poolValidityTimeout/1000L));
       pw.println(" Pool Manager Query Interval/sec : "+(_poolManagerQueryInterval/1000L));
       int pools;
       synchronized( _linkMapLock ){
           pw.println(" Number of Links : "+( _linkMap == null ? "Not yet known" : ""+_linkMap.size()));
       }
       synchronized( _poolHash ){ pools = _poolHash.size() ; }
       pw.println(" Number of pools : "+pools);

   }
   ///////////////////////////////////////////////////////////////////////////////////////////////////
   //
   //    Some runnables (start delay, pool manager and pool scheduler
   //    ------------------------------------------------------------
   //
   private class DoDelayedOnStartup implements Runnable {
       @Override
       public void run(){
          /*
           * wait for awhile before starting startup processes
           */
          _log.info("Collector will be delayed");

          try{ Thread.currentThread().sleep(10000L) ;}
          catch(Exception ee){ return ; }

          _log.info("QueryPoolManager now starting");

          _nucleus.newThread( new QueryPoolManager() , "QueryPoolManager" ).start() ;

          try{ Thread.currentThread().sleep(10000L) ;}
          catch(Exception ee){ return ; }

          _log.info("QueryPools now starting");

          _nucleus.newThread( new QueryPools() , "QueryPools" ).start() ;
       }
   }
   private class QueryPoolManager implements Runnable {
      @Override
      public void run(){
          _log.info("Query Pool Manager worker started");
          while( true ){

             queryLinks() ;
             queryPoolManager() ;
             try{ Thread.currentThread().sleep(_poolManagerQueryInterval) ;}
             catch(Exception ee){
                 _log.warn("Query Pool Manager worker interrupted");
                 break ;
             }

          }
          _log.info("Query Pool Manager worker finished");
      }
   }
   private class QueryPools implements Runnable {

      @Override
      public void run(){
          _log.info("Query Pools worker started");
          while( true ){

             queryPools() ;

             try{ Thread.currentThread().sleep(_poolQueryInterval) ;}
             catch(Exception ee){
                 _log.warn("Query Pools worker interrupted");
                 break ;
             }

          }
          _log.info("Query Pools worker finished");
      }

   }
   //////////////////////////////////////////////////////////////////////////
   //
   //     Message arrived HANDLER
   //     -----------------------------
   //
   //////////////////////////////////////////////////////////////////////////
   /**
     * Subswitchboard for messages from the PoolManager.
     *
     */
   private void messageFromPoolManager( CellMessage message ){

      Object obj = message.getMessageObject() ;

      if( obj instanceof List ){
         //
         // link infos
         //
         try{
            scanLinkInfo( (List<Object[]>)obj ) ;
         }catch(Exception ee ){
            _log.warn("Problem scanning link info : "+ee, ee ) ;
         }

      }else if( obj instanceof PoolManagerCellInfo ){

         synchronized( _poolHash ){

             for (Map.Entry<String, CellAddressCore> entry : ((PoolManagerCellInfo) obj).getPoolMap().entrySet()) {
                 PoolSpaceInfo info = _poolHash.get(entry.getKey());
                 if (info == null) {
                     info = new PoolSpaceInfo(entry.getKey(), entry.getValue());
                     _poolHash.put(entry.getKey(), info);
                 }
             }

         }

      }else if( obj instanceof NoRouteToCellException ){
         _log.warn("NoRouteToCell from PoolManager");
      }else{
         _log.warn("Unknow message arrived from PoolManager : "+obj.getClass().getName() ) ;
      }
   }
   /**
     * Sub switchboard for messages from the pools.
     *
     */
   private void messageFromPool( String poolName , CellMessage message ){

      Object obj = message.getMessageObject() ;

      if( obj instanceof Object [] ){

          try{
             scanSpaceInfo( poolName , (Object [])obj ) ;
          }catch(Exception e){
             _log.warn("Problem scanning space info : "+e, e);
          }

      }else if( obj instanceof NoRouteToCellException ){
          _log.warn("messageFromPool got NoRouteToCellException : "+obj);
      }else{
          _log.warn("messageFromPool got Unknown object : "+obj);
      }
   }
   /**
     *  Quota query handler.
     *
     */
   private void queryQuotas( CellMessage message , QuotaMgrCheckQuotaMessage quota ){

       try{

          String storageClass = quota.getStorageClass() ;
          if( storageClass == null ) {
              throw new
                      IllegalArgumentException("No storage class specified");
          }

          //
          // TODO : we don't need to call this every time. We could cache it.
          //
          Map<String, SpaceInfo> sci = createStorageInfoHash() ;

          SpaceInfo space = sci.get( storageClass );
          if( space == null ) {
              throw new
                      IllegalArgumentException("No such storage class : " + storageClass);
          }

          quota.setQuotas( 0L , 0L , space._space ) ;

       }catch(Exception e){
          quota.setFailed( 55 , e.getMessage() ) ;
       }
       message.revertDirection() ;

       try{
           sendMessage( message ) ;
       }catch(Exception ee ){
           _log.warn("Problem replying PoolMgrGetPoolLinks message");
       }

   }
   /**
     *  Query PoolLinks handler
     *
     */
   private void queryPoolLinks( CellMessage message , PoolMgrGetPoolLinks query ){

       resolveAllLinkInfos();

       try{
          List<LinkInfo> linkList;
          synchronized( _linkMapLock ){
             if( _linkMap == null ) {
                 throw new
                         Exception("Quota service not yet available (please wait)");
             }

             linkList = new ArrayList<>( _linkMap.values() ) ;
          }
          List<PoolLinkInfo> result = new ArrayList<>() ;

           for (LinkInfo info : linkList) {
               List<String> storageClasses = info._storageClasses;
               String[] storageGroups = (storageClasses == null)
                       ? new String[0]
                       : storageClasses
                       .toArray(new String[storageClasses.size()]);

               long available = info._linkFreeSpace + info._linkRemovableSpace;

               PoolLinkInfo poolInfo = new PoolLinkInfo(info._name, available, storageGroups);

               result.add(poolInfo);
           }
          query.setPoolLinkInfos(result
                  .toArray(new PoolLinkInfo[result.size()])) ;

       }catch(Exception ee ){
          _log.warn(ee.toString(), ee);
          query.setFailed( 23 , ee ) ;
       }

       message.revertDirection() ;

       try{
           sendMessage( message ) ;
       }catch(Exception ee ){
           _log.warn("Problem replying PoolMgrGetPoolLinks message");
       }

   }
   /**
     * gets the link info from the 'psux ls link' command
     * and converts it to our link structure.
     * A new hash is created to store the new incoming data.
     * The actual link info is only newly created if the
     * link has not be present in the current link list.
     * Spaces are NOT updated here.
     */
   private void scanLinkInfo( List<Object[]> linkList ){

      Map<String, LinkInfo> linkMap  = new HashMap<>() ;

       for (Object[] link: linkList) {
           String linkName = link[0].toString();

           LinkInfo linkInfo;
           synchronized (_linkMapLock) {
               linkInfo = _linkMap == null ? null : _linkMap
                       .get(linkName);
           }
           if (linkInfo == null) {
               linkInfo = new LinkInfo(linkName);
           }

           synchronized (linkInfo) {
               linkInfo._pools = Arrays.asList((Object[]) link[5]);
               linkInfo._storageClasses = Arrays.asList((String[]) link[9]);
           }
           linkMap.put(linkName, linkInfo);
       }
      synchronized( _linkMapLock ){
         _linkMap = linkMap ;
      }
      _linksUpdated = true ;
   }
   /**
     * The space info from the 'rep ls -s -sum -binary' command
     * is converted to our structure and inserted into the pool
     * infos, NOT yet into the link infos.
     */
   private void scanSpaceInfo( String poolName , Object[] result ){

      List<SpaceInfo> sciList = new ArrayList<>() ;
      SpaceInfo sciSum  = new SpaceInfo("total");
      SpaceInfo sci;

      long totalSpace     = 0L ;
      long freeSpace      = 0L ;
      long removableSpace = 0L ;

       for (Object o : result) {

           Object[] x = (Object[]) o;

           String sciName = (String) x[0];

           long[] counter = (long[]) x[1];

           if (sciName.equals("total")) {
               totalSpace = counter[0];
               freeSpace = counter[1];
               removableSpace = counter[2];
               continue;
           }

           sciList.add(sci = new SpaceInfo(sciName, counter[0], counter[1]));
           if (counter.length > 2) {
               sci._preciousSpace = counter[2];
               sci._preciousFiles = counter[3];
               sci._stickySpace = counter[4];
               sci._stickyFiles = counter[5];
               sci._removableSpace = counter[6];
               sci._removableFiles = counter[7];
           }
           sciSum.add(sci);
       }

      PoolSpaceInfo info;
      synchronized( _poolHash ){
         info = _poolHash.get(poolName);
      }
      synchronized( info ){
         info._time               = System.currentTimeMillis() ;
         info._storageClassInfo   = sciList
                 .toArray(new SpaceInfo[sciList.size()]);
         info._totalSpace         = sciSum ;
         info._poolSize           = totalSpace ;
         info._poolFreeSpace      = freeSpace ;
         info._poolRemovableSpace = removableSpace ;
      }
      _poolsUpdated = true ;
   }
   /////////////////////////////////////////////////////////////////////////////////
   //
   //   query function to the various other dCache services.
   //
   /////////////////////////////////////////////////////////////////////////////////
   /**
     * Queries the poolManager mainly for the available cells.
     *
     */
   private void queryPoolManager(){

      try{
         _log.debug("Sending xgetcellinfo to "+_poolManagerName);
         CellMessage msg = new CellMessage( new CellPath(_poolManagerName) , "xgetcellinfo" );
         sendMessage( msg );
      }catch(NoRouteToCellException cee ){
         _log.warn("NoPath to cell : "+_poolManagerName);
      }catch(Exception ee){
         _log.warn("Exception in sending query to pool "+_poolManagerName);
      }

   }
   /**
     * Sends link info query to PoolManager.
     */
   private void queryLinks(){
       try{
          String command = "psux ls link -x -resolve" ;
          _log.debug("Sending poolManager query "+command+" to "+_poolManagerName);
          CellMessage msg = new CellMessage( new CellPath(_poolManagerName) , command );
          sendMessage( msg );
       }catch(NoRouteToCellException cee ){
          _log.warn("NoPath to cell : "+_poolManagerName);
       }catch(Exception ee){
          _log.warn("Exception in sending query to pool : "+_poolManagerName);
       }
   }
   /**
     * Sends space query to all currently known pools.
     */
   private void queryPools(){

       List<PoolSpaceInfo> list;
       synchronized( _poolHash ){
           list = new ArrayList<>( _poolHash.values() ) ;
       }
       int counter = 0 ;
       for (PoolSpaceInfo info : list) {
          counter++;

          CellAddressCore address = info._address;
          //
          // if we need the structure to determine whether or not
          // to send the query, we have to synchronize :
          // synchronized( info ){
          //     space = info._space ;
          // }
          //
          try{
             _log.debug("Sending pool query 'rep ls -s binary' to {}", address);
             CellMessage msg = new CellMessage(new CellPath(address), "rep ls -s -sum -binary");
             sendMessage( msg );
          }catch(NoRouteToCellException cee ){
             _log.warn("NoPath to cell: {}", address);
          }catch(Exception ee){
             _log.warn("Exception in sending query to pool: {}", address);
          }

          if( ( _poolQuerySteps > 0 ) && (  ( counter % _poolQuerySteps ) == 0 ) ){
             _log.info("Waiting a while ("+_poolQueryBreak+") millis");
             try{
                if( _poolQueryBreak > 0L ) {
                    Thread.sleep(_poolQueryBreak);
                }
             }catch(InterruptedException ee){
                _log.warn("Query pool lock interrupted");
                break ;
             }
          }
       }
   }
   /**
     * creates a map of storage infos, with it's spaces.
     * First attempt to have quotas.
     */
   public Map<String, SpaceInfo> createStorageInfoHash(){

       List<PoolSpaceInfo> list;
       Map<String, SpaceInfo> sciMap = new HashMap<>();

       synchronized( _poolHash ){
           list = new ArrayList<>( _poolHash.values() ) ;
       }
       for (PoolSpaceInfo poolInfo : list) {
           synchronized (poolInfo) {
               SpaceInfo[] sciArray = poolInfo._storageClassInfo;
               if (sciArray == null) {
                   continue;
               }
               for (SpaceInfo sci : sciArray) {
                   SpaceInfo sumSci = sciMap.get(sci._name);
                   if (sumSci == null) {
                       sumSci = new SpaceInfo(sci._name);
                       sciMap.put(sci._name, sci);
                   }
                   sumSci.add(sci);
               }
           }
       }

       return sciMap ;
   }
   /**
     * Calculates the total space per link, and updates the link info
     * in the link map.
     */
   private LinkInfo resolveLinkInfoByName( String linkName ){

       LinkInfo info;

       synchronized( _linkMapLock ){

           if( _linkMap == null ) {
               return null;
           }
           if( ( info = _linkMap.get( linkName )) == null ) {
               return null;
           }

       }

       Map<String, PoolSpaceInfo> poolMap;
       synchronized( _poolHash ){
          poolMap = new HashMap<>( _poolHash ) ;
       }
       return resolveLinkInfoByLink( info , poolMap ) ;
   }
   //
   //
   private void resolveAllLinkInfos(){
       //
       // make a copy of link and pool hash map to avoid
       // permanent synchronization.
       //
       if( ! ( _poolsUpdated || _linksUpdated ) ) {
           return;
       }

       Map<String, PoolSpaceInfo> poolMap;
       List<LinkInfo> linkList;
       synchronized( _linkMapLock ){
          if( _linkMap == null ) {
              return;
          }
          linkList = new ArrayList<>( _linkMap.values() ) ;
       }
       synchronized( _poolHash ){
          poolMap = new HashMap<>( _poolHash ) ;
       }
       for (LinkInfo info : linkList) {
           resolveLinkInfoByLink(info, poolMap);
       }
       _poolsUpdated = _linksUpdated = false ;
   }
   /**
     *  Does the space calculations per link.
     *  Does correct syncs on LinkInfo and pool info but not on the
     *  poolMap.
     */
   private LinkInfo resolveLinkInfoByLink( LinkInfo info , Map<String, PoolSpaceInfo> poolMap ){

       List<?> poolListOfLink;
       synchronized( info ){
          poolListOfLink = info._pools == null ? new ArrayList<>() : info._pools ;
       }

       long      now = System.currentTimeMillis() ;
       SpaceInfo sum = new SpaceInfo( info._name ) ;
       long linkTotalSize = 0L ;
       long linkFreeSpace = 0L ;
       long linkRemovableSpace = 0L ;

       for (Object pool : poolListOfLink) {
           String poolName = pool.toString();
           PoolSpaceInfo poolInfo = poolMap.get(poolName);

           if (poolInfo == null) {
               continue;
           }

           synchronized (poolInfo) {
               //
               // we can't count this because the pool seems to be down.
               //
               if ((poolInfo == null) || (!poolInfo.wasValidAt(now))) {
                   continue;
               }

               sum.add(poolInfo._totalSpace);

               linkTotalSize += poolInfo._poolSize;
               linkFreeSpace += poolInfo._poolFreeSpace;
               linkRemovableSpace += poolInfo._poolRemovableSpace;
           }
       }
       synchronized( info ){
          info._totalSpace         = sum ;
          info._linkTotalSize      = linkTotalSize ;
          info._linkFreeSpace      = linkFreeSpace ;
          info._linkRemovableSpace = linkRemovableSpace ;
       }
       return info ;
   }
   // --------------------------------------------------------------------------------------------
   //
   //   THE COMMANDER
   //
   public static final String hh_show_link = "-a" ;
   public String ac_show_link_$_0( Args args )
   {
   try{
       boolean all = args.hasOption("a") ;

       StringBuilder sb = new StringBuilder() ;

       List<LinkInfo> linkList;

       synchronized( _linkMapLock ){
          if( _linkMap == null ) {
              throw new
                      Exception("Link Map Not yet received");
          }
          linkList = new ArrayList<>( _linkMap.values() ) ;
       }

       resolveAllLinkInfos();

       for (LinkInfo info : linkList) {
           synchronized (info) {
               sb.append(info._name);
               if (info._totalSpace != null) {
                   sb.append("  ").append(info._totalSpace.toString());
               }
               sb.append("\n");

               if (!all) {
                   continue;
               }
               List<?> list = info._pools;
               if (list != null) {
                   sb.append(" Pools:\n");
                   for (Object pool : list) {
                       sb.append("   ").append(pool.toString())
                               .append("\n");
                   }
               }
               list = info._storageClasses;
               if (list != null) {
                   sb.append(" StorageClasses:\n");
                   for (Object storageClass : list) {
                       sb.append("   ").append(storageClass.toString())
                               .append("\n");
                   }
               }
           }
       }
       return sb.toString() ;
    }catch(Exception eeee ){
       _log.warn(eeee.toString(), eeee);
       return "Failed";
    }
   }
   public static final String hh_show_sci = "" ;
   public String ac_show_sci_$_0( Args args ){
      try{
       StringBuilder sb = new StringBuilder() ;
       Map<String, SpaceInfo> sciHash = createStorageInfoHash() ;
          for (Object o : sciHash.values()) {

              SpaceInfo info = (SpaceInfo) o;

              sb.append(info.toString()).append("\n");
          }
       return sb.toString();
     }catch(Exception ee){
         _log.warn(ee.toString(), ee);
        return "Failed";
     }
   }
   public static final String hh_show_pool = "[<pool>]" ;
   public String ac_show_pool_$_0_1( Args args ){
    try{
      StringBuilder sb = new StringBuilder() ;
      String poolName;
      if( args.argc() == 0 ){
          List<PoolSpaceInfo> list;
          synchronized( _poolHash ){
              list = new ArrayList<>( _poolHash.values() ) ;
          }
          for (PoolSpaceInfo info : list) {
              synchronized (info) {
                  sb.append(info.toString()).append("\n");
              }

          }
          return sb.toString();
      }else{
          poolName = args.argv(0);
          PoolSpaceInfo info;
          synchronized( _poolHash ){

              info = _poolHash.get(poolName);
              if( info == null ) {
                  throw new
                          IllegalArgumentException("Pool not found : " + poolName);
              }

              String          general;
              SpaceInfo [] sci;
              synchronized( info ){
                 general = info.toString() ;
                 sci     = info._storageClassInfo ;
              }
              sb.append(general).append("\n");
              if( sci != null ) {
                  for (SpaceInfo aSci : sci) {
                      sb.append(" ").append(aSci.toString()).append("\n");
                  }
              }
              return sb.toString() ;
          }
      }
      }catch(Exception ee ){
        _log.warn(ee.toString(), ee);
         return "Failed";
      }
   }
   public static final String hh_query_links = "" ;
   public String ac_query_links_$_0(Args args ){
       queryLinks() ;
       return "" ;
   }
   public static final String hh_query_poolmanager = "" ;
   public String ac_query_poolmanager_$_0(Args args ){
       queryPoolManager() ;
       return "" ;
   }
   public static final String hh_query_pools = "" ;
   public String ac_query_pools_$_0(Args args ){
      new Thread( new Runnable(){
          @Override
          public void run(){
             queryPools() ;
          }
      } ).start() ;
      return "" ;
   }
   public static final String hh_set_poolmanager_query_interval = "<PoolQueryInterval/seconds> # must be > 0 ";
   public String ac_set_poolmanager_query_interval_$_1( Args args ){
        long n = Long.parseLong(args.argv(0));
        if( n <= 0 ) {
            throw new
                    IllegalArgumentException("Pool Query Interval must be > 0");
        }
        _poolManagerQueryInterval = n * 1000L ;
        return "" ;
   }
   public static final String hh_set_pool_validity_timeout = "<PoolValidityTimeout/seconds> # must be > 0 ";
   public String ac_set_pool_validity_timeout_$_1( Args args ){
        long n = Long.parseLong(args.argv(0));
        if( n <= 0 ) {
            throw new
                    IllegalArgumentException("Pool Query Interval must be > 0");
        }
        _poolValidityTimeout = n * 1000L ;
        return "" ;
   }
   public static final String hh_set_pool_query_interval = "<PoolQueryInterval/seconds> # must be > 0 ";
   public String ac_set_pool_query_interval_$_1( Args args ){
        long n = Long.parseLong(args.argv(0));
        if( n <= 0 ) {
            throw new
                    IllegalArgumentException("Pool Query Interval must be > 0");
        }
        _poolQueryInterval = n * 1000L ;
        return "" ;
   }
   public static final String hh_set_pool_query_break = "<PoolQueryBreak/millis> # no break if <= 0 ";
   public String ac_set_pool_query_break_$_1( Args args ){
        _poolQueryBreak = Long.parseLong(args.argv(0));
        return "" ;
   }
   public static final String hh_set_pool_query_steps = "<PoolQuerySteps> # no steps if <= 0";
   public String ac_set_pool_query_steps_$_1( Args args ){
        _poolQuerySteps = Integer.parseInt(args.argv(0));
        return "" ;
   }
}
