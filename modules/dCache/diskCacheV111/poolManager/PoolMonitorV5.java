// $Id: PoolMonitorV5.java,v 1.25 2006-05-29 08:38:57 patrick Exp $ 

package diskCacheV111.poolManager ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.text.*;
import  java.util.*;
import  java.io.*;

public class PoolMonitorV5 {

   private long              _poolTimeout   = 15 * 1000;
   private CellAdapter       _cell          = null ;
   private PoolSelectionUnit _selectionUnit = null ;
   private PnfsHandler       _pnfsHandler   = null ;
   private CostModule        _costModule    = null ;
   private double            _maxWriteCost          = 1000000.0;
   private boolean           _verbose               = true ;
   private PartitionManager  _partitionManager      = null ;
   
   public PoolMonitorV5( CellAdapter cell , 
                         PoolSelectionUnit selectionUnit ,
                         PnfsHandler  pnfsHandler ,
                         CostModule   costModule ,
                         PartitionManager partitionManager ){
                         
      _cell             = cell ;
      _selectionUnit    = selectionUnit ;
      _pnfsHandler      = pnfsHandler ;
      _costModule       = costModule ;
      _partitionManager = partitionManager ;      
      
   }
   public void messageToCostModule( CellMessage cellMessage ){
      _costModule.messageArrived(cellMessage);
   }
   public void setPoolTimeout( long poolTimeout ){
      _poolTimeout = poolTimeout ;
   }
   /*
   public void setSpaceCost( double spaceCost ){
      _spaceCostFactor = spaceCost ;
   }
   public void setPerformanceCost( double performanceCost ){
      _performanceCostFactor = performanceCost ;
   }*/
   public long getPoolTimeout(){ return _poolTimeout ;}
    // output[0] -> Allowed and Available
    // output[1] -> available but not allowed (sorted, cpu)
    // output[2] -> allowed but not available (sorted, cpu + space)
    // output[3] -> pools from pnfs
    // output[4] -> List of List (all allowed pools)
   public PnfsFileLocation getPnfsFileLocation( 
                               PnfsId pnfsId , 
                               StorageInfo storageInfo , 
                               ProtocolInfo protocolInfo ){
                               
      return new PnfsFileLocation( pnfsId, storageInfo ,protocolInfo ) ;
      
   } 
   public class PnfsFileLocation {
   
      private List _allowedButNotAvailable    = null ;
      private List _listOfListFromDb          = null ;
      private List _listOfPartitions          = null ;
      private List _allowedAndAvailableMatrix = null ;  
      private List _acknowledgedPnfsPools     = null ;
      private int  _allowedPoolCount          = 0 ;
      private int  _availablePoolCount        = 0 ;     
      private boolean  _calculationDone       = false ;
      
      private PnfsId       _pnfsId       = null ;
      private StorageInfo  _storageInfo  = null ;
      private ProtocolInfo _protocolInfo = null ;
      
      //private PoolManagerParameter _recentParameter = _partitionManager.getParameterCopyOf()  ;
      
      private PnfsFileLocation( PnfsId pnfsId , 
                                StorageInfo storageInfo , 
                                ProtocolInfo protocolInfo ){
      
         _pnfsId       = pnfsId ;
         _storageInfo  = storageInfo ;
         _protocolInfo = protocolInfo ;
      }
      public List getListOfParameter(){ return _listOfPartitions ; }
      public void clear(){ 
          _allowedAndAvailableMatrix = null ;
          _calculationDone           = false ;
      }
      public PoolManagerParameter getCurrentParameterSet(){ return (PoolManagerParameter)_listOfPartitions.get(0) ;}
      public List getAllowedButNotAvailable(){ return _allowedButNotAvailable ; }
      public List getAcknowledgedPnfsPools() throws Exception { 
          if( _acknowledgedPnfsPools == null )calculateFileAvailableMatrix() ;
          return _acknowledgedPnfsPools ; 
      }
      public int  getAllowedPoolCount(){ return _allowedPoolCount ; }
      public int  getAvailablePoolCount(){ return _availablePoolCount ; }
      public List getFileAvailableMatrix()
             throws Exception {

         if( _allowedAndAvailableMatrix == null )calculateFileAvailableMatrix();
         
         return _allowedAndAvailableMatrix;
      }
      //
      //   getFileAvailableList
      //  -------------------------
      //
      //  expected  = getPoolsFromPnfs() ;
      //  allowed[] = getAllowedFromConfiguration() ;
      //
      //   +----------------------------------------------------+
      //   |  for i in  0,1,2,3,...                             |
      //   |     result = intersection( expected , allowed[i] ) |
      //   |     found  = CheckFileInPool( result)              |
      //   |     if( found > 0 )break                           |
      //   |     if( ! allowFallbackOnCost )break               |
      //   |     if( minCost( found ) < MAX_COST )break         |
      //   +----------------------------------------------------+
      //   |                  found == 0                        |
      //   |                      |                             |
      //   |        yes           |             NO              |
      //   |----------------------|-----------------------------|
      //   | output[0] = empty    | [0] = SortCpuCost(found)    |
      //   | output[1] = null     | [1] = null                  |
      //   | output[2] = null     | [2] = null                  |
      //   | output[3] = expected | [3] = expected              |
      //   | output[4] = allowed  | [4] = allowed               |
      //   +----------------------------------------------------+
      //
      //   preparePool2Pool
      //  -------------------------
      //
      //  output[1] = SortCpuCost( CheckFileInPool( expected ) )
      // 
      //   +----------------------------------------------------+
      //   |                   output[0] > 0                    |
      //   |                                                    |
      //   |        yes              |             NO           |
      //   |-------------------------|--------------------------|
      //   | veto = Hash( output[0] )|                          |
      //   |-------------------------|                          |
      //   |for i in  0,1,2,3,.      | for i in  0,1,2,3,.      |
      //   |  tmp = allowed[i]-veto  |   if(allowed[i]==0)cont  |
      //   |  if( tmp == 0 )continue |                          |
      //   |  out[2] =               |   out[2] =               |       
      //   |   SortCost(getCost(tmp))|     SortCost(getCost(    |
      //   |                         |        allowed[i]))      |
      //   |   break                 |   break                  |
      //   +----------------------------------------------------+
      //   |if(out[2] == 0)          |if(out[2] == 0)           |
      //   |    out[2] = out[0]      |    out[2] = empty        |
      //   +----------------------------------------------------+
      //
       /*
        *   Input : storage info , pnfsid
        *   Output :  
        *             _acknowledgedPnfsPools
        *             _listOfListFromDb
        *             _allowedAndAvailableMatrix
        *             _allowedAndAvailable
        */
       private void calculateFileAvailableMatrix()
             throws Exception {
             
         if( _storageInfo == null )
            throw new
            CacheException(189,"Storage Info not available");

         String hsm          = _storageInfo.getHsm() ;
         String storageClass = _storageInfo.getStorageClass() ;
         String cacheClass   = _storageInfo.getCacheClass() ;
         String hostName     = 
                    ( _protocolInfo == null                   ) ||
                    ( _protocolInfo instanceof IpProtocolInfo ) ?
                    ((IpProtocolInfo)_protocolInfo).getHosts()[0] :
                    null ;
         String protocolString = _protocolInfo.getProtocol() + "/" + _protocolInfo.getMajorVersion() ;
         //
         // will ask the PnfsManager for a hint
         // about the pool locations of this
         // pnfsId. Returns an enumeration of
         // the possible pools.
         //
         List expectedFromPnfs = _pnfsHandler.getCacheLocations( _pnfsId ) ;
         say( "calculateFileAvailableMatrix _expectedFromPnfs : "+expectedFromPnfs ) ;
         //
         // check if pools are up and file is really there.
         // (returns unsorted list of costs)
         //
         _acknowledgedPnfsPools = queryPoolsForPnfsId( expectedFromPnfs.iterator() , _pnfsId , 0, _protocolInfo.isFileCheckRequired() ) ;
         say( "calculateFileAvailableMatrix _acknowledgedPnfsPools : "+_acknowledgedPnfsPools ) ;
         HashMap availableHash = new HashMap() ;
         for( Iterator i = _acknowledgedPnfsPools.iterator() ; i.hasNext() ; ){
            PoolCheckable cost = (PoolCheckable)i.next() ;
            availableHash.put( cost.getPoolName() , cost ) ;
         }
         //
         //  get the prioratized list of allowed pools for this
         //  request. (We are only allowed to use the level-1
         //  pools.
         //       
         PoolPreferenceLevel [] level = 
             _selectionUnit.match( "read" , 
                                   storageClass+"@"+hsm ,
                                   cacheClass ,
                                   hostName ,
                                   protocolString ,
                                   _storageInfo.getMap()  ) ;

         _listOfListFromDb          = new ArrayList() ;
         _listOfPartitions          = new ArrayList() ;
         _allowedAndAvailableMatrix = new ArrayList() ;
         _allowedPoolCount          = 0 ;
         _availablePoolCount        = 0 ;

         for( int prio = 0 ; prio < level.length ; prio++ ){ 
 
            List poolList = level[prio].getPoolList() ;
            //
            // makes list of list out of array of lists (need it later)
            //
            _listOfListFromDb.add( poolList ) ;
            //
            // 
            PoolManagerParameter parameter = _partitionManager.getParameterCopyOf(level[prio].getTag()) ;
            _listOfPartitions.add(  parameter ) ;
            //
            // get the allowed pools for this level and 
            // and add them to the result list only if
            // they are really available.
            //
            say( "calculateFileAvailableMatrix : db matrix[*,"+prio+"] "+poolList);
            
            List     result   = new ArrayList(poolList.size() ) ;                 
            String   poolName = null ;
            Object   cost     = null ;
            
            for( Iterator ep = poolList.iterator() ; ep.hasNext() ; ){
               poolName = (String)ep.next() ;
               if( ( cost = availableHash.get( poolName ) ) != null ){
                   result.add( cost ) ;
                   _availablePoolCount ++ ;
               }
               _allowedPoolCount ++ ;
            }
           
            if( result.size() > 0 )sortByCost( result , false , parameter ) ;

            say( "calculateFileAvailableMatrix : av matrix[*,"+prio+"] "+result);
            
            _allowedAndAvailableMatrix.add( result ) ;
                        
         }
         //
         // just in case, let us define a default parameter set
         //
         if( _listOfPartitions.size() == 0 )_listOfPartitions.add( _partitionManager.getParameterCopyOf() ) ;
         //
         _calculationDone = true ;                      
         return  ;
      }
      
      public void sortByCost( List list , boolean cpuAndSize ){
         sortByCost( list , cpuAndSize , getCurrentParameterSet() ) ;
      }
      private void sortByCost( List list , boolean cpuAndSize , PoolManagerParameter parameter ){
         if( ( parameter._performanceCostFactor == 0.0 ) && 
             ( parameter._spaceCostFactor == 0.0       )     ){
         
            Collections.shuffle( list ) ;
         }else{
            Collections.sort( list , new CostComparator( cpuAndSize , parameter ) ) ;
         }
      }
      public List getCostSortedAvailable() throws Exception {
         //
         // here we don't now exactly which parameter set to use.
         //
         if( ! _calculationDone )calculateFileAvailableMatrix();
         List list = new ArrayList( getAcknowledgedPnfsPools() ) ;
         sortByCost( list , false ) ;
         return list ; 
               
      }
      public List getStagePoolMatrix(  StorageInfo  storageInfo ,
                                       ProtocolInfo protocolInfo ,
                                       long         filesize  )
              throws Exception {
              
       
             return getFetchPoolMatrix( 
                           "cache" , 
                           storageInfo , 
                           protocolInfo ,
                           storageInfo.getMap() ,
                           filesize  ) ;       
      }
              
      public List getFetchPoolMatrix(  String       mode ,        /* cache, p2p */
                                       StorageInfo  storageInfo ,
                                       ProtocolInfo protocolInfo ,
                                       Map          constrains ,
                                       long         filesize  )
              throws Exception {

         String hsm          = storageInfo.getHsm() ;
         String storageClass = storageInfo.getStorageClass() ;
         String cacheClass   = storageInfo.getCacheClass() ;
         String hostName     = 
                    protocolInfo instanceof IpProtocolInfo ?
                    ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                    null ;
                    

         PoolPreferenceLevel [] level = 
             _selectionUnit.match( mode , 
                                   storageClass+"@"+hsm ,
                                   cacheClass ,
                                   hostName ,
                                   protocolInfo.getProtocol()+"/"+protocolInfo.getMajorVersion() ,
                                   constrains ) ;    
         //
         //
         if( level.length == 0 )return new ArrayList() ;

         //
         // Copy the matrix into a linear HashMap(keys). 
         // Exclude pools which contain the file.
         //
         List    acknowledged = getAcknowledgedPnfsPools() ;
         HashMap poolMap      = new HashMap() ;
         for( int prio = 0 ; prio < level.length ; prio++ ){
                     
            List poolList = level[prio].getPoolList() ;
           
            for( Iterator it = poolList.iterator() ; it.hasNext() ; ){
               
               String poolName = it.next().toString() ;
               //
               // skip if pool already contains the file.
               //
               if( acknowledged.contains( poolName ) )continue ;
               
               poolMap.put( poolName , null ) ;

            }
            
         }  
         //
         // Add the costs to the pool list.
         //
         for( 
               Iterator it = queryPoolsForCost( poolMap.keySet().iterator() , filesize ).iterator() ;
               it.hasNext() ;
             ){

             PoolCheckable cost = (PoolCheckable)it.next() ;
             poolMap.put( cost.getPoolName() , cost ) ;
             
         }
         //
         // Build a new matrix containing the Costs.
         //
         _listOfPartitions    = new ArrayList() ;
         List costMatrix = new ArrayList() ;  
         for( int prio = 0 ; prio < level.length ; prio++ ){ 
             //
             // skip empty level
             //
             PoolManagerParameter parameter = _partitionManager.getParameterCopyOf(level[prio].getTag()) ;
            _listOfPartitions.add( parameter ) ;

             List poolList = level[prio].getPoolList() ;
             if( poolList.size() == 0 )continue ;
                          
             List row = new ArrayList() ;
             for( Iterator ii = poolList.iterator() ; ii.hasNext() ; ){
             
                PoolCheckable cost = (PoolCheckable)poolMap.get(ii.next().toString());
                if( cost != null )row.add(cost);
                
             }
             // 
             // skip if non of the pools is available
             //
             if( row.size() == 0 )continue ;
             //
             // sort according to (cpu & space) cost
             //
             sortByCost( row , true , parameter ) ;
             //
             // and add it to the matrix
             //
             costMatrix.add( row ) ;              

         }  

         return costMatrix ;              
      }
      private void say(String message ){
         if( _verbose )_cell.say("PFL ["+_pnfsId+"] : "+message) ;
      }
      public List getStorePoolList( long filesize ) throws Exception {
         return getStorePoolList( _storageInfo , _protocolInfo , filesize ) ;
      }
      private List getStorePoolList(  StorageInfo  storageInfo ,
                                     ProtocolInfo protocolInfo ,
                                     long         filesize )
              throws Exception {

         String hsm          = storageInfo.getHsm() ;
         String storageClass = storageInfo.getStorageClass() ;
         String cacheClass   = storageInfo.getCacheClass() ;
         String  hostName    = 
                    protocolInfo instanceof IpProtocolInfo ?
                    ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                    null ;
         int  maxDepth      = 9999 ;
         PoolPreferenceLevel [] level = 
             _selectionUnit.match( "write" , 
                                   storageClass+"@"+hsm ,
                                   cacheClass ,
                                   hostName ,
                                   protocolInfo.getProtocol()+"/"+protocolInfo.getMajorVersion() ,
                                   storageInfo.getMap() ) ;
         //
         // this is the final knock out.
         //
         if( level.length == 0 )
            throw new
            CacheException( 19 ,
                             "No write pools configured for <"+
                             storageClass+"@"+hsm+">" ) ;

         List costs = new ArrayList() ;  

         PoolManagerParameter parameter = null ;

         for( int prio = 0 ; prio < Math.min( maxDepth , level.length ) ; prio++ ){ 

            costs     = queryPoolsForCost( level[prio].getPoolList().iterator() , filesize ) ;

            parameter = _partitionManager.getParameterCopyOf(level[prio].getTag()) ;

            if( costs.size() != 0 )break ;
         }  

         if( costs.size() == 0 )
            throw new
            CacheException( 20 ,
                            "No write pool available for <"+
                            storageClass+"@"+hsm+">" ) ;

         sortByCost( costs , true , parameter ) ;                 

         PoolCostCheckable check = (PoolCostCheckable)costs.get(0) ;
         
         double lowestCost = calculateCost( check , true , parameter ) ;  

         if( lowestCost  > _maxWriteCost )           
             throw new 
             CacheException( 21 , "Best pool <"+check.getPoolName()+
                                  "> too high : "+lowestCost ) ;

         return costs ;
      }
   }
   public Comparator getCostComparator( boolean both, PoolManagerParameter parameter ){
       return new CostComparator( both , parameter );
   }
   public class CostComparator implements Comparator {
   
       private boolean              _useBoth = true ;
       private PoolManagerParameter _para    = null ;
       private CostComparator( boolean useBoth , PoolManagerParameter para ){
         _useBoth = useBoth ;
         _para    = para ;
       }
       public int compare( Object o1 , Object o2 ){
          PoolCostCheckable check1 = (PoolCostCheckable)o1 ;
          PoolCostCheckable check2 = (PoolCostCheckable)o2 ;
          Double d1 = new Double( calculateCost( check1 , _useBoth , _para ) ) ;
          Double d2 = new Double( calculateCost( check2 , _useBoth , _para ) ) ;
          int c = d1.compareTo( d2 ) ;
          if( c != 0 )return c ;
          return check1.getPoolName().compareTo( check2.getPoolName() ) ;
       }
    }
    public double calculateCost( PoolCostCheckable checkable , boolean useBoth , PoolManagerParameter para ){
       if( useBoth ){
          return Math.abs(checkable.getSpaceCost())       * para._spaceCostFactor + 
                 Math.abs(checkable.getPerformanceCost()) * para._performanceCostFactor ;
       }else{
          return Math.abs(checkable.getPerformanceCost()) * para._performanceCostFactor ;
       }
    }
    /*
    public double getMinPerformanceCost( List list ){
       double cost = 1000000.0 ;
       for( int i = 0 ; i < list.size() ; i++ ){
          double x = ((PoolCostCheckable)(list.get(i))).getPerformanceCost() ;
          cost = Math.min( cost , x ) ;
       }
       return cost ;
    }
    */
    //------------------------------------------------------------------------------
    //
    //  'queryPoolsForPnfsId' sends PoolCheckFileMessages to all pools
    //  specified in the pool iterator. It waits until all replies
    //  have arrived, the global timeout has expired or the thread
    //  was interrupted.
    //
    
    private List queryPoolsForPnfsId(Iterator pools, PnfsId pnfsId,
            long filesize, boolean checkFileExistence) throws Exception {

        ArrayList list = new ArrayList();

        if (checkFileExistence) {

            SpreadAndWait control = new SpreadAndWait(_cell.getNucleus(),
                    _poolTimeout);

            while (pools.hasNext()) {

                String poolName = (String) pools.next();
                //
                // deselection inactive and disabled pools
                //
                PoolSelectionUnit.SelectionPool pool = _selectionUnit
                        .getPool(poolName);

                if ((pool == null) || (!pool.isEnabled())
                        || (pool.getActive() > (5 * 60 * 1000)))
                    continue;

                _cell.say("queryPoolsForPnfsId : PoolCheckFileRequest to : "
                        + poolName);
                //
                // send query
                //
                CellMessage cellMessage = new CellMessage(
                        new CellPath(poolName), new PoolCheckFileMessage(
                                poolName, pnfsId));

                try {
                    control.send(cellMessage);
                } catch (Exception exc) {
                    // 
                    // here we don't care about exceptions
                    //
                    _cell.esay("Exception sending PoolCheckFileRequest to "
                            + poolName + " : " + exc);
                }
            }

            //
            // scan the replies
            //
            CellMessage answer = null;

            while ((answer = control.next()) != null) {

                Object message = answer.getMessageObject();

                if (!(message instanceof PoolFileCheckable)) {
                    _cell
                            .esay("queryPoolsForPnfsId : Unexpected message from ("
                                    + answer.getSourcePath()
                                    + ") "
                                    + message.getClass());
                    continue;
                }

                PoolFileCheckable poolMessage = (PoolFileCheckable) message;
                _cell.say("queryPoolsForPnfsId : reply : " + poolMessage);

                boolean have = poolMessage.getHave();
                boolean waiting = poolMessage.getWaiting();
                String poolName = poolMessage.getPoolName();
                if (have || waiting) {

                    PoolCheckAdapter check = new PoolCheckAdapter(_costModule
                            .getPoolCost(poolName, filesize));
                    check.setHave(have);
                    check.setPnfsId(pnfsId);

                    list.add(check);
                    _cell.say("queryPoolsForPnfsId : returning : " + check);
                } else {
                    _cell
                            .esay("queryPoolsForPnfsId : clearingCacheLocation for pnfsId "
                                    + pnfsId + " at pool " + poolName);
                    _pnfsHandler.clearCacheLocation(pnfsId, poolName);
                }
            }

        } else {

            while ( pools.hasNext() ) {
                
                String poolName = (String) pools.next();
                PoolCheckAdapter check = new PoolCheckAdapter(_costModule
                        .getPoolCost(poolName, filesize));
                check.setHave(true);
                check.setPnfsId(pnfsId);

                list.add(check);            
            }
            
        }

        _cell.say("queryPoolsForPnfsId : number of valid replies : "
                + list.size());
        return list;

    }
    private boolean _dontAskForCost = true ;
    private List queryPoolsForCost( Iterator pools , long filesize ) throws Exception {
    
        ArrayList     list    = new ArrayList() ;
        SpreadAndWait control = 
              new SpreadAndWait( _cell.getNucleus() , _poolTimeout ) ;

	while( pools.hasNext() ){

	    String poolName = (String)pools.next();
            PoolCostCheckable costCheck = _costModule.getPoolCost( poolName , filesize ) ;
            if( costCheck != null ){
               list.add( costCheck ) ;
               _cell.say( "queryPoolsForCost : costModule : "+poolName+" ("+filesize+") "+costCheck);
            }else{
               //
               // send query
               //
               if( _dontAskForCost )continue ;
	       CellMessage  cellMessage = 
                      new CellMessage(  new CellPath(poolName), 
                                        new PoolCheckCostMessage(poolName,filesize) 
                                     );

               _cell.say( "queryPoolsForCost : "+poolName+" query sent");
	       try{
                  control.send( cellMessage ) ;
	       }catch(Exception exc){
                  // 
                  // here we don't care about exceptions
                  //
	          _cell.esay ("queryPoolsForCost : Exception sending PoolCheckFileRequest to "+poolName+" : "+exc);
	       }
            }
    
        }
        
        if( _dontAskForCost )return list ;

        //
        // scan the replies
        //
        CellMessage answer = null ; 

        while( ( answer = control.next() ) != null ){

           Object message = answer.getMessageObject();

	   if( ! ( message instanceof PoolCostCheckable )){
	      _cell.esay("queryPoolsForCost : Unexpected message from ("+
                   answer.getSourcePath()+") "+message.getClass());
              continue ;
	   } 
	   PoolCostCheckable poolMessage = (PoolCostCheckable)message;
           _cell.say( "queryPoolsForCost : reply : "+poolMessage ) ;
	   String  poolName = poolMessage.getPoolName();
           list.add( poolMessage ) ;
        }
        _cell.say( "queryPoolsForCost : number of valid replies : "+list.size() );
        return list ;
    }

}
