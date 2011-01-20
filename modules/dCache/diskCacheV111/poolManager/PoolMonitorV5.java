//  $Id: PoolMonitorV5.java,v 1.32 2007-08-01 20:00:45 tigran Exp $

package diskCacheV111.poolManager ;

import com.google.common.base.Function;
import static com.google.common.collect.Collections2.*;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.cells.AbstractCellComponent;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SpreadAndWait;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolCheckFileMessage;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

public class PoolMonitorV5
    extends AbstractCellComponent
{
    private final static Logger _log = LoggerFactory.getLogger(PoolMonitorV5.class);

   private long              _poolTimeout   = 15 * 1000;
   private PoolSelectionUnit _selectionUnit ;
   private PnfsHandler       _pnfsHandler   ;
   private CostModule        _costModule    ;
   private double            _maxWriteCost          = 1000000.0;
   private PartitionManager  _partitionManager ;

    public PoolMonitorV5()
    {
    }

    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler)
    {
        _pnfsHandler = pnfsHandler;
    }

    public void setCostModule(CostModule costModule)
    {
        _costModule = costModule;
    }

    public void setPartitionManager(PartitionManager partitionManager)
    {
        _partitionManager = partitionManager;
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
                               ProtocolInfo protocolInfo, String linkGroup){

      return new PnfsFileLocation( pnfsId, storageInfo ,protocolInfo , linkGroup) ;

   }
   public class PnfsFileLocation {

      private List<PoolManagerParameter> _listOfPartitions;
      private List<List<PoolCostCheckable>> _allowedAndAvailableMatrix;
      private List<PoolCostCheckable> _acknowledgedPnfsPools;
      private int  _allowedPoolCount          = 0 ;
      private int  _availablePoolCount        = 0 ;
      private boolean  _calculationDone       = false ;

      private final PnfsId       _pnfsId       ;
      private final StorageInfo  _storageInfo  ;
      private final ProtocolInfo _protocolInfo ;
      private final String _linkGroup          ;

      //private PoolManagerParameter _recentParameter = _partitionManager.getParameterCopyOf()  ;

      private PnfsFileLocation( PnfsId pnfsId ,
                                StorageInfo storageInfo ,
                                ProtocolInfo protocolInfo ,
                                String linkGroup){

         _pnfsId       = pnfsId ;
         _storageInfo  = storageInfo ;
         _protocolInfo = protocolInfo ;
         _linkGroup    = linkGroup;
      }

       public List<PoolManagerParameter> getListOfParameter()
       {
           return _listOfPartitions;
       }

      public void clear(){
          _allowedAndAvailableMatrix = null ;
          _calculationDone           = false ;
      }

       public PoolManagerParameter getCurrentParameterSet()
       {
           return _listOfPartitions.get(0);
       }

       public List<PoolCostCheckable> getAcknowledgedPnfsPools()
           throws CacheException, InterruptedException
       {
           if (_acknowledgedPnfsPools == null)
               calculateFileAvailableMatrix();
           return _acknowledgedPnfsPools;
       }

       public int getAllowedPoolCount()
       {
           return _allowedPoolCount;
       }

       public int getAvailablePoolCount()
       {
           return _availablePoolCount;
       }

       public List<List<PoolCostCheckable>> getFileAvailableMatrix()
           throws CacheException, InterruptedException
       {
           if (_allowedAndAvailableMatrix == null)
               calculateFileAvailableMatrix();
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
        *             _allowedAndAvailableMatrix
        *             _allowedAndAvailable
        */
       private void calculateFileAvailableMatrix()
           throws CacheException, InterruptedException
       {

         if( _storageInfo == null )
            throw new
            CacheException(189,"Storage Info not available");

         String hostName     = _protocolInfo instanceof IpProtocolInfo  ?((IpProtocolInfo)_protocolInfo).getHosts()[0] : null ;
         String protocolString = _protocolInfo.getProtocol() + "/" + _protocolInfo.getMajorVersion() ;
         //
         // will ask the PnfsManager for a hint
         // about the pool locations of this
         // pnfsId. Returns an enumeration of
         // the possible pools.
         //
         List<String> expectedFromPnfs = _pnfsHandler.getCacheLocations( _pnfsId ) ;
         say( "calculateFileAvailableMatrix _expectedFromPnfs : "+expectedFromPnfs ) ;
         //
         // check if pools are up and file is really there.
         // (returns unsorted list of costs)
         //
         _acknowledgedPnfsPools =
             queryPoolsForPnfsId(expectedFromPnfs, _pnfsId, 0,
                                 _protocolInfo.isFileCheckRequired());
         say( "calculateFileAvailableMatrix _acknowledgedPnfsPools : "+_acknowledgedPnfsPools ) ;
         Map<String, PoolCostCheckable> availableHash =
             new HashMap<String, PoolCostCheckable>() ;
         for( PoolCostCheckable cost: _acknowledgedPnfsPools ){
            availableHash.put( cost.getPoolName() , cost ) ;
         }
         //
         //  get the prioritized list of allowed pools for this
         //  request. (We are only allowed to use the level-1
         //  pools.
         //
         PoolPreferenceLevel [] level =
             _selectionUnit.match( DirectionType.READ ,
                                   hostName ,
                                   protocolString ,
                                   _storageInfo,
                                   _linkGroup ) ;

         _listOfPartitions          = new ArrayList<PoolManagerParameter>();
         _allowedAndAvailableMatrix = new ArrayList<List<PoolCostCheckable>>();
         _allowedPoolCount          = 0 ;
         _availablePoolCount        = 0 ;

         for( int prio = 0 ; prio < level.length ; prio++ ){

            List<String> poolList = level[prio].getPoolList() ;
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

            List<PoolCostCheckable> result =
                new ArrayList<PoolCostCheckable>(poolList.size());
            for (String poolName : poolList) {
                PoolCostCheckable cost;
                if ((cost = availableHash.get(poolName)) != null) {
                    result.add(cost);
                    _availablePoolCount++;
                }
                _allowedPoolCount++;
            }

            sortByCost(result, false, parameter);

            say("calculateFileAvailableMatrix : av matrix[*," + prio + "] "
                + result);

            _allowedAndAvailableMatrix.add(result);
         }
         //
         // just in case, let us define a default parameter set
         //
         if( _listOfPartitions.isEmpty() )
             _listOfPartitions.add( _partitionManager.getParameterCopyOf() ) ;
         //
         _calculationDone = true ;
         return  ;
      }

       public List<PoolCostCheckable> getCostSortedAvailable()
           throws CacheException, InterruptedException
       {
           //
           // here we don't now exactly which parameter set to use.
           //
           if (!_calculationDone)
               calculateFileAvailableMatrix();
           List<PoolCostCheckable> list =
               new ArrayList<PoolCostCheckable>(getAcknowledgedPnfsPools());
           sortByCost(list, false);
           return list;
       }

       public List<List<PoolCostCheckable>>
           getStagePoolMatrix(StorageInfo  storageInfo,
                              ProtocolInfo protocolInfo,
                              long         filesize)
           throws CacheException, InterruptedException
       {
           return getFetchPoolMatrix(DirectionType.CACHE,
                                     storageInfo,
                                     protocolInfo,
                                     filesize);
       }

       public List<List<PoolCostCheckable>>
           getFetchPoolMatrix(DirectionType       mode ,        /* cache, p2p */
                              StorageInfo  storageInfo ,
                              ProtocolInfo protocolInfo ,
                              long         filesize  )
           throws CacheException, InterruptedException
       {

         String hostName     =
                    protocolInfo instanceof IpProtocolInfo ?
                    ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                    null ;


         PoolPreferenceLevel [] level =
             _selectionUnit.match( mode ,
                                   hostName ,
                                   protocolInfo.getProtocol()+"/"+protocolInfo.getMajorVersion() ,
                                   storageInfo,
                                   _linkGroup) ;
         //
         //
         if( level.length == 0 )
             return Collections.EMPTY_LIST;

         //
         // Copy the matrix into a linear HashMap(keys).
         // Exclude pools which contain the file.
         //
         List<PoolCostCheckable> acknowledged =
             getAcknowledgedPnfsPools();
         Map<String, PoolCostCheckable> poolMap =
             new HashMap<String,PoolCostCheckable>();
         Set<String> poolAvailableSet =
             new HashSet<String>();
         for (PoolCheckable pool : acknowledged)
             poolAvailableSet.add(pool.getPoolName());
         for (int prio = 0; prio < level.length; prio++) {
            for (String poolName : level[prio].getPoolList()) {
               //
               // skip if pool already contains the file.
               //
               if (poolAvailableSet.contains(poolName))
                   continue;

               poolMap.put(poolName, null);
            }
         }
         //
         // Add the costs to the pool list.
         //
         for (PoolCostCheckable cost :
                  queryPoolsForCost(poolMap.keySet(), filesize)) {
             poolMap.put(cost.getPoolName(), cost);
         }
         //
         // Build a new matrix containing the Costs.
         //
         _listOfPartitions = new ArrayList<PoolManagerParameter>();
         List<List<PoolCostCheckable>> costMatrix =
             new ArrayList<List<PoolCostCheckable>>();

         for (PoolPreferenceLevel preferenceLevel: level) {
             //
             // skip empty level
             //
             PoolManagerParameter parameter =
                 _partitionManager.getParameterCopyOf(preferenceLevel.getTag());
            _listOfPartitions.add(parameter);

             List<String> poolList = preferenceLevel.getPoolList() ;
             if( poolList.isEmpty() )continue ;

             List<PoolCostCheckable> row = new ArrayList<PoolCostCheckable>(poolList.size());
             for (String pool : poolList) {
                PoolCostCheckable cost = poolMap.get(pool);
                if (cost != null)
                    row.add(cost);
             }
             //
             // skip if non of the pools is available
             //
             if( row.isEmpty() )continue ;
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
          _log.debug("PFL ["+_pnfsId+"] : "+message);
      }

       public List<PoolCostCheckable> getStorePoolList(long filesize)
           throws CacheException, InterruptedException
       {
           return getStorePoolList(_storageInfo, _protocolInfo, filesize);
       }

       private List<PoolCostCheckable>
           getStorePoolList(StorageInfo  storageInfo,
                            ProtocolInfo protocolInfo,
                            long         filesize)
           throws CacheException, InterruptedException
       {
         String  hostName    =
                    protocolInfo instanceof IpProtocolInfo ?
                    ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                    null ;
         int  maxDepth      = 9999 ;
         PoolPreferenceLevel [] level =
             _selectionUnit.match( DirectionType.WRITE ,
                                   hostName ,
                                   protocolInfo.getProtocol()+"/"+protocolInfo.getMajorVersion() ,
                                   storageInfo,
                                   _linkGroup ) ;
         //
         // this is the final knock out.
         //
         if( level.length == 0 )
            throw new
            CacheException( 19 ,
                             "No write pools configured for <"+ storageInfo +
                                "> in the linkGroup " +
                                ( _linkGroup == null ? "[none]" : _linkGroup) ) ;

         List<PoolCostCheckable> costs = null ;

         PoolManagerParameter parameter = null ;

         for( int prio = 0 ; prio < Math.min( maxDepth , level.length ) ; prio++ ){

            costs     = queryPoolsForCost( level[prio].getPoolList() , filesize ) ;

            parameter = _partitionManager.getParameterCopyOf(level[prio].getTag()) ;

            if( !costs.isEmpty() ) break ;
         }

         if( costs == null || costs.isEmpty() )
            throw new CacheException( 20 ,
                            "No write pool available for <"+ storageInfo +
                                "> in the linkGroup " +
                                ( _linkGroup == null ? "[none]" : _linkGroup));

         sortByCost( costs , true , parameter ) ;

         PoolCostCheckable check = costs.get(0) ;

         double lowestCost = calculateCost( check , true , parameter ) ;

         /* Notice that
          *
          *    !(lowestCost  <= _maxWriteCost)  != (lowerCost > _maxWriteCost)
          *
          * when using floating point calculations!
          */
         if( !(lowestCost  <= _maxWriteCost) )
             throw new
             CacheException( 21 , "Best pool <"+check.getPoolName()+
                                  "> too high : "+lowestCost ) ;

         return costs ;
      }

       public void sortByCost(List<PoolCostCheckable> list, boolean cpuAndSize)
       {
           sortByCost(list, cpuAndSize, getCurrentParameterSet());
       }

       public void sortByCost(List<PoolCostCheckable> list, boolean cpuAndSize,
                               PoolManagerParameter parameter)
       {
           ssortByCost(list, cpuAndSize, parameter);
       }
   }

    private void ssortByCost(List<PoolCostCheckable> list, boolean cpuAndSize,
                            PoolManagerParameter parameter)
    {
        Collections.shuffle(list);
        Collections.sort(list, new CostComparator(cpuAndSize, parameter));
    }

    public Comparator<PoolCostCheckable>
        getCostComparator(boolean both, PoolManagerParameter parameter)
    {
        return new CostComparator(both, parameter);
    }

   public class CostComparator implements Comparator<PoolCostCheckable> {

       private final boolean              _useBoth;
       private final PoolManagerParameter _para;
       private CostComparator( boolean useBoth , PoolManagerParameter para ){
         _useBoth = useBoth ;
         _para    = para ;
       }

       @Override
       public int compare(PoolCostCheckable check1, PoolCostCheckable check2)
       {
           return Double.compare(calculateCost(check1, _useBoth, _para),
                                 calculateCost(check2, _useBoth, _para));
       }
    }
    private double calculateCost( PoolCostCheckable checkable , boolean useBoth , PoolManagerParameter para ){
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

    private List<PoolCostCheckable> queryPoolsForPnfsId(Collection<String> pools,
                                                        PnfsId pnfsId,
                                                        long filesize,
                                                        boolean checkFileExistence)
        throws InterruptedException
    {
        List<PoolCostCheckable> list = new ArrayList<PoolCostCheckable>();

        if (checkFileExistence) {

            SpreadAndWait control = new SpreadAndWait(getCellEndpoint(),
                    _poolTimeout);

            for (String poolName: pools) {

                //
                // deselection inactive and disabled pools
                //
                PoolSelectionUnit.SelectionPool pool = _selectionUnit
                        .getPool(poolName);
                if ((pool == null) || !pool.canRead() || !pool.isActive())
                    continue;

                _log.info("queryPoolsForPnfsId : PoolCheckFileRequest to : {}",
                      poolName);
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
                    _log.warn("Exception sending PoolCheckFileRequest to "
                            + poolName + " : " + exc);
                }
            }

            //
            // scan the replies
            //
            CellMessage answer = null;

            while ((answer = control.next()) != null) {

                Object message = answer.getMessageObject();

                if (!(message instanceof PoolCheckFileMessage)) {
                    _log.warn("queryPoolsForPnfsId : Unexpected message from ({}) {}",
                         answer.getSourcePath(), message.getClass());
                    continue;
                }

                PoolCheckFileMessage poolMessage =
                    (PoolCheckFileMessage) message;
                _log.info("queryPoolsForPnfsId : reply : {}", poolMessage);

                boolean have = poolMessage.getHave();
                String poolName = poolMessage.getPoolName();
                if (have) {

                    PoolCostCheckable cost =
                        _costModule.getPoolCost(poolName, filesize);
                    if (cost != null) {
                        PoolCheckAdapter check = new PoolCheckAdapter(cost);
                        check.setHave(have);
                        check.setPnfsId(pnfsId);
                        list.add(check);
                        _log.info("queryPoolsForPnfsId : returning : {}", check);
                    }
                } else if (!poolMessage.getWaiting() && poolMessage.getReturnCode() == 0) {
                    _log.warn("queryPoolsForPnfsId : clearingCacheLocation for pnfsId {} at pool {}",
                        pnfsId, poolName);
                    _pnfsHandler.clearCacheLocation(pnfsId, poolName);
                }
            }

        } else {

            for (String poolName : pools) {

                PoolCostCheckable cost =
                        _costModule.getPoolCost(poolName, filesize);
                if (cost != null) {
                    PoolCheckAdapter check = new PoolCheckAdapter(cost);
                    check.setHave(true);
                    check.setPnfsId(pnfsId);
                    list.add(check);
                }
            }
        }

        _log.info("queryPoolsForPnfsId : number of valid replies : {}", list.size());
        return list;

    }
    public List<PoolCostCheckable> queryPoolsByLinkName(String linkName, long filesize) {

        PoolSelectionUnit.SelectionLink link = _selectionUnit.getLinkByName(linkName);
        PoolManagerParameter parameter = _partitionManager.getParameterCopyOf(link.getTag());

        Collection<String> poolNames = transform(link.pools(),
            new Function<PoolSelectionUnit.SelectionPool, String>()   {

                @Override
                public String apply(PoolSelectionUnit.SelectionPool pool) {
                    return pool.getName();
                }
            });

        List<PoolCostCheckable> list = queryPoolsForCost(poolNames, filesize);

        ssortByCost(list, true, parameter);

        return list;
    }

    private List<PoolCostCheckable> queryPoolsForCost(Collection<String> pools,
                                                      long filesize)
    {
        List<PoolCostCheckable> list = new ArrayList<PoolCostCheckable>();

        for( String poolName: pools ){

            PoolCostCheckable costCheck = _costModule.getPoolCost( poolName , filesize ) ;
            if( costCheck != null ){
               list.add( costCheck ) ;
               _log.info( "queryPoolsForCost : costModule : "+poolName+" ("+filesize+") "+costCheck);
            }
        }

        return list ;
    }

    private PoolManagerPoolInformation getPoolInformation(PoolSelectionUnit.SelectionPool pool)
    {
        String name = pool.getName();
        PoolManagerPoolInformation info = new PoolManagerPoolInformation(name);
        PoolCostCheckable cost = _costModule.getPoolCost(name, 0);
        if (!pool.isActive() || cost == null) {
            info.setSpaceCost(Double.POSITIVE_INFINITY);
            info.setCpuCost(Double.POSITIVE_INFINITY);
        } else {
            info.setSpaceCost(cost.getSpaceCost());
            info.setCpuCost(cost.getPerformanceCost());
        }
        info.setPoolCostInfo(_costModule.getPoolCostInfo(name));
        return info;
    }

    private Collection<PoolManagerPoolInformation> getPoolInformation(Collection<PoolSelectionUnit.SelectionPool> pools) {
        return transform(pools,
            new Function<PoolSelectionUnit.SelectionPool, PoolManagerPoolInformation>()   {

                @Override
                public PoolManagerPoolInformation apply(SelectionPool pool) {
                    return getPoolInformation(pool);
                }
            });
    }

    public PoolManagerPoolInformation getPoolInformation(String name)
        throws InterruptedException, NoSuchElementException
    {
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(name);
        if (pool == null) {
            throw new NoSuchElementException("No such pool: " + name);
        }
        return getPoolInformation(pool);
    }

    public Collection<PoolManagerPoolInformation>
        getPoolsByLink(String linkName)
        throws InterruptedException, NoSuchElementException
    {
        PoolSelectionUnit.SelectionLink link =
            _selectionUnit.getLinkByName(linkName);
        return getPoolInformation(link.pools());
    }

    public Collection<PoolManagerPoolInformation>
        getPoolsByPoolGroup(String poolGroup)
        throws InterruptedException, NoSuchElementException
    {
        Collection<PoolSelectionUnit.SelectionPool> pools =
            _selectionUnit.getPoolsByPoolGroup(poolGroup);
        return getPoolInformation(pools);
    }

    /**
     * Fetch the percentile performance cost; that is, the cost
     * of the <code>n</code>th pool, in increasing order of performance cost,
     * where <code>n</code> is <code>(int)floor( fraction * numberOfPools)</code>
     * @param fraction the percentile fraction.  The value must be between 0 and 1.
     * @return the nth percentile performance cost, or 0 if there are no pools.
     */
    public double getPoolsPercentilePerformanceCost( double fraction) {
        return _costModule.getPoolsPercentilePerformanceCost( fraction);
    }

}
