//  $Id: PoolMonitorV5.java,v 1.32 2007-08-01 20:00:45 tigran Exp $

package diskCacheV111.poolManager;

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
import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileLocality;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.pools.PoolCostInfo;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileType;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.ClassicPartition;
import static org.dcache.namespace.FileAttribute.*;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

public class PoolMonitorV5
    implements Serializable
{
    private final static Logger _log =
        LoggerFactory.getLogger(PoolMonitorV5.class);

    static final long serialVersionUID = -2400834413958127412L;

    private PoolSelectionUnit _selectionUnit ;
    private CostModule        _costModule    ;
    private double            _maxWriteCost          = 1000000.0;
    private PartitionManager  _partitionManager ;

    public PoolMonitorV5()
    {
    }

    public PoolSelectionUnit getPoolSelectionUnit()
    {
        return _selectionUnit;
    }

    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    public CostModule getCostModule()
    {
        return _costModule;
    }

    public void setCostModule(CostModule costModule)
    {
        _costModule = costModule;
    }

    public PartitionManager getPartitionManager()
    {
        return _partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager)
    {
        _partitionManager = partitionManager;
    }

    public void messageToCostModule(CellMessage cellMessage)
    {
        _costModule.messageArrived(cellMessage);
    }

    // output[0] -> Allowed and Available
    // output[1] -> available but not allowed (sorted, cpu)
    // output[2] -> allowed but not available (sorted, cpu + space)
    // output[3] -> pools from pnfs
    // output[4] -> List of List (all allowed pools)

    public PnfsFileLocation getPnfsFileLocation(FileAttributes fileAttributes,
                                                ProtocolInfo protocolInfo,
                                                String linkGroup)
    {
        return new PnfsFileLocation(fileAttributes, protocolInfo, linkGroup);
    }

   public class PnfsFileLocation
   {
       private List<ClassicPartition> _listOfPartitions;
       private List<List<PoolCostCheckable>> _allowedAndAvailableMatrix;

       /**
        * Pools that are supposed to have a copy of the file and which
        * are online. Online here means that they are actively
        * registering themselves with the pool manager.
        */
       private List<PoolCostCheckable> _onlinePools;

       /**
        * Number of pools that would be able to serve the request if
        * they had a copy of the file.
        */
       private int  _allowedPoolCount          = 0;

      private boolean  _calculationDone       = false ;

      private final FileAttributes _fileAttributes;
      private final ProtocolInfo _protocolInfo ;
      private final String _linkGroup          ;

       public PnfsFileLocation(FileAttributes fileAttributes,
                               ProtocolInfo protocolInfo,
                               String linkGroup)
       {
           _fileAttributes = fileAttributes;
           _protocolInfo = protocolInfo;
           _linkGroup    = linkGroup;
       }

       public List<ClassicPartition> getListOfParameter()
           throws CacheException
       {
           if (!_calculationDone) {
               calculateFileAvailableMatrix();
           }
           return _listOfPartitions;
       }

       public ClassicPartition getCurrentParameterSet()
           throws CacheException
       {
           if (!_calculationDone) {
               calculateFileAvailableMatrix();
           }
           return _listOfPartitions.get(0);
       }

       public List<PoolCostCheckable> getOnlinePools()
           throws CacheException
       {
           if (!_calculationDone) {
               calculateFileAvailableMatrix();
           }
           return _onlinePools;
       }

       public int getAllowedPoolCount()
           throws CacheException
       {
           if (!_calculationDone) {
               calculateFileAvailableMatrix();
           }
           return _allowedPoolCount;
       }

       public List<List<PoolCostCheckable>> getFileAvailableMatrix()
           throws CacheException
       {
           if (!_calculationDone) {
               calculateFileAvailableMatrix();
           }
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
        *   Input : _fileAttributes
        *   Output :
        *             _onlinePools
        *             _allowedAndAvailableMatrix
        *             _allowedAndAvailable
        */
       private void calculateFileAvailableMatrix()
           throws CacheException
       {
         String hostName     = _protocolInfo instanceof IpProtocolInfo  ?((IpProtocolInfo)_protocolInfo).getHosts()[0] : null ;
         String protocolString = _protocolInfo.getProtocol() + "/" + _protocolInfo.getMajorVersion() ;

         Collection<String> expectedFromPnfs = _fileAttributes.getLocations();

         _log.debug("calculateFileAvailableMatrix _expectedFromPnfs : {}",
                    expectedFromPnfs);

         //
         // check if pools are up and file is really there.
         // (returns unsorted list of costs)
         //
         _onlinePools = new ArrayList<PoolCostCheckable>();
         for (String poolName: expectedFromPnfs) {
             PoolCostCheckable cost = _costModule.getPoolCost(poolName, 0);
             if (cost != null) {
                 PoolCheckAdapter check = new PoolCheckAdapter(cost);
                 check.setHave(true);
                 check.setPnfsId(_fileAttributes.getPnfsId());
                 _onlinePools.add(check);
             }
         }

         _log.debug("calculateFileAvailableMatrix _onlinePools : {}",
                    _onlinePools);

         Map<String, PoolCostCheckable> availableHash =
             new HashMap<String, PoolCostCheckable>() ;
         for( PoolCostCheckable cost: _onlinePools ){
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
                                   _fileAttributes.getStorageInfo(),
                                   _linkGroup ) ;

         _listOfPartitions          = new ArrayList<ClassicPartition>();
         _allowedAndAvailableMatrix = new ArrayList<List<PoolCostCheckable>>();
         _allowedPoolCount          = 0 ;

         for( int prio = 0 ; prio < level.length ; prio++ ){

            List<String> poolList = level[prio].getPoolList() ;

            //
            // get the allowed pools for this level and
            // and add them to the result list only if
            // they are really available.
            //
            _log.debug("calculateFileAvailableMatrix : db matrix[*,{}] {}",
                       prio, poolList);

            List<PoolCostCheckable> result =
                new ArrayList<PoolCostCheckable>(poolList.size());
            for (String poolName : poolList) {
                PoolCostCheckable cost;
                if ((cost = availableHash.get(poolName)) != null) {
                    result.add(cost);
                }
                _allowedPoolCount++;
            }

            if (result.isEmpty()) {
                continue;
            }

            ClassicPartition partition =
                _partitionManager.getPartition(level[prio].getTag()) ;
            _listOfPartitions.add(partition);

            sortByCost(result, false, partition);

            _log.debug("calculateFileAvailableMatrix : av matrix[*,{}] {}",
                       prio, result);

            _allowedAndAvailableMatrix.add(result);
         }
         //
         // just in case, let us define a default parameter set
         //
         if( _listOfPartitions.isEmpty() )
             _listOfPartitions.add( _partitionManager.getDefaultPartition() ) ;
         //
         _calculationDone = true ;
         return  ;
      }

       public List<PoolCostCheckable> getCostSortedAvailable()
           throws CacheException
       {
           //
           // here we don't now exactly which parameter set to use.
           //
           if (!_calculationDone)
               calculateFileAvailableMatrix();
           List<PoolCostCheckable> list =
               new ArrayList<PoolCostCheckable>(getOnlinePools());
           sortByCost(list, false);
           return list;
       }

       public List<List<PoolCostCheckable>>
           getFetchPoolMatrix(DirectionType       mode ,        /* cache, p2p */
                              long         filesize  )
           throws CacheException, InterruptedException
       {

         String hostName     =
             _protocolInfo instanceof IpProtocolInfo ?
             ((IpProtocolInfo) _protocolInfo).getHosts()[0] :
             null ;


         PoolPreferenceLevel [] level =
             _selectionUnit.match( mode ,
                                   hostName ,
                                   _protocolInfo.getProtocol()+"/"+_protocolInfo.getMajorVersion() ,
                                   _fileAttributes.getStorageInfo(),
                                   _linkGroup) ;
         //
         //
         if( level.length == 0 )
             return Collections.EMPTY_LIST;

         //
         // Copy the matrix into a linear HashMap(keys).
         // Exclude pools which contain the file.
         //
         List<PoolCostCheckable> online = getOnlinePools();
         Map<String, PoolCostCheckable> poolMap =
             new HashMap<String,PoolCostCheckable>();
         Set<String> poolAvailableSet =
             new HashSet<String>();
         for (PoolCheckable pool: online)
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
         _listOfPartitions = new ArrayList<ClassicPartition>();
         List<List<PoolCostCheckable>> costMatrix =
             new ArrayList<List<PoolCostCheckable>>();

         for (PoolPreferenceLevel preferenceLevel: level) {
             //
             // skip empty level
             //
             ClassicPartition partition =
                 _partitionManager.getPartition(preferenceLevel.getTag());
            _listOfPartitions.add(partition);

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
             sortByCost(row, true, partition);
             //
             // and add it to the matrix
             //
             costMatrix.add( row ) ;
         }

         return costMatrix ;
       }

      public List<PoolCostCheckable> getStorePoolList(long filesize)
          throws CacheException, InterruptedException
      {
         String  hostName    =
                    _protocolInfo instanceof IpProtocolInfo ?
                    ((IpProtocolInfo)_protocolInfo).getHosts()[0] :
                    null ;
         int  maxDepth      = 9999 ;
         PoolPreferenceLevel [] level =
             _selectionUnit.match( DirectionType.WRITE ,
                                   hostName ,
                                   _protocolInfo.getProtocol()+"/"+_protocolInfo.getMajorVersion() ,
                                   _fileAttributes.getStorageInfo(),
                                   _linkGroup ) ;
         //
         // this is the final knock out.
         //
         if (level.length == 0) {
             throw new CacheException(19,
                                      "No write pools configured for <" +
                                      _fileAttributes.getStorageInfo() +
                                      "> in the linkGroup " +
                                      (_linkGroup == null ? "[none]" : _linkGroup));
         }

         List<PoolCostCheckable> costs = null ;

         ClassicPartition partition = null;

         for( int prio = 0 ; prio < Math.min( maxDepth , level.length ) ; prio++ ){

            costs     = queryPoolsForCost( level[prio].getPoolList() , filesize ) ;

            partition = _partitionManager.getPartition(level[prio].getTag()) ;

            if( !costs.isEmpty() ) break ;
         }

         if( costs == null || costs.isEmpty() )
            throw new CacheException( 20 ,
                                      "No write pool available for <"+ _fileAttributes.getStorageInfo() +
                                "> in the linkGroup " +
                                ( _linkGroup == null ? "[none]" : _linkGroup));

         sortByCost( costs , true , partition ) ;

         PoolCostCheckable check = costs.get(0) ;

         double lowestCost = calculateCost( check , true , partition ) ;

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
           throws CacheException
       {
           sortByCost(list, cpuAndSize, getCurrentParameterSet());
       }

       public void sortByCost(List<PoolCostCheckable> list, boolean cpuAndSize,
                              ClassicPartition partition)
       {
           ssortByCost(list, cpuAndSize, partition);
       }
   }

    private void ssortByCost(List<PoolCostCheckable> list, boolean cpuAndSize,
                            ClassicPartition partition)
    {
        Collections.shuffle(list);
        Collections.sort(list, new CostComparator(cpuAndSize, partition));
    }

    public Comparator<PoolCostCheckable>
        getCostComparator(boolean both, ClassicPartition partition)
    {
        return new CostComparator(both, partition);
    }

   public class CostComparator implements Comparator<PoolCostCheckable> {

       private final boolean _useBoth;
       private final ClassicPartition _partition;
       private CostComparator(boolean useBoth, ClassicPartition partition) {
         _useBoth = useBoth;
         _partition = partition;
       }

       @Override
       public int compare(PoolCostCheckable check1, PoolCostCheckable check2)
       {
           return Double.compare(calculateCost(check1, _useBoth, _partition),
                                 calculateCost(check2, _useBoth, _partition));
       }
    }
    private double calculateCost( PoolCostCheckable checkable , boolean useBoth , ClassicPartition partition){
       if( useBoth ){
          return Math.abs(checkable.getSpaceCost())       * partition._spaceCostFactor +
                 Math.abs(checkable.getPerformanceCost()) * partition._performanceCostFactor ;
       }else{
          return Math.abs(checkable.getPerformanceCost()) * partition._performanceCostFactor ;
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

    public List<PoolCostCheckable> queryPoolsByLinkName(String linkName, long filesize) {

        PoolSelectionUnit.SelectionLink link = _selectionUnit.getLinkByName(linkName);
        ClassicPartition partition = _partitionManager.getPartition(link.getTag());

        Collection<String> poolNames = transform(link.pools(),
            new Function<PoolSelectionUnit.SelectionPool, String>()   {

                @Override
                public String apply(PoolSelectionUnit.SelectionPool pool) {
                    return pool.getName();
                }
            });

        List<PoolCostCheckable> list = queryPoolsForCost(poolNames, filesize);

        ssortByCost(list, true, partition);

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

    private Collection<PoolManagerPoolInformation>
        getPoolInformation(Collection<PoolSelectionUnit.SelectionPool> pools)
    {
        List<PoolManagerPoolInformation> result =
            new ArrayList<PoolManagerPoolInformation>();
        for (PoolSelectionUnit.SelectionPool pool: pools) {
            result.add(getPoolInformation(pool));
        }
        return result;
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
        return new ArrayList(getPoolInformation(link.pools()));
    }

    public Collection<PoolManagerPoolInformation>
        getPoolsByPoolGroup(String poolGroup)
        throws InterruptedException, NoSuchElementException
    {
        Collection<PoolSelectionUnit.SelectionPool> pools =
            _selectionUnit.getPoolsByPoolGroup(poolGroup);
        return new ArrayList(getPoolInformation(pools));
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

    public static Set<FileAttribute> getRequiredAttributesForFileLocality()
    {
        return EnumSet.of(STORAGEINFO, SIZE, LOCATIONS);
    }

    public FileLocality
        getFileLocality(FileAttributes attributes, String hostName)
    {
        if (attributes.getFileType() == FileType.DIR ||
            attributes.getSize() == 0) {
            return FileLocality.NONE;
        }

        StorageInfo storageInfo = attributes.getStorageInfo();
        PoolPreferenceLevel[] levels =
            _selectionUnit.match(DirectionType.READ,
                                 hostName,
                                 "*/*",
                                 storageInfo,
                                 null);

        Collection<String> locations = attributes.getLocations();
        for (PoolPreferenceLevel level: levels) {
            if (!Collections.disjoint(level.getPoolList(), locations)) {
                return (storageInfo.isStored()
                        ? FileLocality.ONLINE_AND_NEARLINE
                        : FileLocality.ONLINE);
            }
        }

        if (storageInfo.isStored()) {
            return FileLocality.NEARLINE;
        }

        for (String name: locations) {
            PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(name);
            if (pool == null || !pool.canReadForP2P()) {
                continue;
            }

            PoolCostInfo cost = _costModule.getPoolCostInfo(name);
            if (cost == null) {
                continue;
            }

            // REVISIT: This check should be integrated into
            // SelectionPool.canReadForP2P
            if (cost.getP2pQueue().getMaxActive() > 0){
                return FileLocality.NEARLINE;
            }
        }
        return FileLocality.UNAVAILABLE;
    }
}
