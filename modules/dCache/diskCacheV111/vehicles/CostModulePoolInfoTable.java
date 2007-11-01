package diskCacheV111.vehicles ;

import  java.util.Date ;
import  java.util.Collection ;
import  java.util.HashMap ;
import  diskCacheV111.pools.PoolCostInfo ;

/**
 * An Hashtable with the timestamping feature.<br/>
 * This table is designed to contain PoolCostInfo objects.
 * In this table there are all information about all pools,
 * then the pool's state at a given time (recorded by the 
 * timestamp). The key is the name of the pool as String.
 *  
 * @author Nicolò Fioretti
 * @see java.util.Hashtable
 * @see diskCacheV111.pools.PoolCostInfo
 */
public class CostModulePoolInfoTable implements java.io.Serializable  {
   
   private   long    _timestamp   = new Date().getTime() ;
   private   HashMap _table       = new HashMap() ;
   
   private static final long serialVersionUID = -8035876156296337291L;
   
   
   public long getTimestamp(){ return _timestamp; }
  
   public String toString(){
      return "Pools Space Information: " + getTimestamp() ;
   }
   public void addPoolCostInfo( String poolName , PoolCostInfo info ){
      _table.put( poolName , info ) ;
   }
   public PoolCostInfo getPoolCostInfoByName( String poolName ){
      return (PoolCostInfo)_table.get( poolName ) ;
   }
   public Collection poolInfos(){
       return _table.values();
   }
   
    
}
