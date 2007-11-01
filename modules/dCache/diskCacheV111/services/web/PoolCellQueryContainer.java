// $Id: PoolCellQueryContainer.java,v 1.1 2006-06-05 08:51:27 patrick Exp $Cg

package diskCacheV111.services.web ;

import java.io.* ;
import java.util.* ;
import dmg.cells.nucleus.* ;
import diskCacheV111.pools.* ;


public class PoolCellQueryContainer implements java.io.Serializable {

   private Map _infoMap  = new HashMap() ;
   private Map _topology = null ;
   static public class PoolCellQueryInfo implements java.io.Serializable {

        private PoolCellInfo _poolInfo    = null ;
        private long         _pingTime    = 0L ;
        private long         _arrivalTime = 0L ;
        public PoolCellQueryInfo( PoolCellInfo poolInfo , long pingTime , long arrivalTime ){
            _poolInfo = poolInfo ;
            _pingTime = pingTime ;
            _arrivalTime = arrivalTime ;
        }
        public String toString(){ return _poolInfo.toString() ; }
        public PoolCellInfo getPoolCellInfo(){ return _poolInfo ; }
        public long getPingTime(){ return _pingTime ; }
        public boolean isOk(){ return true ; }
   }
   public void put( String name , PoolCellQueryInfo info ){
      _infoMap.put( name , info ) ;
   }
   public PoolCellQueryInfo getInfoByName( String name ){ 
      return (PoolCellQueryInfo)_infoMap.get(name) ; 
   }
   public void setTopology( Map topology ){
      _topology = topology ;
   }
   public Set getPoolClassSet(){ return _topology.keySet() ; }
   public Set getPoolGroupSetByClassName( String className ){
       Map map = (Map)_topology.get(className);
       if( map == null )return null ;
       return map.keySet() ;
   }
   public Map getPoolMap( String className , String groupName ){
       Map groupMap = (Map)_topology.get(className);
       
       if( groupMap == null )return null ;
       
       return (Map)groupMap.get(groupName);
   }
   public Map getTopolgy(){ return _topology ; }
   public String toString(){
   
      StringBuffer sb = new StringBuffer() ;
      
      for( Iterator i = _topology.entrySet().iterator() ; i.hasNext() ; ){
      
          Map.Entry classes    = (Map.Entry)i.next() ;
          String    className  = (String)classes.getKey() ;
          Map       groupsMap  = (Map)classes.getValue() ;
          
          sb.append(" ").append(className).append("\n");
          
          for( Iterator j = groupsMap.entrySet().iterator() ; j.hasNext() ; ){
          
             Map.Entry groups    = (Map.Entry)j.next() ;
             String    groupName = (String)groups.getKey() ;
             Map       poolsMap  = (Map)groups.getValue() ;
             
             sb.append("  ").append(groupName).append("\n");
             
             for( Iterator k = poolsMap.entrySet().iterator() ; k.hasNext() ; ){
                 Map.Entry         pools    = (Map.Entry)k.next() ;
                 String            poolName = (String)pools.getKey() ;
                 PoolCellQueryInfo info     = (PoolCellQueryInfo)pools.getValue() ;
                 
                 sb.append("    ").append(poolName).append(info.toString()).append("\n");
                 
             }
          
          }
      }
      
      return sb.toString();
   }
}
