// $Id: PoolOperator.java,v 1.2 2003-08-23 16:53:47 cvs Exp $

package diskCacheV111.poolManager ;

import dmg.cells.nucleus.* ;
import java.util.* ;
import diskCacheV111.vehicles.* ;


public class PoolOperator extends CostModuleV1 {

   private HashMap _poolHash = new HashMap() ;

   public PoolOperator( CellAdapter cell ) throws Exception {
      super(cell) ;
   }
   public synchronized void messageArrived( CellMessage cellMessage ){
      super.messageArrived( cellMessage ) ;
      Object message = cellMessage.getMessageObject() ;

      if( message instanceof PoolManagerPoolUpMessage ){
         PoolManagerPoolUpMessage msg = (PoolManagerPoolUpMessage)message ;
         String poolName = msg.getPoolName() ;

//         System.out.println("PoolOperator :  "+poolName+" -> tags="+msg.getTagMap());
         _poolHash.put(  poolName ,  msg.getTagMap() ) ;

      }

   }
   public PoolCostCheckable getPoolCost( String poolName , long filesize ){

//      System.out.println("PoolOperator : getPoolCost "+poolName ) ;
      try{
         PoolCostCheckable cost = super.getPoolCost( poolName , filesize ) ;
         if( cost == null )return null ;
         Map map = (Map)_poolHash.get( poolName ) ;
         if( map != null )cost.setTagMap(map) ;
         return cost ;
      }catch(MissingResourceException ee ){
         esay( "Missing resource exception from get cost : "+ee ) ;
         return null ;
      }

   }
}
