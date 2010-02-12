// $Id: PoolOperator.java,v 1.2 2003-08-23 16:53:47 cvs Exp $

package diskCacheV111.poolManager ;

import dmg.cells.nucleus.* ;
import java.util.* ;
import diskCacheV111.vehicles.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolOperator
    extends CostModuleV1
{
    private final static Logger _log = LoggerFactory.getLogger(PoolOperator.class);

   private HashMap _poolHash = new HashMap() ;

    public PoolOperator()
    {
        super();
    }

    public synchronized void messageArrived(PoolManagerPoolUpMessage msg)
    {
        super.messageArrived(msg);
        _poolHash.put(msg.getPoolName(), msg.getTagMap());
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
          _log.error( "Missing resource exception from get cost : "+ee ) ;
         return null ;
      }

   }
}
