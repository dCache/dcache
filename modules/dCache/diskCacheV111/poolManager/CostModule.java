// $Id: CostModule.java,v 1.2 2006-10-10 13:50:50 tigran Exp $

package diskCacheV111.poolManager ;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolCostCheckable;
import dmg.cells.nucleus.CellMessage;

public interface CostModule {

   public void messageArrived( CellMessage cellMessage ) ;
   public boolean isActive() ;
   public PoolCostCheckable getPoolCost( String poolName , long filesize ) ;
   public PoolCostInfo getPoolCostInfo(String poolName);
}
