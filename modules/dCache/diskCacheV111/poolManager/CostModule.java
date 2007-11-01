// $Id: CostModule.java,v 1.1 2003-02-26 11:42:07 cvs Exp $ 

package diskCacheV111.poolManager ;
import  dmg.cells.nucleus.* ;
import  diskCacheV111.vehicles.PoolCostCheckable ;
import  java.io.PrintWriter ;
public interface CostModule {

   public void messageArrived( CellMessage cellMessage ) ;
   public boolean isActive() ;
   public PoolCostCheckable getPoolCost( String poolName , long filesize ) ;
   public void dumpSetup( StringBuffer sb ) ;
   public void getInfo( PrintWriter pw ) ;

}
