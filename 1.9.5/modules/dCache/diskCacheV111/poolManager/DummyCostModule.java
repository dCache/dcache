// $Id: DummyCostModule.java,v 1.2 2006-10-10 13:50:50 tigran Exp $

package diskCacheV111.poolManager ;
import java.io.PrintWriter;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolCostCheckable;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.util.Args;

public class DummyCostModule
    extends AbstractCellComponent
    implements CostModule,
               CellCommandListener
{

   private CellAdapter _cell = null ;

   public DummyCostModule( CellAdapter cellAdapter ){
      _cell = cellAdapter ;
   }
   public void messageArrived( CellMessage cellMessage ){
      Object message = cellMessage.getMessageObject() ;
      _cell.say( "DummyCostModule : messageArrived : "+message.getClass().getName() ) ;

   }
   public  PoolCostCheckable getPoolCost( String poolName , long filesize ){
      return null ;
   }
   public boolean isActive(){ return false ; }
   public String hh_cm = "" ;
   public String ac_cm_$_0_99( Args args ){
     return "Ok (DUMMY)";
   }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.append( " Submodule CostModule (cm) : ").
            append(this.getClass().getName()).
            append("\n $Id: DummyCostModule.java,v 1.2 2006-10-10 13:50:50 tigran Exp $\n\n");
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.append( "#\n# Submodule CostModule (cm) : ").
            append(this.getClass().getName()).
            append("\n# $Id: DummyCostModule.java,v 1.2 2006-10-10 13:50:50 tigran Exp $\n#\n");
    }

   public PoolCostInfo getPoolCostInfo(String poolName) {
	// TODO Auto-generated method stub
	return null;
   }

   @Override
   public double getPoolsPercentilePerformanceCost( double fraction) {
       return 0;
   }
}
