// $Id: DummyCostModule.java,v 1.1 2003-02-26 11:42:07 cvs Exp $ 

package diskCacheV111.poolManager ;
import  dmg.cells.nucleus.* ;
import  dmg.util.Args ;
import  diskCacheV111.vehicles.PoolCostCheckable ;
import  java.io.PrintWriter ;

public class DummyCostModule implements CostModule {

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
   public void getInfo( StringBuffer sb ){
      sb.append( " Submodule CostModule (cm) : ").
         append(this.getClass().getName()).
         append("\n $Id: DummyCostModule.java,v 1.1 2003-02-26 11:42:07 cvs Exp $\n\n") ;
   }
   public void getInfo( PrintWriter pw ){
      StringBuffer sb = new StringBuffer() ;
      getInfo(sb) ;
      pw.print(sb.toString());
   }
   public void dumpSetup( StringBuffer sb ){
      sb.append( "#\n# Submodule CostModule (cm) : ").
         append(this.getClass().getName()).
         append("\n# $Id: DummyCostModule.java,v 1.1 2003-02-26 11:42:07 cvs Exp $\n#\n") ;
   }

}
