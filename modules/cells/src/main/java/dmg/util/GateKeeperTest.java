package dmg.util ;

import dmg.cells.nucleus.CellNucleus;

import org.dcache.util.Args;

public class GateKeeperTest {
   private CellNucleus _nucleus ;
   private GateKeeper  _gate ;
   public GateKeeperTest( CellNucleus nucleus ){
       _nucleus = nucleus ;
       _gate    = (GateKeeper)_nucleus.getDomainContext().get( "gate" ) ;
       if( _gate == null ){
          _gate = new GateKeeper() ;
          _nucleus.getDomainContext().put( "gate" , _gate ) ;
       }
   }
   public static final String hh_open = "<priority> <waitMillis>" ;
   public String ac_open_$_2( Args args )
          throws InterruptedException , ExpiredException {

      int prio = Integer.parseInt( args.argv(0) ) ;
      long wm  = Long.parseLong( args.argv(1) ) ;

      _gate.open( prio , wm ) ;

      return "GateKeeper opened for "+Thread.currentThread()  ;
   }
   public String ac_close( Args args ){
      _gate.close() ;
      return "GateKeeper closed for "+Thread.currentThread() ;
   }



}
