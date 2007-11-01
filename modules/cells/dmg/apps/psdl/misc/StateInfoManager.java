package dmg.apps.psdl.misc ;

import java.util.Vector ;
import java.util.Hashtable ;
import java.util.Enumeration ; 
import dmg.apps.psdl.vehicles.StateInfo ;
import dmg.cells.nucleus.CellNucleus ;
import dmg.cells.nucleus.CellPath ;
import dmg.cells.nucleus.CellMessage ;

public class StateInfoManager  implements  Runnable {
   private Thread      _worker ;
   private Object      _lock = new Object() ;
   private int         _period = 10 ;
   private Hashtable   _infoHash = new Hashtable() ;
   private boolean     _active = true ;
   
   public class StateInfoEntry {
      private CellPath  _path ;
      private StateInfo _info ; 
      private long      _timestamp ;
      private StateInfoEntry( CellPath path , StateInfo info ){
        _path = path ;
        _info = info ;
        _timestamp = System.currentTimeMillis() ;
      }
      public CellPath  getPath(){ return _path ; }
      public StateInfo getInfo(){ return _info ; }
      public String    getName(){ return _info.getName() ; }
   }

   public void stop(){ 
      synchronized( _lock ){ 
         _active = false ; 
         _lock.notifyAll() ;
      }
   }
   public void run(){
      synchronized( _lock ){
         Vector _waste = new Vector() ;
         while( _active ){
            
            long now = System.currentTimeMillis() ;
            Enumeration e = _infoHash.elements() ;
            for( ; e.hasMoreElements() ; ){
               StateInfoEntry info = (StateInfoEntry)e.nextElement() ;
               int period = info._info.getPeriod() ;
               if( ( period == 0 ) ||
                   ( ( now - info._timestamp )/1000 < 
                     ( 3 * period )                   ) )continue ;
               
               _waste.addElement( info ) ;
            }
            int size = _waste.size() ;
            for( int i = 0 ; i < size ; i++ )
               _infoHash.remove( 
                   ((StateInfoEntry)_waste.elementAt(i)).getName() ) ;
            _waste.removeAllElements() ;
            try{ _lock.wait( _period*1000 ) ; }
            catch( InterruptedException eee){}
         }
      }
   }
 
 
}
