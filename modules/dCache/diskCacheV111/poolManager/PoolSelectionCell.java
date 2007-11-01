package diskCacheV111.poolManager ;

import java.util.* ;
import diskCacheV111.util.* ;
import dmg.util.* ;
import dmg.cells.nucleus.* ;

public class PoolSelectionCell extends CellAdapter {

   public PoolSelectionCell( String name , String sargs ){
      super( name , sargs , false ) ;
      PoolSelectionUnit psu = new PoolSelectionUnitV1() ;
      
      addCommandListener( psu ) ;
      
      start() ;
   }

}
