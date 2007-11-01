package dmg.apps.libraryServer ;

import dmg.cells.nucleus.* ;
import dmg.util.Args;

public class LibraryServerCell extends CellAdapter implements Runnable {

   private CellNucleus _nucleus = null ;
   private Args        _args    = null ;
   private CellPath    _path    = null ;
   public LibraryServerCell( String name , String args )throws Exception {
      super( name , args , false ) ;
      
      _args    = getArgs() ;
      _nucleus = getNucleus() ;
      
      String mcPath = _args.getOpt("mc") ;
      if( mcPath == null )_path = new CellPath("mc");
      _path = new CellPath( mcPath==null ? "mc" : mcPath ) ;
      
      String protocol = _args.getOpt("protocol") ;
      
   }
   public void run()
   {
   }







}
