package dmg.apps.ampexCopy ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import java.util.* ;
import java.io.* ;

public class AmpexCopyCell extends CellAdapter {
   private CellNucleus _nucleus = null ;
   private Args        _args    = null ;
   private File        _requestDir = null ;
   private File        _toOsmDir   = null ;
   private File        _diskDir    = null ;
   private long        _update     = 30 ; 
   public AmpexCopyCell( String name , String argsString ){
      super( name , argsString , false ) ;
      
      _nucleus = getNucleus() ;
      _args    = getArgs() ;
      try{
            
         _requestDir = checkDirectory( "requests" ) ;
         _toOsmDir   = checkDirectory( "toOsm" ) ;
         _diskDir    = checkDirectory( "disk" ) ;
            
            
      }catch(IllegalArgumentException iew ){
         start() ;
         kill() ;
         throw iew ;
      }
      start() ;
   }
   private File checkDirectory( String name )throws IllegalArgumentException{
      String tmp = null ;
      if( ( tmp = _args.getOpt(name) ) == null )
         throw new 
         IllegalArgumentException( "Need '"+name+"'") ;
      File dir = new File(tmp) ;
      if( ! dir.isDirectory() )
         throw new
         IllegalArgumentException( "Not a dir. : "+tmp ) ;

      return dir ;
   }
   private void assembleInfo(){
   
       String [] reqTapes = _requestDir.list(
                               new FilenameFilter(){
                                  public boolean accept( File dir , String name ){
                                     return ( name.length()  >    0 ) && 
                                            ( name.charAt(0) >= '0' ) &&
                                            ( name.charAt(0) <= '9' )    ;
                                  }
                               
                               }
                            ) ;
       String [] osmTapes = _toOsmDir.list(
                               new FilenameFilter(){
                                  public boolean accept( File dir , String name ){
                                     return ( name.length()  >    0 ) && 
                                            ( name.charAt(0) >= '0' ) &&
                                            ( name.charAt(0) <= '9' )    ;
                                  }
                               
                               }
                            ) ;
       String [] diskTapes = _diskDir.list(
                               new FilenameFilter(){
                                  public boolean accept( File dir , String name ){
                                     return ( name.length()  >    0 ) && 
                                            ( name.charAt(0) >= '0' ) &&
                                            ( name.charAt(0) <= '9' )    ;
                                  }
                               
                               }
                            ) ;
       Vector activeTapes = new Vector() ;
       for( int i= 0 ; i < reqTapes.length ; i++ ){
       
          File tape = new File( _diskDir , diskTapes[i] ) ;
          //
          // not yet started
          //
          if( ! tape.isDirectory() )continue ;
          //
          // scan
          //
          String [] f = tape.list() ;
          for( int j = 0 ; j < f.length ; j++ ){
          
          }
       }
   }
   private class ActiveTape {
      int _total , _fromAmpex , _toOsm ;
      private ActiveTape( int total , int fromAmpex , int toOsm ){
         _total = total ; _fromAmpex = fromAmpex ; _toOsm = toOsm ;

      }
   }


}

