package dmg.cells.services ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Hashtable;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.Args;

public class DataProviderCell extends CellAdapter {

    private final static Logger _log =
        LoggerFactory.getLogger(DataProviderCell.class);

    private CellNucleus _nucleus ;
    private Args        _args ;
    private File        _dir ;
    private Hashtable<String, byte[]> _classHash      = new Hashtable<>() ;
    private int         _requestCounter;
    private int         _errorCounter;
    public DataProviderCell( String cellName , String args ){
        super( cellName , args ,false ) ;

        _nucleus = getNucleus() ;
        _args    = getArgs() ;

        try{
           if( _args.argc() < 1 ) {
               throw new
                       IllegalArgumentException("USAGE : ... <storeBase>");
           }

           _dir = new File( _args.argv(0) ) ;
           if( ! _dir.isDirectory() ) {
               throw new IllegalArgumentException("Not a directory : " + _dir);
           }
        }catch( Exception e ){
           start() ;
           kill() ;
           if( e instanceof IllegalArgumentException ) {
               throw (IllegalArgumentException) e;
           }
           throw new IllegalArgumentException( e.toString() ) ;
        }
        useInterpreter( false ) ;
        start() ;
   }
   @Override
   public void getInfo( PrintWriter pw ){
      pw.println( " Directory   : "+_dir ) ;
      pw.println( " Requests    : "+_requestCounter ) ;
      pw.println( " Errors      : "+_errorCounter ) ;
       for (Object o : _classHash.keySet()) {
           String className = (String) o;
           byte[] data = _classHash.get(className);
           pw.println("   " + className + "=" + data.length);
       }
   }
   public String toString(){ return _dir.toString() ; }
   @Override
   public void messageArrived( CellMessage msg ){
      Object req = msg.getMessageObject() ;
      if( ! ( req instanceof String ) ) {
          return;
      }
      Args args = new Args( (String) req ) ;
      if( args.argc() < 2 ) {
          return;
      }
      String command = args.argv(0) ;
      if( command.equals( "getclass" ) ){
         args.shift() ;
         do_getclass_command( msg , args ) ;
      }
   }
   private void do_getclass_command( CellMessage msg , Args args ){
      String fileName = args.argv(0) ;
      _log.info( "Request for class : "+fileName ) ;
      File file = new File( _dir , fileName+".class" ) ;
      if( ! file.canRead() ){
          fileName = fileName.replace( '.' , '/' ) ;
          file = new File( _dir , fileName+".class" ) ;
          if( ! file.canRead() ){
              reply( msg , "Class not found" ) ;
              _log.info( "File not found : "+fileName ) ;
              _errorCounter ++ ;
              return ;
          }
      }
      int len = (int)file.length() ;
      if( len == 0 ){
          _log.info( "File has zero length" ) ;
          reply( msg , "Zero size entry found" ) ;
          _errorCounter ++ ;
          return ;
      }
      byte [] data = new byte[len] ;
      FileInputStream in = null ;
      try{
         in = new FileInputStream( file ) ;
         in.read( data ) ;
         _requestCounter ++ ;
         _classHash.put( fileName , data ) ;
         reply( msg , data ) ;
      }catch( IOException ee ){
         _errorCounter ++ ;
      }finally {
         try{in.close();}catch(IOException ex ){}
      }
   }
   private void reply( CellMessage msg , Serializable o ){
      try{
         _log.info( "returning message" + o.toString() ) ;
         msg.setMessageObject( o ) ;
         msg.revertDirection() ;
         sendMessage( msg ) ;
      }catch( NoRouteToCellException e ){

      }
   }
}
