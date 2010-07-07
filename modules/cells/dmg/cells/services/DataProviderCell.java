package dmg.cells.services ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import java.net.* ;
import java.io.* ;
import java.util.* ;


public class DataProviderCell extends CellAdapter {
    private CellNucleus _nucleus ;
    private Args        _args ;
    private File        _dir ;
    private Hashtable   _classHash      = new Hashtable() ;
    private int         _requestCounter = 0 ;
    private int         _errorCounter   = 0 ;
    public DataProviderCell( String cellName , String args ){
        super( cellName , args ,false ) ;

        _nucleus = getNucleus() ;
        _args    = getArgs() ;

        try{
           if( _args.argc() < 1 )
               throw new
               IllegalArgumentException( "USAGE : ... <storeBase>" ) ;

           _dir = new File( _args.argv(0) ) ;
           if( ! _dir.isDirectory() )
               throw new IllegalArgumentException( "Not a directory : "+_dir) ;
        }catch( Exception e ){
           start() ;
           kill() ;
           if( e instanceof IllegalArgumentException )
              throw (IllegalArgumentException) e ;
           throw new IllegalArgumentException( e.toString() ) ;
        }
        useInterpreter( false ) ;
        start() ;
   }
   public void getInfo( PrintWriter pw ){
      pw.println( " Directory   : "+_dir ) ;
      pw.println( " Requests    : "+_requestCounter ) ;
      pw.println( " Errors      : "+_errorCounter ) ;
      Enumeration e = _classHash.keys() ;
      for( ; e.hasMoreElements() ; ){
         String className = (String)e.nextElement() ;
         byte [] data = (byte [] )_classHash.get(className);
         pw.println( "   "+className+"="+data.length ) ;
      }
   }
   public String toString(){ return _dir.toString() ; }
   public void messageArrived( CellMessage msg ){
      Object req = msg.getMessageObject() ;
      if( ! ( req instanceof String ) )return ;
      Args args = new Args( (String) req ) ;
      if( args.argc() < 2 )return ;
      String command = args.argv(0) ;
      if( command.equals( "getclass" ) ){
         args.shift() ;
         do_getclass_command( msg , args ) ;
      }
      return ;
   }
   private void do_getclass_command( CellMessage msg , Args args ){
      String fileName = args.argv(0) ;
      say( "Request for class : "+fileName ) ;
      File file = new File( _dir , fileName+".class" ) ;
      if( ! file.canRead() ){
          fileName = fileName.replace( '.' , '/' ) ;
          file = new File( _dir , fileName+".class" ) ;
          if( ! file.canRead() ){
              reply( msg , "Class not found" ) ;
              say( "File not found : "+fileName ) ;
              _errorCounter ++ ;
              return ;
          }
      }
      int len = (int)file.length() ;
      if( len == 0 ){
          say( "File has zero length" ) ;
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
      }catch( Exception ee ){
         _errorCounter ++ ;
         return ;
      }finally {
         try{in.close();}catch(Exception ex ){}
      }
      return ;
   }
   private void reply( CellMessage msg , Object o ){
      try{
         say( "returning message" + o.toString() ) ;
         msg.setMessageObject( o ) ;
         msg.revertDirection() ;
         sendMessage( msg ) ;
      }catch( Exception e ){

      }
   }
}
