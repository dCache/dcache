package  dmg.cells.services ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;

import java.util.* ;
import java.io.* ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class BatchCell extends CellAdapter implements Runnable {
   private BufferedReader _in     = null ;
   private Thread         _worker = null ;
   private CellShell      _shell  = null ;
   private CellNucleus    _nucleus ;

   public BatchCell( String name , String [] argStrings )
          throws Exception {
      super( name , ""  , true ) ;

      useInterpreter(false) ;
//      setPrintoutLevel( 3 ) ;
      _nucleus = getNucleus() ;
      _shell   = new CellShell( _nucleus ) ;
      try{
         String line = null ;
         for( int i = 0 ; i < argStrings.length ; i++ ){
            line = argStrings[i] ;
            if( line.length() == 0 )continue ;
            if( line.charAt(0) == '#' )continue ;
            try{
               say( "Executing ...  : "+line ) ;
               Object result = _shell.objectCommand2( line ) ;
               if( result == null )break ;
               else if( result instanceof Throwable )esay( (Throwable) result ) ;
               say( result.toString() ) ;
            }catch( CommandExitException cee ){
               int rc = cee.getErrorCode() ;
               if( rc == 666 ){
                  System.err.println( "PANIC : "+cee.getErrorMessage() ) ;
                  System.exit(6) ;
               }
               break ;
            }catch( Exception ee ){
               esay( "Problem executing : '"+line+"' : "+ee ) ;
               break ;
            }
         }
      }catch( Exception e ){
          kill() ;
          throw e ;

      }
      kill() ;
   }
   public BatchCell( String name , String argString )
          throws Exception {
      super( name , argString  , false ) ;

      Args args = getArgs() ;

      useInterpreter(false) ;
//      setPrintoutLevel( 3 ) ;
      _nucleus = getNucleus() ;
      try{
         if( args.argc() < 1 )
            throw new IllegalArgumentException( "Usage : ... <batchFilename>" ) ;

         String inputObject = args.argv(0) ;

         if( args.getOpt("jar") != null ){
            InputStream input = ClassLoader.getSystemResourceAsStream(inputObject) ;
            if( input == null )
               throw new
               IllegalArgumentException("Resource not found : "+inputObject);
            _in = new BufferedReader( new InputStreamReader(input) ) ;
         }else{
            _in = new BufferedReader( new FileReader( inputObject ) ) ;
         }
         _shell   = new CellShell( getNucleus() ) ;
         //         _worker  = getNucleus().newThread(this);
         _worker = new Thread(this);
         _worker.start() ;
      }catch( Exception e ){
          start() ;
          kill() ;
          throw e ;

      }
      start() ;
   }
   public void run(){
     if( Thread.currentThread() == _worker ){
        String line = null ;
        StringBuffer sb = null ;
        try{
           while( ( line = _in.readLine() ) != null ){
              if( line.length() == 0 )continue ;
              if( line.charAt(0) == '#' )continue ;
              if( sb == null )sb = new StringBuffer() ;
              int len = line.length()  ;
              if( line.charAt(len-1) == '\\' ){
                 if( len < 2 )continue ;
                 sb.append( line.substring(0,len-1) ) ;
                 continue ;
              }else{
                 sb.append( line ) ;
              }
              try{
                 line = sb.toString() ;
                 sb   = null ;
                 say( "Executing ...  : "+line ) ;
                 Object result = _shell.objectCommand2( line ) ;
                 if( result == null )break ;
                 if( ( result instanceof Throwable ) && ! ( result instanceof CommandException ) ){
                     esay( (Throwable) result ) ;
                 }
                 say( result.toString() ) ;
              }catch( CommandExitException cee ){
                 int rc = cee.getErrorCode() ;
                 if( rc == 666 ){
                    System.err.println( "PANIC : "+cee.getErrorMessage() ) ;
                    System.exit(6) ;
                 }
                 break ;
              }catch( Exception ee ){
                 esay( "Problem executing : '"+line+"' : "+ee ) ;
                 break ;
              }
           }

        }catch( IOException ioe ){
           esay( "Io Problem : "+ioe ) ;
        }
        try{ _in.close() ; }catch( IOException e ) {}
        kill() ;
     }
   }

}
