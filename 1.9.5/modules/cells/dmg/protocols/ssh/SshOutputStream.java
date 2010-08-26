package dmg.protocols.ssh ;

import java.net.* ;
import java.io.* ;
import java.util.* ;

public class SshOutputStream extends OutputStream {

   private SshStreamEngine _core =  null ;
   private int             _mode ;
 
   public SshOutputStream( SshStreamEngine core ){
       _core = core ;
       _mode = _core.getMode() ;
   }
   public void write( int out ) throws IOException {
      _core.printout( "SshOutputStream : write( int "+out+" )" ) ;
      byte [] x = new byte[1] ;
      x[0] = (byte) out ;
      write( x , 0 , 1 ) ;
      return ;
   }
   public void write( byte [] a , int off , int len )
          throws IOException {
          
      if( ! _core.isActive() )throw new IOException( "Stream not Active" ) ;
      
      if( _mode == SshStreamEngine.SERVER_MODE ){
          _core.printout( "SshOutputStream (s) : write( byte [] a , int off , int "+len+" )" ) ;
         _core.sendStdout( a , off , len ) ;    
      }else{
         _core.printout( "SshOutputStream (c) : write( byte [] a , int off , int "+len+" )" ) ;
         _core.sendStdin( a , off , len ) ;   
      } 
          
   }
   public void write( byte [] a ) throws IOException {
      _core.printout( "SshOutputStream : write( byte [] a )" ) ;
      write( a , 0 , a.length ) ;
   }
   public void close() throws IOException {
      _core.printout( "SshOutputStream : close()" ) ;
      _core.close() ;
   }
 
}
