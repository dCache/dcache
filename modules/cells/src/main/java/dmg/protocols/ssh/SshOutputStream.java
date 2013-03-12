package dmg.protocols.ssh ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class SshOutputStream extends OutputStream {

   private static final Logger _log = LoggerFactory.getLogger(SshOutputStream.class);

   private SshStreamEngine _core;
   private int             _mode ;

   public SshOutputStream( SshStreamEngine core ){
       _core = core ;
       _mode = _core.getMode() ;
   }

   @Override
   public void write( int out ) throws IOException {
      _log.debug("write( int {} )", out);
      byte [] x = new byte[1] ;
      x[0] = (byte) out ;
      write( x , 0 , 1 ) ;
   }
   @Override
   public void write( byte [] a , int off , int len )
          throws IOException {

      if( ! _core.isActive() ) {
          throw new IOException("Stream not Active");
      }

      if( _mode == SshStreamEngine.SERVER_MODE ){
         _log.debug("server-mode: write(byte [] a, int off, int {})", len);
         _core.sendStdout( a , off , len ) ;
      }else{
         _log.debug("client-mode: write(byte [] a, int off, int {})", len);
         _core.sendStdin( a , off , len ) ;
      }

   }
   @Override
   public void write( byte [] a ) throws IOException {
      _log.debug("write( byte [] a )");
      write( a , 0 , a.length ) ;
   }

   @Override
   public void close() throws IOException {
       _core.close();
   }
}
