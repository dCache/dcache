package dmg.protocols.telnet ;

import java.net.* ;
import java.io.* ;
import java.util.* ;

public class TelnetOutputStream2 extends OutputStream {

   private TelnetStreamEngine _core;
   
   public TelnetOutputStream2( TelnetStreamEngine core ){
      _core = core ;
   }
   @Override
   public void write( int c ) throws IOException {
      _core.write( c ) ;
   }
   @Override
   public void close() throws IOException {
       _core.close() ;
   }
} 
