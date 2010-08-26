package dmg.protocols.telnet ;

import java.net.* ;
import java.io.* ;
import java.util.* ;

public class TelnetOutputStream2 extends OutputStream {

   private TelnetStreamEngine _core =  null ;
   
   public TelnetOutputStream2( TelnetStreamEngine core ){
      _core = core ;
   }
   public void write( int c ) throws IOException {   
      _core.write( c ) ;
   }
   public void close() throws IOException {
       _core.close() ;
   }
} 
