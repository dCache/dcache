package dmg.protocols.telnet ;

import java.io.IOException;
import java.io.InputStream;

public class TelnetInputStream2 extends InputStream {

   private TelnetStreamEngine _core;

   public TelnetInputStream2( TelnetStreamEngine core ){
      _core = core ;
   }
   @Override
   public int read() throws IOException {
       return _core.read();
   }
   //
   // we have to overwrite the following two
   // read methods. Otherwise the call to the
   // superclass method will block until all
   // requestes byte will have arrived.
   //
   @Override
   public int read( byte [] b )throws IOException {
      return this.read( b , 0 , b.length ) ;
   }
   @Override
   public int read( byte [] b , int off , int i ) throws IOException {
       int rc ;
       if( i <= 0 ) {
           return i;
       }
       if( (  rc = this.read() ) < 0 ) {
           return -1;
       }
       b[off] = (byte)rc ;
       return 1 ;
   }
   @Override
   public void close() throws IOException {
       _core.close() ;
   }
}
