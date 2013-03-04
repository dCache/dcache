package dmg.protocols.telnet ;

import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class TelnetOutputStreamWriter extends OutputStreamWriter {

   public TelnetOutputStreamWriter( OutputStream output ){
     super( output ) ;
   }


}

