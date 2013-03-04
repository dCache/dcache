package dmg.security.cipher ;

import java.io.IOException;


/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface EncryptionKeyInputStream {

   public EncryptionKey readEncryptionKey()
          throws IOException  ;


}
