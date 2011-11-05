package dmg.security.cipher ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface StreamSecurity {

   public StreamEncryption getEncryption( String domain )
          throws EncryptionKeyNotFoundException ;
   public StreamEncryption getSessionEncryption();
   public StreamEncryption getSessionEncryption( byte [] keyDescriptor )
          throws IllegalEncryptionException ;
} 
