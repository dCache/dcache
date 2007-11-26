package dmg.security.cipher ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface EncryptionKey {

   public String    getKeyMode() ;   //  shared , public , private
   public String    getKeyType() ;   //  idea , rsa
   public String [] getDomainList() ;

}
