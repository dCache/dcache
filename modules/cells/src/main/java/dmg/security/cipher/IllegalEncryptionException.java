package dmg.security.cipher ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class IllegalEncryptionException 
       extends EncryptionException {
       
   private static final long serialVersionUID = 1270420052211300722L;
   public IllegalEncryptionException( String msg ){
      super( msg ) ;
   }       
} 
