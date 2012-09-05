package dmg.security.cipher ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class EncryptionKeyNotFoundException 
       extends EncryptionException {
       
   private static final long serialVersionUID = 2630645045566354324L;
   public EncryptionKeyNotFoundException( String msg ){
      super( msg ) ;
   }       
} 
