package dmg.security.cipher ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class EncryptionException 
       extends Exception {
       
   private static final long serialVersionUID = -2606805781429231740L;
   public EncryptionException( String msg ){
      super( msg ) ;
   }       
} 
 
