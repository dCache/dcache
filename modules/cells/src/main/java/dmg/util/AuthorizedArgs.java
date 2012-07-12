package dmg.util ;

import java.util.* ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class AuthorizedArgs 
       extends Args
       implements Authorizable  {
       
   static final long serialVersionUID = 3671609275481043876L;
   private String _principal = null ;
   public AuthorizedArgs( Authorizable authObject ){
       super( authObject.toString() ) ;
       _principal = authObject.getAuthorizedPrincipal() ;
   } 
   public AuthorizedArgs( String principal , String args ){
       super( args ) ;
       _principal = principal ;
   }  
   @Override
   public String getAuthorizedPrincipal(){
      return _principal == null ? "nobody" : _principal ;
   }   
}
