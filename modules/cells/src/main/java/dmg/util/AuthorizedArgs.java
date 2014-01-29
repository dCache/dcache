package dmg.util ;

import org.dcache.util.Args;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class AuthorizedArgs 
       extends Args
       implements Authorizable  {

    private String _principal;
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
