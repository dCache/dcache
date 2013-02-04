package dmg.util ;

import java.io.Serializable;

public class AuthorizedString
       implements Serializable ,
                  Authorizable {
                  
     private static final long serialVersionUID = 2869160459177517712L;
     private String _principal = "" ;
     private String _string;
     public AuthorizedString( String principal , String string ){
        _principal = principal ;
        _string    = string ; 
     }                 
     public AuthorizedString( String string ){
        _string    = string ; 
     }                 
     @Override
     public String getAuthorizedPrincipal(){ return _principal ; }
     public String toString(){ return _string ; }
     
}
