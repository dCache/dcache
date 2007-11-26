package dmg.util ;
public class AuthorizedString 
       implements java.io.Serializable ,
                  Authorizable {
                  
     static final long serialVersionUID = 2869160459177517712L;
     private String _principal = "" ;
     private String _string    = null ;
     public AuthorizedString( String principal , String string ){
        _principal = principal ;
        _string    = string ; 
     }                 
     public AuthorizedString( String string ){
        _string    = string ; 
     }                 
     public String getAuthorizedPrincipal(){ return _principal ; }
     public String toString(){ return _string ; }
     
}
