package dmg.util ;
 
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class AclException
       extends Exception
       implements java.io.Serializable {

     static final long serialVersionUID = 1511398885429392728L;
 
    private String _acl       = "<unknown>" ;
    private String _principal = "<unknown>" ;
    public AclException( String message ){
        super( message ) ;
    }
    public AclException( String principal , String acl ){
        super( "Acl >"+acl+"< denied for >"+principal+"<" ) ;
        _acl       = acl ;
        _principal = principal ;
    }
    public AclException( Authorizable auth , String acl ){
        super( "Acl >"+acl+"< denied for >"+auth.getAuthorizedPrincipal()+"<" ) ;
        _acl       = acl ;
        _principal = auth.getAuthorizedPrincipal() ;
    }
    public String getAcl(){ return _acl ; }
    public String getPrincipal(){ return _principal ; }
    
    
}
