package dmg.util ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 20 Nov 2006
  *
  * The CommandAclException is thrown by the 
  * CommandInterpreter if acl's are violated.
  *
  *
  */
public class CommandAclException extends CommandException {

   private static final long serialVersionUID = 1511398885429392728L;
 
    private String _acl       = "<unknown>" ;
    private String _principal = "<unknown>" ;
    
    public CommandAclException( String message ){
        super( message ) ;
    }
    public CommandAclException( String principal , String acl ){
        super("Acl >" + acl + "< denied for >" + principal + '<') ;
        _acl       = acl ;
        _principal = principal ;
    }
    public CommandAclException( Authorizable auth , String acl ){
        super("Acl >" + acl + "< denied for >" + auth.getAuthorizedPrincipal() + '<') ;
        _acl       = acl ;
        _principal = auth.getAuthorizedPrincipal() ;
    }
    public String getAcl(){ return _acl ; }
    public String getPrincipal(){ return _principal ; }
    
   
   
}
