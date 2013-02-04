// $Id: AclPermissionException.java,v 1.2 2005-03-08 15:37:16 patrick Exp $

package dmg.cells.services.login.user  ;

import java.io.Serializable;

public class AclPermissionException
       extends Exception 
       implements Serializable {
       
    private static final long serialVersionUID = -2497174074062405497L;
    public AclPermissionException( String message ){
        super( message ) ;    
    }      
}
