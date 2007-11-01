package dmg.util ;

import java.util.* ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface PermissionCheckable {

   public void checkPermission( Authorizable auth ,
                                String aclName      ) 
          throws AclException ;
          
}
