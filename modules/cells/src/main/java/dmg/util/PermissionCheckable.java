package dmg.util ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface PermissionCheckable {

   void checkPermission(Authorizable auth,
                        String aclName)
          throws AclException ;
          
}
