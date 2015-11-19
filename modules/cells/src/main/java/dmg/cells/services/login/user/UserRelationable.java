// $Id: UserRelationable.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.util.Enumeration;
import java.util.NoSuchElementException;
public interface UserRelationable extends TopDownUserRelationable {

   Enumeration<String> getParentsOf(String element)
          throws NoSuchElementException ;
   boolean     isParentOf(String element, String container)
          throws NoSuchElementException ;
}
