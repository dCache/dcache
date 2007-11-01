// $Id: UserRelationable.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;
import java.util.* ;
public interface UserRelationable extends TopDownUserRelationable {

   public Enumeration getParentsOf( String element )
          throws NoSuchElementException ;
   public boolean     isParentOf( String element , String container ) 
          throws NoSuchElementException ;       
} 
