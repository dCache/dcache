// $Id: UserMetaDictionary.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;
import java.util.* ;

public interface UserMetaDictionary {

   public Enumeration keys() ;
   public String valueOf( String key ) ;
}
