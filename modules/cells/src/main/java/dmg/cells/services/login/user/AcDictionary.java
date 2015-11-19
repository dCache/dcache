package dmg.cells.services.login.user ;

import java.util.Enumeration;
import java.util.NoSuchElementException;
public interface AcDictionary {

    Enumeration<String> getPrincipals() ;
    boolean getPermission(String prinicalName)
           throws NoSuchElementException ;
    boolean isResolved() ;
    String getInheritance() ;

}
