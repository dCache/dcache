package dmg.cells.services.login.user ;

import java.util.Enumeration;
import java.util.NoSuchElementException;
public interface AcDictionary {

    public Enumeration<String> getPrincipals() ;
    public boolean getPermission(String prinicalName )
           throws NoSuchElementException ;
    public boolean isResolved() ;
    public String getInheritance() ;

}
