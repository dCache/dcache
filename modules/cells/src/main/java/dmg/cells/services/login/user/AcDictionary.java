package dmg.cells.services.login.user ;
import java.util.* ;
public interface AcDictionary {

    public Enumeration<String> getPrincipals() ;
    public boolean getPermission(String prinicalName ) 
           throws NoSuchElementException ;
    public boolean isResolved() ;
    public String getInheritance() ;

}
