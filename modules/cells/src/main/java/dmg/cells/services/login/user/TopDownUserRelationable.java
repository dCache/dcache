// $Id: TopDownUserRelationable.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.util.Enumeration;
import java.util.NoSuchElementException;

public interface TopDownUserRelationable {

    Enumeration<String> getContainers() ;
    void        createContainer(String container)
        throws DatabaseException ;
    Enumeration<String> getElementsOf(String container)
        throws NoSuchElementException ;
    boolean     isElementOf(String container, String element)
        throws NoSuchElementException ;
    void        addElement(String container, String element)
        throws NoSuchElementException ;
    void     removeElement(String container, String element)
        throws NoSuchElementException ;
    void     removeContainer(String container)
        throws NoSuchElementException ,
               DatabaseException ;
}
