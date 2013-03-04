// $Id: TopDownUserRelationable.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.util.Enumeration;
import java.util.NoSuchElementException;

public interface TopDownUserRelationable {

    public Enumeration<String> getContainers() ;
    public void        createContainer( String container )
        throws DatabaseException ;
    public Enumeration<String> getElementsOf( String container )
        throws NoSuchElementException ;
    public boolean     isElementOf( String container , String element )
        throws NoSuchElementException ;
    public void        addElement( String container , String element )
        throws NoSuchElementException ;
    public void     removeElement( String container , String element )
        throws NoSuchElementException ;
    public void     removeContainer( String container )
        throws NoSuchElementException ,
               DatabaseException ;
}
