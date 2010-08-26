/*
 * OneToManyMapChangeListenerSupport.java
 *
 * Created on February 10, 2004, 11:28 AM
 */

package org.dcache.srm.util.events;
import org.dcache.srm.util.OneToManyMap;
import java.util.HashSet;
import java.util.Vector;

/**
 *
 * @author  timur
 */
public class OneToManyMapChangeSupport extends java.beans.PropertyChangeSupport{
    
    private static final long serialVersionUID = 3657406789310028547L;
    
    public OneToManyMapChangeSupport(OneToManyMap source) {
        super(source);
        
    }
    
}
