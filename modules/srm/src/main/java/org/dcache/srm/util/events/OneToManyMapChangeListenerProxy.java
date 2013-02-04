/*
 * OneToManyMapChangeListenerProxy.java
 *
 * Created on February 10, 2004, 11:31 AM
 */

package org.dcache.srm.util.events;

import java.beans.PropertyChangeListenerProxy;

/**
 *
 * @author  timur
 */
public class OneToManyMapChangeListenerProxy 
    extends PropertyChangeListenerProxy
    implements OneToManyMapChangeListener{
    /** Creates a new instance of OneToManyMapChangeListenerProxy */
    public OneToManyMapChangeListenerProxy(String eventName,
                                    OneToManyMapChangeListener listener) {
        super(eventName, listener);
    }
        
    
}
