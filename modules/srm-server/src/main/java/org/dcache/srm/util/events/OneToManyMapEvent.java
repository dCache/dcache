/*
 * OneToManyMapEvent.java
 *
 * Created on February 10, 2004, 10:57 AM
 */

package org.dcache.srm.util.events;

import java.beans.PropertyChangeEvent;

/**
 *
 * @author  timur
 */
public abstract class OneToManyMapEvent extends PropertyChangeEvent{
    
    Object key;
    Object value;
    
    private static final long serialVersionUID = 6544655934489891825L;
    
    public abstract String getEventName();
    /** Creates a new instance of OneToManyMapEvent */
    public OneToManyMapEvent(Object source,String eventName,Object key, Object value) {
        super(source,eventName,key,value);
        this.key = key;
        this.value = value;
    }
}
