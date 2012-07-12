/*
 * NewKeyValueMapAdded.java
 *
 * Created on February 10, 2004, 11:13 AM
 */

package org.dcache.srm.util.events;

/**
 *
 * @author  timur
 */
public final class KeyMapNonLastValueRemoved extends OneToManyMapEvent {
    
    private static final long serialVersionUID = -241085729105665748L;
    
    public KeyMapNonLastValueRemoved(Object source,Object key, Object value) 
    {
        super(source,"KeyMapNonLastValueRemoved",key,value);
    }
    @Override
    public String getEventName() {
        return "KeyMapNonLastValueRemoved";
    }
    
}
