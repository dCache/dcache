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
public class KeyValueMapRemoved extends OneToManyMapEvent {
    
    private static final long serialVersionUID = -410827494513923881L;
    
    public KeyValueMapRemoved(Object source,Object key, Object value) 
    {
        super(source,"KeyValueMapRemoved",key,value);
    }
    
    @Override
    public String getEventName() {
        return "KeyValueMapRemoved";
    }
    
}
