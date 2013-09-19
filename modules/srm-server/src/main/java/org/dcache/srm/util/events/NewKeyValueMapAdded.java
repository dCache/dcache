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
public class NewKeyValueMapAdded extends OneToManyMapEvent {
    
    private static final long serialVersionUID = 5777422122732408054L;
    
    public NewKeyValueMapAdded(Object source,Object key, Object value) 
    {
        super(source,"NewKeyValueMapAdded",key,value);
    }
    
    
    @Override
    public String getEventName() {
        return "NewKeyValueMapAdded";
    }
    
}
