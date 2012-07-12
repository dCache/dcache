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
public class ExistingKeyValueMapAdded extends OneToManyMapEvent {
    
    private static final long serialVersionUID = 7787714419135491361L;
    
    public ExistingKeyValueMapAdded(Object source,Object key, Object value) 
    {
        super(source,"ExistingKeyValueMapAdded",key,value);
    }
    
    @Override
    public String getEventName() {
        return "ExistingKeyValueMapAdded";
    }
    
}
