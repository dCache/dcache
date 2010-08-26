/*
 * BroadcastUnregisterMessage.java
 *
 * Created on February 1, 2005, 3:01 PM
 */

package dmg.cells.services.multicaster;
import  dmg.cells.nucleus.* ;
/**
 *
 * @author  patrick
 */
public class BroadcastUnregisterMessage extends BroadcastEventCommandMessage {
    
    private static final long serialVersionUID = 2465891234565157834L;  
    
    
    /** Creates a new instance of BroadcastCommandMessage */
    public BroadcastUnregisterMessage( String eventClass ) {
        super( eventClass ) ;
    }
    public BroadcastUnregisterMessage( String eventClass , CellPath target ) {
        super( eventClass , target ) ;
    }
    public String toString(){
       return "Unregister:"+super.toString();
    }
}
    
