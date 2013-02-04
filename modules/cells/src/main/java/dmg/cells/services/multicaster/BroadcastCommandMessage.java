/*
 * BroadcastCommandMessage.java
 *
 * Created on February 1, 2005, 2:57 PM
 */

package dmg.cells.services.multicaster;

import java.io.Serializable;

/**
 *
 * @author  patrick
 */
public class BroadcastCommandMessage implements Serializable {
    
    private static final long serialVersionUID = 2469891234565157834L;  
    private Object _returnObject;
    private int    _returnCode;
    /** Creates a new instance of BroadcastCommandMessage */
    public BroadcastCommandMessage( ) {
        
    }
    public void setReturnValues( int code , Serializable obj ){
       _returnObject = obj ;
       _returnCode   = code ;
    }
    public int getReturnCode(){ return _returnCode ; }
    public Object getReturnObject(){ return _returnObject ; }
    public String toString(){ 
        if( _returnCode == 0 ) {
            return "{OK}";
        } else {
            return "{" + _returnCode + ";" + (_returnObject == null ? "" : _returnObject
                    .toString()) + "}";
        }
    }
}
