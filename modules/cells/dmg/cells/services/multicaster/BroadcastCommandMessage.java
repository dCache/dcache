/*
 * BroadcastCommandMessage.java
 *
 * Created on February 1, 2005, 2:57 PM
 */

package dmg.cells.services.multicaster;

/**
 *
 * @author  patrick
 */
public class BroadcastCommandMessage implements java.io.Serializable {
    
    private static final long serialVersionUID = 2469891234565157834L;  
    private Object _returnObject = null ;
    private int    _returnCode   = 0 ;
    /** Creates a new instance of BroadcastCommandMessage */
    public BroadcastCommandMessage( ) {
        
    }
    public void setReturnValues( int code , Object obj ){
       _returnObject = obj ;
       _returnCode   = code ;
    }
    public int getReturnCode(){ return _returnCode ; }
    public Object getReturnObject(){ return _returnObject ; }
    public String toString(){ 
        if( _returnCode == 0 )
            return "{OK}";
        else
            return "{"+_returnCode+";"+(_returnObject==null?"":_returnObject.toString())+"}";
    }
}
