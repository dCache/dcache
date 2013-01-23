/*
 * BroadcastRegisterMesssage.java
 *
 * Created on February 1, 2005, 3:00 PM
 */

package dmg.cells.services.multicaster;

import dmg.cells.nucleus.* ;

/**
 *
 * @author  patrick
 */
public class BroadcastRegisterMessage extends BroadcastEventCommandMessage {
    private static final long serialVersionUID = 246989178965157834L;  
    private long    _expires         = -1L ;
    private boolean _cancelOnFailure;
    /** Creates a new instance of BroadcastUnregisterMessage */
    public BroadcastRegisterMessage( String eventClass ) {
       super( eventClass ) ;
    }
    public BroadcastRegisterMessage( String eventClass , CellPath target ) {
       super( eventClass , target ) ;
    }
    public void setCancelOnFailure( boolean cancel ){ _cancelOnFailure = cancel ; }
    public void setExpires( long expires ){ _expires = expires ; }
    public boolean isCancelOnFailure(){ return _cancelOnFailure ; }
    public long    getExpires(){ return _expires ; }
    public String  toString(){
        return "Register(C="+_cancelOnFailure+";"+_expires+"):"+super.toString();
    }
}
