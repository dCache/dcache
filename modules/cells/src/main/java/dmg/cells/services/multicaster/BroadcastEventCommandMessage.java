/*
 * BroadcastEventCommandMessage.java
 *
 * Created on February 1, 2005, 4:57 PM
 */

package dmg.cells.services.multicaster;

import dmg.cells.nucleus.CellPath;
/**
 *
 * @author  patrick
 */
public class BroadcastEventCommandMessage extends BroadcastCommandMessage {

    private static final long serialVersionUID = 2465891234568767834L;

    private String   _eventClass;
    private CellPath _target;

    /** Creates a new instance of BroadcastEventCommandMessage */
    public BroadcastEventCommandMessage( String eventClass ){
        this(eventClass,null ) ;
    }
    public BroadcastEventCommandMessage(String eventClass , CellPath target ) {
        _eventClass = eventClass ;
        _target = target != null ? target.clone() : null ;
    }
    public CellPath getTarget(){
        return _target == null ? null : _target.clone();
    }
    public String getEventClass(){ return _eventClass ; }
    public String toString(){
       return _eventClass+(_target==null?"<Dynamic>":_target.toString())+super.toString();
    }


}
