package dmg.cells.nucleus ;
  /**
    *   The message event is the vehicle which is used
    *   to deliver a message to a cell.
 
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
 
    */
public class MessageEvent extends CellEvent {
    /**
      *  The MessageEvent constructor creates a MessageEvent 
      *  containing the specified CellMessage.
      */
    public MessageEvent( CellMessage msg ){
        super( msg , CellEvent.OTHER_EVENT ) ;
        return ;
    } 
    /**
      *  getMessage extracts the CellMessage out of the
      *  MessageEvent.
      */
    public CellMessage getMessage(){
       return (CellMessage) getSource() ;
    }
    public String toString(){
      return "MessageEvent(source="+getSource().toString()+")" ;
    }
}
