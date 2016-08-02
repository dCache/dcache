package dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RoutedMessageEvent extends MessageEvent {

    public RoutedMessageEvent( CellMessage msg ){ super( msg ) ; } 
    public String toString(){
      return "RoutedMessageEvent(source=" + getSource() + ')';
    }
}
 
