package dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class LastMessageEvent extends MessageEvent {

    public LastMessageEvent(){ 
       super(null);
    }
    public String toString(){
      return "LastMessageEvent(source=CellGlue)" ;
    }


}
