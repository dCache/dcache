package dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class KillEvent extends CellEvent {
    long _timeout;
    public KillEvent( CellPath source , long timeout ){
        super( source , CellEvent.OTHER_EVENT ) ;
        _timeout = timeout ;
    }
    public CellPath getKiller(){ return (CellPath)getSource() ; }
    public long getTimeout(){ return _timeout ; }
    public String toString(){
      return "KillEvent(source=" + getSource() +
             ";timeout=" + _timeout + ')';
    }
}
