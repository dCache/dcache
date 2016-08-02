package dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ExceptionEvent extends CellEvent {
    Exception _exception ;
    public ExceptionEvent( CellPath source , Exception exception ){
        super( source , CellEvent.OTHER_EVENT ) ;
        _exception = exception ;
    }
    public CellPath getSender(){ return (CellPath)getSource() ; }
    public Exception   getException(){ return _exception ; }
    public String toString(){
      return "ExceptionEvent(source=" + getSource() +
             ";Exception=" + _exception + ')';
    }
}
 
