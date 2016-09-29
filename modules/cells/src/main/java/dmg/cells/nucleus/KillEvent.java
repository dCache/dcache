package dmg.cells.nucleus ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class KillEvent extends CellEvent {
    private final String _target;

    public KillEvent(String target, CellPath source)
    {
        super( source , CellEvent.OTHER_EVENT ) ;
        _target = target;
    }

    public String getTarget()
    {
        return _target;
    }

    public CellPath getKiller(){ return (CellPath)getSource() ; }
    public String toString(){
      return "KillEvent(source=" + getSource() + ')';
    }
}
