package dmg.cells.nucleus ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class KillEvent extends CellEvent {
    private final long _timeout;
    private final String _target;

    public KillEvent(String target, CellPath source, long timeout)
    {
        super( source , CellEvent.OTHER_EVENT ) ;
        _timeout = timeout ;
        _target = target;
    }

    public String getTarget()
    {
        return _target;
    }

    public CellPath getKiller(){ return (CellPath)getSource() ; }
    public long getTimeout(){ return _timeout ; }
    public String toString(){
      return "KillEvent(source=" + getSource() +
             ";timeout=" + _timeout + ')';
    }
}
