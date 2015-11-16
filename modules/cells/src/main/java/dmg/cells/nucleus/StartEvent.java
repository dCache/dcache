package dmg.cells.nucleus;

public class StartEvent extends CellEvent
{
    private final long _timeout;

    public StartEvent(CellPath source, long timeout)
    {
        super(source, CellEvent.OTHER_EVENT);
        _timeout = timeout;
    }

    public long getTimeout()
    {
        return _timeout;
    }

    public String toString()
    {
        return "StartEvent(source=" + getSource() + ";timeout=" + _timeout + ")";
    }
}
