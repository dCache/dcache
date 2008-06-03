package org.dcache.pool;

public class FaultEvent
{
    private final String _source;
    private final FaultAction _action;
    private final String _message;
    private final Throwable _cause;

    public FaultEvent(String source, FaultAction action,
                      String message, Throwable cause)
    {
        _source = source;
        _action = action;
        _message = message;
        _cause = cause;
    }

    public String getSource()
    {
        return _source;
    }

    public FaultAction getAction()
    {
        return _action;
    }

    public String getMessage()
    {
        return _message;
    }

    public Throwable getCause()
    {
        return _cause;
    }
}