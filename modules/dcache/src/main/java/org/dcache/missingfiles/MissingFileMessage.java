package org.dcache.missingfiles;

import diskCacheV111.vehicles.Message;

/**
 * A message to notify that a user requested a file that is currently not
 * present in dCache.  The return message includes a recommended action for
 * the door.
 */
public class MissingFileMessage extends Message
{
    private static final long serialVersionUID = 1L;

    private final String _requestedPath;
    private final String _internalPath;
    private Action _action = Action.FAIL;

    public MissingFileMessage(String path)
    {
        this(path, path);
    }

    public MissingFileMessage(String requestedPath, String internalPath)
    {
        _requestedPath = requestedPath;
        _internalPath = internalPath;
    }

    public String getRequestedPath()
    {
        return _requestedPath;
    }

    public String getInternalPath()
    {
        return _internalPath;
    }

    public void setAction(Action action)
    {
        _action = action;
    }

    public Action getAction()
    {
        return _action;
    }
}
