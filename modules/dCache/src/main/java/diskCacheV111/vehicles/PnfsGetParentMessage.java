package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PnfsGetParentMessage extends PnfsMessage
{
    private PnfsId _parent;

    public PnfsGetParentMessage(PnfsId pnfsId)
    {
        super(pnfsId);
        setReplyRequired(true);
    }

    public void setParent(PnfsId parent)
    {
        _parent = parent;
    }

    public PnfsId getParent()
    {
        return _parent;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
