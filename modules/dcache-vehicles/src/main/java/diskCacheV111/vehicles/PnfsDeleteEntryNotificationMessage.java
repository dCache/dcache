package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import static com.google.common.base.Preconditions.checkNotNull;

public class PnfsDeleteEntryNotificationMessage extends PnfsMessage
{
    private static final long serialVersionUID = -835476659990130630L;

    public PnfsDeleteEntryNotificationMessage(PnfsId pnfsId)
    {
        super(checkNotNull(pnfsId));
        setReplyRequired(true);
    }
}
