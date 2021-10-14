package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsId;

public class PnfsDeleteEntryNotificationMessage extends PnfsMessage {

    private static final long serialVersionUID = -835476659990130630L;

    public PnfsDeleteEntryNotificationMessage(PnfsId pnfsId) {
        super(requireNonNull(pnfsId));
        setReplyRequired(true);
    }
}
