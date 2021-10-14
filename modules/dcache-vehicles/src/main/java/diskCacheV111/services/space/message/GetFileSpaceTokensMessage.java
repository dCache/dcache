package diskCacheV111.services.space.message;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

public class GetFileSpaceTokensMessage extends Message {

    private static final long serialVersionUID = 8671820912506234154L;
    private final PnfsId pnfsId;
    private long[] spaceTokens;

    public GetFileSpaceTokensMessage(PnfsId pnfsId) {
        this.pnfsId = requireNonNull(pnfsId);
        setReplyRequired(true);
    }

    public long[] getSpaceTokens() {
        return spaceTokens;
    }

    public void setSpaceToken(long[] spaceTokens) {
        this.spaceTokens = spaceTokens;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }
}
