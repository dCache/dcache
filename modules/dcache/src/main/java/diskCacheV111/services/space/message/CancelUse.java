package diskCacheV111.services.space.message;

import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.Message;

import static com.google.common.base.Preconditions.checkNotNull;

public class CancelUse extends Message
{
    private static final long serialVersionUID = 1530375623803317300L;
    private final long spaceToken;
    private final String path;

    public CancelUse(long spaceToken, String path)
    {
        this.spaceToken = spaceToken;
        this.path = checkNotNull(path);
        setReplyRequired(true);
    }

    public long getSpaceToken()
    {
        return spaceToken;
    }

    public FsPath getPath()
    {
        return new FsPath(path);
    }
}
