package diskCacheV111.services.space.message;

import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.Message;

import static com.google.common.base.Preconditions.checkNotNull;

public class Use extends Message
{
    private static final long serialVersionUID = 7864026870745603985L;
    private final long spaceToken;
    private final String path;
    private final long sizeInBytes;
    private final long lifetime; //this is the lifetime of this file reservation
                                 // not file lifetime after it is written
                                 // this is how long user has to write the file
    private final boolean overwrite;
    private long fileId;

    public Use(
            long spaceToken,
            String path,
            long sizeInBytes,
            long lifetime,
            boolean overwrite){
        this.spaceToken = spaceToken;
        this.sizeInBytes = sizeInBytes;
        this.path = checkNotNull(path);
        this.lifetime = lifetime;
        this.overwrite = overwrite;
        setReplyRequired(true);
    }

    public long getSpaceToken() {
        return spaceToken;
    }

    public FsPath getPath() {
        return new FsPath(path);
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getLifetime() {
        return lifetime;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public boolean isOverwrite() {
        return overwrite;
    }
}
