package diskCacheV111.services.space;

import com.google.common.base.Function;

import java.io.Serializable;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

public class File implements Serializable {
        private static final long serialVersionUID = 1231338433325990419L;
        private long id;
	private String voGroup;
	private String voRole;
	private long spaceId;
	private long sizeInBytes;
    private long creationTime;
	private Long expirationTime;
	private FsPath path;
	private PnfsId pnfsId;
	private FileState state;
	private boolean isDeleted;

	public File(
		long id,
		String voGroup,
		String voRole,
		long spaceId,
		long sizeInBytes,
		long creationTime,
		Long expirationTime,
		FsPath path,
		PnfsId pnfsId,
		FileState state,
		boolean isDeleted
		) {
		this.id = id;
		this.voGroup = voGroup;
		this.voRole = voRole;
		this.spaceId = spaceId;
		this.sizeInBytes = sizeInBytes;
		this.creationTime = creationTime;
		this.expirationTime = expirationTime;
		this.path = path;
		this.pnfsId = pnfsId;
		this.state = state;
		this.isDeleted = isDeleted;
	}

	public FileState getState() {
		return state;
	}

	public void setState(FileState state) {
		this.state = state;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}


	public long getSpaceId() {
		return spaceId;
	}

	public void setSpaceId(long spaceId) {
		this.spaceId = spaceId;
	}

	public long getSizeInBytes() {
		return sizeInBytes;
	}

	public void setSizeInBytes(long sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public Long getExpirationTime() {
		return expirationTime;
	}

	public void setExpirationTime(Long expirationTime) {
		this.expirationTime = expirationTime;
	}

	public FsPath getPath() {
		return path;
	}

	public void setPath(FsPath path) {
		this.path = path;
	}

	public PnfsId getPnfsId() {
		return pnfsId;
	}

	public void setPnfsId(PnfsId pnfsId) {
		this.pnfsId = pnfsId;
	}
	public String toString() {
		return ""+ id+" "+
			voGroup+" "+
			voRole+" "+
			spaceId+" "+
			sizeInBytes+" "+
			creationTime+" "+
			expirationTime+" "+
                path +" "+
			pnfsId+" "+
			state+" "+
                isDeleted +" ";

	}

	public String getVoGroup() {
		return voGroup;
	}

	public void setVoGroup(String voGroup) {
		this.voGroup = voGroup;
	}

	public String getVoRole() {
		return voRole;
	}

	public void setVoRole(String voRole) {
		this.voRole = voRole;
	}

	public void setDeleted(boolean value) {
		this.isDeleted = value;
	}

	public boolean isDeleted() {
		return this.isDeleted;
	}

    public boolean isExpired()
    {
        return (state == FileState.ALLOCATED || state == FileState.TRANSFERRING) && expirationTime != null && expirationTime < System.currentTimeMillis();
    }

    public static Function<File, Long> getSpaceToken =
            new Function<File, Long>()
            {
                @Override
                public Long apply(File file)
                {
                    return file.getSpaceId();
                }
            };
}
