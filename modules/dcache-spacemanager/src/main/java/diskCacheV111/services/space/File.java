package diskCacheV111.services.space;

import com.google.common.base.Function;

import java.io.Serializable;

import diskCacheV111.util.PnfsId;

public class File implements Serializable {
        private static final long serialVersionUID = 1231338433325990419L;
        private long id;
	private String voGroup;
	private String voRole;
	private final long spaceId;
	private long sizeInBytes;
    private long creationTime;
	private PnfsId pnfsId;
	private FileState state;

	public File(
		long id,
		String voGroup,
		String voRole,
		long spaceId,
		long sizeInBytes,
		long creationTime,
		PnfsId pnfsId,
		FileState state
		) {
		this.id = id;
		this.voGroup = voGroup;
		this.voRole = voRole;
		this.spaceId = spaceId;
		this.sizeInBytes = sizeInBytes;
		this.creationTime = creationTime;
		this.pnfsId = pnfsId;
		this.state = state;
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
			pnfsId+" "+
			state;
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
