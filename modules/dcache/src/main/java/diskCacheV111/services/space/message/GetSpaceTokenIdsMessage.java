package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;

public class GetSpaceTokenIdsMessage extends Message {

	private static final long serialVersionUID = 2174849203532159155L;

	private long spaceTokenIds[];

	public GetSpaceTokenIdsMessage() {
		setReplyRequired(true);
	}

	public long[] getSpaceTokenIds() {
		return spaceTokenIds;
	}

	public void setSpaceTokenIds(long ids[]){
		this.spaceTokenIds=ids;
	}

}
