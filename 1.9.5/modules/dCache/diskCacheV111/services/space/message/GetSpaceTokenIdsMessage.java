package diskCacheV111.services.space.message;

import java.util.Set;
import diskCacheV111.vehicles.Message;
import diskCacheV111.services.space.Space;

public class GetSpaceTokenIdsMessage extends Message {

	static final long serialVersionUID = 2174849203532159155L;

	private long spaceTokenIds[]=null;
	
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