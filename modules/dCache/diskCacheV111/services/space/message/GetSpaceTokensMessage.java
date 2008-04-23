package diskCacheV111.services.space.message;

import java.util.Set;
import diskCacheV111.vehicles.Message;
import diskCacheV111.services.space.Space;

public class GetSpaceTokensMessage extends Message {

	static final long serialVersionUID = -419540669938740860L;

	private Set<Space> list;

	public GetSpaceTokensMessage() { 
		setReplyRequired(true);
	}

	public Set<Space> getSpaceTokenSet() {
		return list;
	}
	
	public void setSpaceTokenSet(Set<Space> lglist) { 
		this.list=lglist;
	}

}  