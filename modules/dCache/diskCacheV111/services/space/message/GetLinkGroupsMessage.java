package diskCacheV111.services.space.message;

import java.util.Set;
import diskCacheV111.vehicles.Message;
import diskCacheV111.services.space.LinkGroup;

public class GetLinkGroupsMessage extends Message {

	static final long serialVersionUID = 2889995137324365133L;

	private Set<LinkGroup> list;

	public GetLinkGroupsMessage() { 
		setReplyRequired(true);
	}

	public Set<LinkGroup> getLinkGroupSet() {
		return list;
	}
	
	public void setLinkGroupSet(Set<LinkGroup> lglist) { 
		this.list=lglist;
	}

}  