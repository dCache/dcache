//______________________________________________________________________________
//
// Intended usage: specify requested linkgroup id and get a set with one 
//                 element back 
//                 do not specify requested linkgroup id and get the set 
//                 containing all linkgroups
//
//  $Id$
//  $Author$
//______________________________________________________________________________
package diskCacheV111.services.space.message;

import java.util.Set;
import diskCacheV111.vehicles.Message;
import diskCacheV111.services.space.LinkGroup;

public class GetLinkGroupsMessage extends Message {

	static final long serialVersionUID = 2889995137324365133L;
	private Long linkGroupId=null;

	private Set<LinkGroup> list;

	public void setLinkGroupidI(long id) { 
		linkGroupId = new Long(id);
	}

	public void setLinkGroupId(Long id) { 
		linkGroupId = id;
	}

	public Long getLinkgroupidId() { 
		return linkGroupId;
	}


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