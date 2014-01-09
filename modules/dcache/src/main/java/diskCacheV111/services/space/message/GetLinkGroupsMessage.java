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

import java.util.Collection;
import java.util.Collections;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.vehicles.Message;

public class GetLinkGroupsMessage extends Message {

	private static final long serialVersionUID = 2889995137324365133L;

	private Collection<LinkGroup> list = Collections.emptySet();

	public GetLinkGroupsMessage() {
		setReplyRequired(true);
	}

	public Collection<LinkGroup> getLinkGroups() {
		return list;
	}

	public void setLinkGroups(Collection<LinkGroup> lglist) {
		this.list=lglist;
	}

}
