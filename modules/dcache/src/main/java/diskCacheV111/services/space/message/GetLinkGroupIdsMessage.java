//______________________________________________________________________________
//
// Intended usage: send this message to space manager to get a list of link group
//                 ids
//
//  $Id$
//  $Author$
//______________________________________________________________________________

package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;

public class GetLinkGroupIdsMessage extends Message {

	static final long serialVersionUID = 1794764314342568402L;

	private long linkGroupIds[]=null;

	public GetLinkGroupIdsMessage() {
		setReplyRequired(true);
	}

	public long[] getLinkGroupIds() {
		return linkGroupIds;
	}

	public void setLinkGroupIds(long ids[]){
		this.linkGroupIds=ids;
	}

}