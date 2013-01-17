//______________________________________________________________________________
//
// Intended usage: specify requested space token id and get a set with one
//                 element back
//                 do not specify requested space token id and get the set
//                 containing all unexpired space tokens
//
//  $Id$
//  $Author$
//______________________________________________________________________________

package diskCacheV111.services.space.message;

import java.util.Set;
import diskCacheV111.vehicles.Message;
import diskCacheV111.services.space.Space;

public class GetSpaceTokensMessage extends Message {

	private static final long serialVersionUID = -419540669938740860L;
	private Long spacetokenId;

	private Set<Space> list;

	public void setSpaceTokenId(long id) {
		spacetokenId = id;
	}

	public void setSpaceTokenId(Long id) {
		spacetokenId = id;
	}

	public Long getSpaceTokenId() {
		return spacetokenId;
	}

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
