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

import java.util.Collection;
import java.util.Collections;

import diskCacheV111.services.space.Space;
import diskCacheV111.vehicles.Message;

public class GetSpaceTokensMessage extends Message {

	private static final long serialVersionUID = -419540669938740860L;
	private Long spacetokenId;

	private Collection<Space> list = Collections.emptySet();

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

	public Collection<Space> getSpaceTokenSet() {
		return list;
	}

	public void setSpaceTokenSet(Collection<Space> list) {
		this.list = list;
	}

}
