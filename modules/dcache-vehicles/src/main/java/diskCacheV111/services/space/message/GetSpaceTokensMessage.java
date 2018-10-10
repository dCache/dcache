package diskCacheV111.services.space.message;

import java.util.Collection;
import java.util.Collections;

import diskCacheV111.services.space.Space;
import diskCacheV111.vehicles.Message;

public class GetSpaceTokensMessage extends Message {

	private static final long serialVersionUID = -419540669938740860L;

	private Collection<Space> list = Collections.emptySet();
	private final boolean isFileCountRequested;

	public GetSpaceTokensMessage() {
		setReplyRequired(true);
		isFileCountRequested = false;
	}

	public GetSpaceTokensMessage(boolean isFileCountRequested)
        {
		setReplyRequired(true);
		this.isFileCountRequested = isFileCountRequested;
	}

	public Collection<Space> getSpaceTokenSet() {
		return list;
	}

	public void setSpaceTokenSet(Collection<Space> list) {
		this.list = list;
	}

	public boolean isFileCountRequested() {
		return isFileCountRequested;
	}
}
