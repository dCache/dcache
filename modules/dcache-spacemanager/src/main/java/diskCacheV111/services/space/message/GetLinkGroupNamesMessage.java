package diskCacheV111.services.space.message;

import java.util.Collection;

import diskCacheV111.vehicles.Message;

public class GetLinkGroupNamesMessage extends Message {
	private static final long serialVersionUID=-6265306732546318691L;

	private Collection<String> linkGroupNames;

	public GetLinkGroupNamesMessage() {
		setReplyRequired(true);
	}

	public Collection<String> getLinkGroupNames() {
		return linkGroupNames;
	}

	public void setLinkGroupNames(Collection<String> names){
		this.linkGroupNames = names;
	}

}
