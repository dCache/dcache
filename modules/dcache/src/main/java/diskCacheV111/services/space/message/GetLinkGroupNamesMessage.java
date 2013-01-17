package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;

public class GetLinkGroupNamesMessage extends Message {
	private static final long serialVersionUID=-6265306732546318691L;

	private String linkGroupNames[];

	public GetLinkGroupNamesMessage() {
		setReplyRequired(true);
	}

	public String[] getLinkGroupNames() {
		return linkGroupNames;
	}

	public void setLinkGroupNames(String names[]){
		this.linkGroupNames=names;
	}

}
