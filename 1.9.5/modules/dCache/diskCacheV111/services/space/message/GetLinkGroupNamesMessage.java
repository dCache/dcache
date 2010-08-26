package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;

public class GetLinkGroupNamesMessage extends Message {
	static final long serialVersionUID=-6265306732546318691L;
      
	private String linkGroupNames[]=null;
	
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