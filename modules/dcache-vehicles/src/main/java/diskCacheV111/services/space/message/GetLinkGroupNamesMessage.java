package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;
import java.util.Collection;

public class GetLinkGroupNamesMessage extends Message {

    private static final long serialVersionUID = -6265306732546318691L;

    private Collection<String> linkGroupNames;

    public GetLinkGroupNamesMessage() {
        setReplyRequired(true);
    }

    public Collection<String> getLinkGroupNames() {
        return linkGroupNames;
    }

    public void setLinkGroupNames(Collection<String> names) {
        this.linkGroupNames = names;
    }

}
