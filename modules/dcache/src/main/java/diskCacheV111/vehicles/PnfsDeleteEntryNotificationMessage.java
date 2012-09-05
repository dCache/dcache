// $Id: PnfsDeleteEntryMessage.java,v 1.3 2005-01-14 13:56:25 tigran Exp $

package diskCacheV111.vehicles;
import diskCacheV111.util.PnfsId;

public class PnfsDeleteEntryNotificationMessage extends PnfsMessage {
    private static final long serialVersionUID = -835476659990130630L;

    private String _path;


    public PnfsDeleteEntryNotificationMessage(PnfsId pnfsId, String path){
	super(pnfsId);
	this._path = path;
	setReplyRequired(false);
    }


    public String getPath(){
	return _path;
    }
}
