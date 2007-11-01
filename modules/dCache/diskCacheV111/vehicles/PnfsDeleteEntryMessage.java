// $Id: PnfsDeleteEntryMessage.java,v 1.3 2005-01-14 13:56:25 tigran Exp $

package diskCacheV111.vehicles;
import diskCacheV111.util.PnfsId;

public class PnfsDeleteEntryMessage extends PnfsMessage {
    
    private String _path = null;
    
    private static final long serialVersionUID = 7375207858020099787L;
    
    public PnfsDeleteEntryMessage(String path){
	_path = path;
	setReplyRequired(false);
    }

    public PnfsDeleteEntryMessage(PnfsId pnfsId){
	super(pnfsId);
	setReplyRequired(false);
    }
    
    public String getPath(){
	return _path;
    }
}
