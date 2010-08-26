// $Id: PnfsSetLengthMessage.java,v 1.5 2007-05-24 13:51:05 tigran Exp $

package diskCacheV111.vehicles;
import diskCacheV111.util.PnfsId;

public class PnfsSetLengthMessage extends PnfsMessage {

    private final long _length;

    private static final long serialVersionUID = -7881380742681787681L;

    public PnfsSetLengthMessage(PnfsId pnfsId, long length){
	super(pnfsId);
	_length = length;
	setReplyRequired(true);
    }

    public long getLength(){
	return _length;
    }
}
