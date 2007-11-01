// $Id: PnfsSetLengthMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;
public class PnfsSetLengthMessage extends PnfsMessage {
    long _length;

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
