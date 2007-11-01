// $Id: DoorTransferSucceededMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

//Base class for messages to Door

public class DoorTransferSucceededMessage extends DoorMessage {
    private long _length;

    private static final long serialVersionUID = 5928611369019938956L;
    
    public DoorTransferSucceededMessage(String pnfsId, long length, boolean isPut){
	super(pnfsId,isPut);
	_length = length;
    }
    
    public long getLength(){
	return _length;
    }
}


    
