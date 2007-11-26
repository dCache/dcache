// $Id: DoorTransferFailedMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

//Base class for messages to Door

public class DoorTransferFailedMessage extends DoorMessage {
    private String _reason;
    private String _path;

    private static final long serialVersionUID = 9197939373322775259L;
    
    public DoorTransferFailedMessage(String pnfsId, String path, String reason, boolean isPut){
	super(pnfsId,isPut);
	_path = path;
	_reason = reason;
    }

    public String getReason(){
	return _reason;
    }

    public String getPath(){
	return _path;
    }
}


    
