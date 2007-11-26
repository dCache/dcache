// $Id: DoorMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

//Base class for messages to Door


public class DoorMessage extends Message {
    
    private String _pnfsId;
    private boolean _isPut;

    private static final long serialVersionUID = 3289927811996665837L;
    
    public DoorMessage(String pnfsId, boolean isPut){
	_pnfsId = pnfsId;
	_isPut = isPut;
    }
    
    public String getPnfsId(){
	return _pnfsId;
    }
    
    public boolean isPut(){
	return _isPut;
    }

    public boolean isGet(){
	return !_isPut;
    }
}


    
