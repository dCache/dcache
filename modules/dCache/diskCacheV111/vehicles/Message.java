// $Id: Message.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

// Base class for all Messages

public class Message  implements java.io.Serializable {

    private boolean _replyRequired = false;
    private boolean _isReply       = false;
    private int     _returnCode    = 0;
    private Object  _errorObject   = null;
    private long    _id            = 0 ;

    private static final long serialVersionUID = 2056896713066252504L;

    public Message(){
    }

    public Message(boolean replyRequired){
	_replyRequired = replyRequired;
    }
    @Override
    public String toString(){
        return _returnCode==0?"":"("+_returnCode+")="+_errorObject ;
    }
    public void setSucceeded(){
	setReply(0,null);
    }

    public void setFailed(int errorCode, Object errorObject){
	setReply(errorCode, errorObject);
    }
    public void setReply(){
        _isReply = true ;
    }
    public void setReply(int returnCode, Object errorObject){
	_isReply     = true;
	_returnCode  = returnCode;
	_errorObject = errorObject;
    }

    public boolean isReply(){
	return _isReply;
    }

    public void clearReply(){
	//allows us to reuse message objects
	_isReply     = false;
	_returnCode  = 0;
	_errorObject = null;
    }

    public int getReturnCode(){
	return _returnCode;
    }

    public Object getErrorObject(){
	return _errorObject;
    }

    public boolean getReplyRequired(){
	return _replyRequired;
    }

    public void setReplyRequired(boolean replyRequired){
	_replyRequired = replyRequired;
    }
    public void setId( long id ){ _id = id ; }
    public long getId(){ return _id ; }
}

