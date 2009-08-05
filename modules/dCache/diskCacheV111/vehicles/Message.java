// $Id: Message.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import dmg.cells.nucleus.HasDiagnosticContext;
import org.dcache.util.ReflectionUtils;
import diskCacheV111.util.PnfsId;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;

// Base class for all Messages

public class Message
    implements java.io.Serializable,
               HasDiagnosticContext
{
    private boolean _replyRequired = false;
    private boolean _isReply       = false;
    private int     _returnCode    = 0;
    private Object  _errorObject   = null;
    private long    _id            = 0 ;
    private Subject _subject;

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

    public void setSubject(Subject subject)
    {
        _subject = subject;
    }

    public Subject getSubject()
    {
        return (_subject == null) ? Subjects.ROOT : _subject;
    }

    /**
     * Returns a human readable name of the message class. By default
     * this is the short class name with the "Message" or "Msg" suffix
     * removed.
     */
    public String getMessageName()
    {
        String name = getClass().getSimpleName();
        int length = name.length();
        if ((length > 7) && name.endsWith("Message")) {
            name = name.substring(0, name.length() - 7);
        } else if ((length > 3) && name.endsWith("Msg")) {
            name = name.substring(0, name.length() - 3);
        }

        return name;
    }

    public String getDiagnosticContext()
    {
        String name = getMessageName();
        PnfsId id = ReflectionUtils.getPnfsId(this);
        return (id == null) ? name : (name + " " + id);
    }

    /**
     * Returns true if this message could possibly change the effect
     * or result of <code>message</code>.
     *
     * In a message queue, two idempotent operations can be folded if
     * they are not invalidated by any messages in between.
     */
    public boolean invalidates(Message message)
    {
        return true;
    }

    /**
     * Returns true if this message is subsumed by the message
     * provided as an argument.
     *
     * A message A subsumes a message B if and only if all of the
     * following conditions are satisfied:
     *
     * - both A and B are idempotent,
     * - a reply to A can be used as a valid reply to B,
     * - executing B after A does not change the state of the system.
     */
    public boolean isSubsumedBy(Message message)
    {
        return false;
    }

    /**
     * Returns true if this message is idempotent, that is, the result
     * of applying two identical instances of this message is the
     * same, assuming the state of the system has not changed in
     * between.
     */
    public boolean isIdempotent()
    {
        return false;
    }
}

