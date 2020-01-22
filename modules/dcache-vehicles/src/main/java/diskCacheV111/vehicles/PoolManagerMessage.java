// $Id: PoolManagerMessage.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

//Base class for messages to PoolManager


public class PoolManagerMessage extends Message {

    private static final long serialVersionUID = 4607229352454456613L;

    public PoolManagerMessage(){
        super();
    }
    public PoolManagerMessage(boolean replyNeeded){
        super(replyNeeded);
    }

    /**
     * Returns {@code true} if this message of a given pnfsid must be sent to the
     * same instance of PoolManager.
     * @return {@code true} if PoolManager affinity is required, otherwise
     * {@code false}.
     */
    public boolean requiresAffinity() {
        /*
	 * By default, all messages prefer the message affinity.
	 * Subclasses my override to change the behaviour.
	 */
	return true;
    }
}



