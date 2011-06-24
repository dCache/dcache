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

}



