// $Id: PoolMessage.java,v 1.3 2005-02-21 14:39:14 patrick Exp $

package diskCacheV111.vehicles;

//Base class for messages to Pool


public class PoolMessage extends Message {

    private final String _poolName;

    private static final long serialVersionUID = 2092239799703859605L;

    public PoolMessage(String poolName){
	_poolName = poolName;
    }

    public String getPoolName(){
	return _poolName;
    }
    public String toString(){
        return "Pool="+_poolName ;
    }
}



