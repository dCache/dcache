//$Id: PnfsModifyCacheLocationMessage.java,v 1.3 2005-02-21 14:20:50 patrick Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PnfsModifyCacheLocationMessage extends PnfsMessage {

    private String _poolName;

    private static final long serialVersionUID = -7996549495498661141L;

    public PnfsModifyCacheLocationMessage(PnfsId pnfsId, String poolName){
	super(pnfsId);
	_poolName = poolName;
    }

    public String getPoolName(){
	return _poolName;
    }
    public String toString(){
        return super.toString()+";Pool="+_poolName;
    }
}

