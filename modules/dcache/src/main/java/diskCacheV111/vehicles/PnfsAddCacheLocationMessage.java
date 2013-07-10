//$Id: PnfsAddCacheLocationMessage.java,v 1.5 2005-02-21 14:20:50 patrick Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PnfsAddCacheLocationMessage extends PnfsModifyCacheLocationMessage {

    private static final long serialVersionUID = 4683846056284598394L;

    public PnfsAddCacheLocationMessage(PnfsId pnfsId, String poolName){
	super(pnfsId,poolName);
    }
    public String toString(){
        return super.toString()+";adding;";
    }
}

