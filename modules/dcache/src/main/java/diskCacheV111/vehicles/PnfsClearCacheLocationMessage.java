//$Id: PnfsClearCacheLocationMessage.java,v 1.6 2005-02-21 14:20:50 patrick Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PnfsClearCacheLocationMessage extends PnfsModifyCacheLocationMessage {

    private boolean _removeIfLast;

    private static final long serialVersionUID = 3605282936760879338L;

    public PnfsClearCacheLocationMessage(PnfsId pnfsId, String poolName){
	super(pnfsId,poolName);
    }
    public PnfsClearCacheLocationMessage(PnfsId pnfsId, String poolName , boolean removeIfLast ){
	super(pnfsId,poolName);
        _removeIfLast = removeIfLast ;
    }
    public boolean removeIfLast(){ return _removeIfLast ; }
    public String toString(){
        return super.toString()+";removing;"+(_removeIfLast?"removeIfLast;":"");
    }

}

