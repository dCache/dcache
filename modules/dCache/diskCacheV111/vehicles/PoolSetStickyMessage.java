// $Id: PoolSetStickyMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

import  diskCacheV111.util.PnfsId ;

public class PoolSetStickyMessage extends PoolMessage {
    
    public PnfsId  _pnfsId  = null ;
    public boolean _sticky  = true ;
    
    private static final long serialVersionUID = -7816096827797365873L;
    
    public PoolSetStickyMessage(String poolName , PnfsId pnfsId , boolean sticky ){
	super(poolName);
        setReplyRequired(true);
        _pnfsId = pnfsId ;
        _sticky = sticky ;
    }
    public PnfsId getPnfsId(){ return _pnfsId ; }
    public boolean isSticky(){ return _sticky ; }
}


