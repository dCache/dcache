// $Id: PoolModifyPersistencyMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PoolModifyPersistencyMessage extends PoolMessage {

    public PnfsId  _pnfsId;
    public boolean _precious  = true ;

    private static final long serialVersionUID = 2876195986537751420L;

    public PoolModifyPersistencyMessage(String poolName , PnfsId pnfsId , boolean precious ){
	super(poolName);
        setReplyRequired(true);
        _pnfsId   = pnfsId ;
        _precious = precious ;
    }
    public PnfsId getPnfsId(){ return _pnfsId ; }
    public boolean isPrecious(){ return _precious ; }
    public boolean isCached(){ return ! _precious ; }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}

