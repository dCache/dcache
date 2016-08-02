// $Id: PoolSetStickyMessage.java,v 1.5 2007-08-13 20:50:48 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PoolSetStickyMessage extends PoolMessage {

    private final PnfsId  _pnfsId;
    private final boolean _sticky;
    private final String _owner;
    private final long _validTill;

    private static final long serialVersionUID = -7816096827797365873L;

    /**
     *
     * @param poolName
     * @param pnfsId
	 * @param sticky
	 * @param owner flag owner
	 * @param validTill time milliseconds since 00:00:00 1 Jan. 1970.
     */
    public PoolSetStickyMessage(String poolName , PnfsId pnfsId , boolean sticky , String owner, long validTill){
    	super(poolName);
        setReplyRequired(true);
        _pnfsId = pnfsId ;
        _sticky = sticky ;
        _owner = owner;
        _validTill = validTill;
    }


    public PoolSetStickyMessage(String poolName , PnfsId pnfsId , boolean sticky ){
    	this(poolName, pnfsId, sticky, "system", -1);
    }
    public PnfsId getPnfsId(){ return _pnfsId ; }
    public boolean isSticky(){ return _sticky ; }
    public String getOwner() { return _owner; }
    public long getLifeTime() {return _validTill; }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }

}

