// $Id: PoolCheckFileMessage.java,v 1.6 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PoolCheckFileMessage extends PoolCheckMessage implements
        PoolFileCheckable {

    private PnfsId _pnfsId;
    private boolean _have;
    private boolean _waiting;

    private static final long serialVersionUID = -642190491441448681L;

    public PoolCheckFileMessage(String poolName, PnfsId pnfsId) {
        super(poolName);
        _pnfsId = pnfsId;
        setReplyRequired(true);
    }

    public String toString() {
        return super.toString() + ";PnfsId="
                + (_pnfsId == null ? "<unknown>" : _pnfsId.toString())
                + ";have=" + _have + ";waiting=" + _waiting;
    }

    public void setPnfsId(PnfsId pnfsId) {
        _pnfsId = pnfsId;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public boolean getHave() {
        return _have;
    }

    public void setHave(boolean have) {
        _have = have;
    }

    public boolean getWaiting() {
        return _waiting;
    }

    public void setWaiting(boolean waiting) {
        _waiting = waiting;
    }
}
