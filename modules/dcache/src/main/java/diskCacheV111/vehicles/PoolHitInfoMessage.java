// $Id: PoolHitInfoMessage.java,v 1.3 2006-04-06 23:26:47 podstvkv Exp $
package diskCacheV111.vehicles ;

import diskCacheV111.util.PnfsId;

public class PoolHitInfoMessage extends PnfsFileInfoMessage {

    private ProtocolInfo _protocolInfo;
    private boolean      _fileCached;

    private static final long serialVersionUID = -1487408937648228544L;

    public PoolHitInfoMessage(String cellName, PnfsId pnfsId)
    {
		super("hit", "pool", cellName, pnfsId);
    }

    public void setFileCached(boolean cached)
    {
		_fileCached = cached;
    }

    public void setProtocolInfo(ProtocolInfo protocolInfo)
    {
		_protocolInfo = protocolInfo;
    }

    public boolean getFileCached()
    {
		return _fileCached;
    }

    public ProtocolInfo getProtocolInfo()
    {
		return _protocolInfo;
    }

    public String toString()
    {
		return getInfoHeader()+" "+
			getFileInfo()+" "+
			_fileCached+" {"+
			_protocolInfo+"} "+
			getResult() ;
    }
}
