// $Id: PoolHitInfoMessage.java,v 1.3 2006-04-06 23:26:47 podstvkv Exp $
package diskCacheV111.vehicles ;

import org.stringtemplate.v4.ST;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

public class PoolHitInfoMessage extends PnfsFileInfoMessage {

    private ProtocolInfo _protocolInfo;
    private boolean      _fileCached;

    private static final long serialVersionUID = -1487408937648228544L;
    private String _transferPath;

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

    public String getTransferPath()
    {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path)
    {
        _transferPath = path;
    }

    public void setTransferPath(FsPath path)
    {
        setTransferPath(path.toString());
    }

    public String toString()
    {
		return getInfoHeader()+" "+
			getFileInfo()+" "+
			_fileCached+" {"+
			_protocolInfo+"} "+
			getResult() ;
    }

    @Override
    public void fillTemplate(ST template)
    {
        super.fillTemplate(template);
        template.add("protocol", _protocolInfo);
        template.add("cached", _fileCached);
    }
}
