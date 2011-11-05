// $Id: PoolCostInfoMessage.java,v 1.3 2006-04-06 23:26:24 podstvkv Exp $
package diskCacheV111.vehicles ;
import  diskCacheV111.vehicles.ProtocolInfo ;
import  diskCacheV111.util.PnfsId ;

public class PoolCostInfoMessage extends PnfsFileInfoMessage {

    private ProtocolInfo _protocolInfo = null ;
    private double       _cost  = 0.0 ;

    private static final long serialVersionUID = 850395703347910511L;

    public PoolCostInfoMessage(String cellName, PnfsId pnfsId)
    {
		super("cost", "pool", cellName, pnfsId);
    }

    public void setProtocolInfo(ProtocolInfo protocolInfo)
    {
		_protocolInfo = protocolInfo;
    }

    public void setCost(double cost)
    {
		_cost = cost;
    }

    public ProtocolInfo getProtocolInfo()
    {
		return _protocolInfo;
    }

    public double getCost()
    {
		return _cost;
    }

    public String toString()
    {
		return getInfoHeader()+" "+
			getFileInfo()+" "+
			_cost+" {"+
			_protocolInfo+"} "+
			getResult() ;
    }
}
