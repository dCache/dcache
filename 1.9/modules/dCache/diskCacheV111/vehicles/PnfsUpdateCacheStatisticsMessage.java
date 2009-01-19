// $Id: PnfsUpdateCacheStatisticsMessage.java,v 1.3 2007-05-24 13:51:05 tigran Exp $

package diskCacheV111.vehicles;


public class PnfsUpdateCacheStatisticsMessage extends PnfsMessage {
    private final long _accessTime;

    private static final long serialVersionUID = -3806931631136707060L;

    public PnfsUpdateCacheStatisticsMessage(String pnfsId){
    	this(pnfsId, System.currentTimeMillis());
    }

    public PnfsUpdateCacheStatisticsMessage(String pnfsId, long accessTime){
		super(pnfsId);
		_accessTime = accessTime;
    }

    public long getAccessTime(){
    	return _accessTime;
    }
}
