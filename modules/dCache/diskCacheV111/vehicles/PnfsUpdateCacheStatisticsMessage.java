// $Id: PnfsUpdateCacheStatisticsMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.*;

public class PnfsUpdateCacheStatisticsMessage extends PnfsMessage {
    private long _accessTime = 0;

    private static final long serialVersionUID = -3806931631136707060L;
    
    public PnfsUpdateCacheStatisticsMessage(String pnfsId){
	super(pnfsId);
	_accessTime = new Date().getTime();
    }
    
    public PnfsUpdateCacheStatisticsMessage(String pnfsId, long accessTime){
	super(pnfsId);
	_accessTime = accessTime;
    }
    
    public long getAccessTime(){
	return _accessTime;
    }
}
