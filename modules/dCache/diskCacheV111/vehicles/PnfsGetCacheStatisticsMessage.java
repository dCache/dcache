// $Id: PnfsGetCacheStatisticsMessage.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;

import java.util.*;

public class PnfsGetCacheStatisticsMessage extends PnfsMessage {

    private CacheStatistics _cacheStatistics;

    private static final long serialVersionUID = 8035834344050871201L;

    public PnfsGetCacheStatisticsMessage(String pnfsId){
	super(pnfsId);
	setReplyRequired(true);
    }
    public PnfsGetCacheStatisticsMessage(PnfsId pnfsId){
	super(pnfsId);
	setReplyRequired(true);
    }

    public CacheStatistics getCacheStatistics(){
	return _cacheStatistics;
    }

    public void setCacheStatistics(CacheStatistics cacheStatistics){
	_cacheStatistics = cacheStatistics;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
