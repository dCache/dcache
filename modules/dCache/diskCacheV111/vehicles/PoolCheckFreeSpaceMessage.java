// $Id: PoolCheckFreeSpaceMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.*; 


public class PoolCheckFreeSpaceMessage extends PoolMessage {
   
    private long _freeSpace = 0;

    private static final long serialVersionUID = 2269590062279181028L;
    
    public PoolCheckFreeSpaceMessage(String poolName){
	super(poolName);
	setReplyRequired(true);
    }

    public void setFreeSpace(long freeSpace){
	_freeSpace = freeSpace;
    }

    public long getFreeSpace(){
	return _freeSpace;
    }
}
