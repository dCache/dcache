// $Id: PoolFileCheckable.java,v 1.2 2003-08-19 15:46:11 cvs Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;


public interface PoolFileCheckable extends PoolCheckable  {

    public void setPnfsId(PnfsId pnfsId);
    public PnfsId getPnfsId();
    public boolean getHave();
    public void setHave(boolean have);
    public boolean getWaiting();
    public void setWaiting(boolean waiting);

}
