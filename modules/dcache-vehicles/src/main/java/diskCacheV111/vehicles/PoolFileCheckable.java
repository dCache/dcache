// $Id: PoolFileCheckable.java,v 1.2 2003-08-19 15:46:11 cvs Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;


public interface PoolFileCheckable extends PoolCheckable  {

    void setPnfsId(PnfsId pnfsId);
    PnfsId getPnfsId();
    boolean getHave();
    void setHave(boolean have);
    boolean getWaiting();
    void setWaiting(boolean waiting);

}
