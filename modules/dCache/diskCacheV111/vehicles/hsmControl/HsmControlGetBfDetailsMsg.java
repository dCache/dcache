/*
 * HsmControlGetBfDetailsMsg.java
 *
 * Created on January 14, 2005, 12:37 PM
 */

package diskCacheV111.vehicles.hsmControl;

import diskCacheV111.vehicles.Message ;
import diskCacheV111.vehicles.StorageInfo ;
import diskCacheV111.util.PnfsId ;

/**
 *
 * @author  patrick
 */
public class HsmControlGetBfDetailsMsg extends Message  {

    private StorageInfo _storageInfo = null ;
    private PnfsId      _pnfsId  = null ;
    private String      _detail  = null ;
    /** Creates a new instance of HsmControlGetBfDetailsMsg
     *
     */
    public HsmControlGetBfDetailsMsg( PnfsId pnfsId , StorageInfo storageInfo , String detail ) {
       _pnfsId      = pnfsId ;
       _storageInfo = storageInfo ;
       _detail      = detail ;
    }
    public PnfsId getPnfsId(){ return _pnfsId ; }
    public StorageInfo getStorageInfo(){ return _storageInfo ; }
    public String getDetails(){ return _detail ; }


}
