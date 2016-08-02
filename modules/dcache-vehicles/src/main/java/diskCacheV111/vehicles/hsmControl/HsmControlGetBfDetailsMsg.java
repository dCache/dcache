/*
 * HsmControlGetBfDetailsMsg.java
 *
 * Created on January 14, 2005, 12:37 PM
 */

package diskCacheV111.vehicles.hsmControl;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;

/**
 *
 * @author  patrick
 */
public class HsmControlGetBfDetailsMsg extends Message  {

    private static final long serialVersionUID = -8423976847654758059L;
    private final StorageInfo _storageInfo;
    private final PnfsId      _pnfsId;
    private final String      _detail;
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

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}
