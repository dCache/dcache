// $Id: PoolMgrGetWritePoolMsg.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;
public class PoolMgrGetWritePoolMsg extends PoolMgrGetPoolMsg {

    private static final long serialVersionUID = -2381044790545470117L;
    
    public PoolMgrGetWritePoolMsg( PnfsId       pnfsId ,
                                   StorageInfo  storageInfo ){
	super( pnfsId , storageInfo ) ;
    }

}
