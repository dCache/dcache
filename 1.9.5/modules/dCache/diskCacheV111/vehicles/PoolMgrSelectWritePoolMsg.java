// $Id: PoolMgrSelectWritePoolMsg.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;
public class PoolMgrSelectWritePoolMsg extends PoolMgrSelectPoolMsg {

    private static final long serialVersionUID = 1935227143005174577L;    

    public PoolMgrSelectWritePoolMsg( String       pnfsId ,
                                      StorageInfo  storageInfo,
				      ProtocolInfo protocolInfo,
				      long fileSize ){
	super( pnfsId , storageInfo , protocolInfo , fileSize ) ;
    }
    public PoolMgrSelectWritePoolMsg( PnfsId       pnfsId ,
                                      StorageInfo  storageInfo,
				      ProtocolInfo protocolInfo,
				      long fileSize ){
	super( pnfsId , storageInfo , protocolInfo , fileSize ) ;
    }

}
