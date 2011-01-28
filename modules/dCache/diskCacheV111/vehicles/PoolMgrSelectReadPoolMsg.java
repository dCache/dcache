// $Id: PoolMgrSelectReadPoolMsg.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import diskCacheV111.poolManager.RequestContainerV5;
import  diskCacheV111.util.* ;
import java.util.EnumSet;

public class PoolMgrSelectReadPoolMsg extends PoolMgrSelectPoolMsg {

    private static final long serialVersionUID = -2126253028981131441L;

    public PoolMgrSelectReadPoolMsg( PnfsId       pnfsId ,
                                     StorageInfo  storageInfo,
                    ProtocolInfo protocolInfo,
                    long fileSize ){
    this( pnfsId , storageInfo , protocolInfo , fileSize, RequestContainerV5.allStates ) ;
    }

    public PoolMgrSelectReadPoolMsg( PnfsId pnfsId ,
            StorageInfo  storageInfo,
            ProtocolInfo protocolInfo,
            long fileSize,
            EnumSet<RequestContainerV5.RequestState> allowedStates){

            super( pnfsId , storageInfo , protocolInfo , fileSize , allowedStates) ;
    }

}
