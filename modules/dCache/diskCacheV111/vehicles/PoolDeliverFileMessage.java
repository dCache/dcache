// $Id: PoolDeliverFileMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.*; 
import diskCacheV111.util.* ;

public class PoolDeliverFileMessage extends PoolIoFileMessage {
   
    private static final long serialVersionUID = 1168612224820572770L;
    
    public PoolDeliverFileMessage( String pool , 
                                  String pnfsId ,
                                  ProtocolInfo protocolInfo ,
                                  StorageInfo  storageInfo   ){
       super( pool , pnfsId , protocolInfo , storageInfo ) ;
    }
    public PoolDeliverFileMessage( String pool , 
                                  PnfsId pnfsId ,
                                  ProtocolInfo protocolInfo ,
                                  StorageInfo  storageInfo   ){
       super( pool , pnfsId , protocolInfo , storageInfo ) ;
    }
}
