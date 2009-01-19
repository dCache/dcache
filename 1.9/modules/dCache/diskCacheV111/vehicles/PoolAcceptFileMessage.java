// $Id: PoolAcceptFileMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.*; 
import diskCacheV111.util.*;

public class PoolAcceptFileMessage extends PoolIoFileMessage {
    
    private static final long serialVersionUID = 7898737438685700742L;
    
    public PoolAcceptFileMessage( String pool , 
                                  String pnfsId ,
                                  ProtocolInfo protocolInfo ,
                                  StorageInfo  storageInfo   ){
       super( pool , pnfsId , protocolInfo , storageInfo ) ;
    }
    public PoolAcceptFileMessage( String pool , 
                                  PnfsId pnfsId ,
                                  ProtocolInfo protocolInfo ,
                                  StorageInfo  storageInfo   ){
       super( pool , pnfsId , protocolInfo , storageInfo ) ;
    }
   
}
