// $Id: PoolAcceptFileMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import org.dcache.vehicles.FileAttributes;

public class PoolAcceptFileMessage extends PoolIoFileMessage {

    private static final long serialVersionUID = 7898737438685700742L;

    public PoolAcceptFileMessage( String pool ,
                                  ProtocolInfo protocolInfo ,
                                  FileAttributes fileAttributes){
       super( pool , protocolInfo , fileAttributes ) ;
    }
}
