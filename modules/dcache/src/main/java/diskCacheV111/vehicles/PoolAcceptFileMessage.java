// $Id: PoolAcceptFileMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.EnumSet;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;

public class PoolAcceptFileMessage extends PoolIoFileMessage {

    private static final long serialVersionUID = 7898737438685700742L;

    public PoolAcceptFileMessage( String pool ,
                                  ProtocolInfo protocolInfo ,
                                  FileAttributes fileAttributes){
        super( pool , protocolInfo , fileAttributes ) ;
        checkArgument(fileAttributes.isDefined(
                EnumSet.of(ACCESS_LATENCY, RETENTION_POLICY)));

    }
}
