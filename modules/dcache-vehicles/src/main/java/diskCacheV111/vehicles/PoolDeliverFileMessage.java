// $Id: PoolDeliverFileMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import org.dcache.pool.assumption.Assumption;
import org.dcache.vehicles.FileAttributes;

public class PoolDeliverFileMessage extends PoolIoFileMessage
{
    private static final long serialVersionUID = 1168612224820572770L;

    public PoolDeliverFileMessage(String pool,
                                  ProtocolInfo protocolInfo,
                                  FileAttributes fileAttributes,
                                  Assumption assumption)
    {
        super(pool, protocolInfo, fileAttributes, assumption);
    }
}
