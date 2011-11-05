// $Id: PoolMgrSelectWritePoolMsg.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import org.dcache.vehicles.FileAttributes;

public class PoolMgrSelectWritePoolMsg extends PoolMgrSelectPoolMsg
{
    private static final long serialVersionUID = 1935227143005174577L;

    public PoolMgrSelectWritePoolMsg(FileAttributes fileAttributes,
                                     ProtocolInfo protocolInfo,
                                     long fileSize)
    {
	super(fileAttributes, protocolInfo, fileSize);
    }
}
