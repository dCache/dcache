// $Id: PoolMgrSelectReadPoolMsg.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.util.* ;
import java.util.EnumSet;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import static org.dcache.namespace.FileAttribute.*;
import static com.google.common.base.Preconditions.*;

public class PoolMgrSelectReadPoolMsg extends PoolMgrSelectPoolMsg
{
    private static final long serialVersionUID = -2126253028981131441L;

    public PoolMgrSelectReadPoolMsg(FileAttributes fileAttributes,
                                    ProtocolInfo protocolInfo,
                                    long fileSize)
    {
        this(fileAttributes, protocolInfo, fileSize,
             RequestContainerV5.allStates);
        checkArgument(fileAttributes.getDefinedAttributes().containsAll(getRequiredAttributes()),
                      "Required attributes are missing");
    }

    public PoolMgrSelectReadPoolMsg(FileAttributes fileAttributes,
                                    ProtocolInfo protocolInfo,
                                    long fileSize,
                                    EnumSet<RequestContainerV5.RequestState> allowedStates)
    {
        super(fileAttributes, protocolInfo , fileSize , allowedStates);
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        return EnumSet.of(PNFSID, STORAGEINFO, LOCATIONS);
    }
}
