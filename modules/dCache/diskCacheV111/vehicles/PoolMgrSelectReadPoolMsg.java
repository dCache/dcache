// $Id: PoolMgrSelectReadPoolMsg.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.util.* ;
import java.util.EnumSet;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import static org.dcache.namespace.FileAttribute.*;
import static com.google.common.base.Preconditions.*;

/**
 * Requests pool manager to provide a pool from which a given file can
 * be read.
 *
 * The requestor must provide sufficient information for PoolManager
 * to perform the pool selection. The caller may use the
 * getRequiredAttributes method to learn which attributes are required
 * by PoolManager.
 *
 * If available, PoolManager will select one of the pools already
 * containing the file. If that is not possible then PoolManager is
 * free to either copy the file to another pool or stage it from
 * tape. These operations will cause the file attributes to be out of
 * date. In such cases PoolManager will reply with an OUT_OF_DATE
 * error code. The requestor is expected to refresh avilable file
 * attributes and retry the request immediately.
 *
 * Should pool selection fail for any reason then the requestor may
 * retry the request. In such cases PoolManager needs access to state
 * from the previous request. It is the responsibility of the
 * requestor to maintain this state and provide when retrying the
 * request. The state is encapsulated in the reply message. This
 * message should be attached to the retry request.
 *
 * The requestor should expect that a subsequent request to read the
 * file from a pool may fail. Typical reasons for such failures is
 * that the pool was disabled after pool manager selected the pool, or
 * that the name space contained stale information (such stale
 * information is cleared by pool on attempt to read the file). The
 * requestor may retry the pool selection and should reread file meta
 * data before doing so.
 */
public class PoolMgrSelectReadPoolMsg extends PoolMgrSelectPoolMsg
{
    private static final long serialVersionUID = -2126253028981131441L;

    private PoolMgrSelectReadPoolMsg _previousMessage;

    public PoolMgrSelectReadPoolMsg(FileAttributes fileAttributes,
                                    ProtocolInfo protocolInfo,
                                    long fileSize,
                                    PoolMgrSelectReadPoolMsg previousMessage)
    {
        this(fileAttributes, protocolInfo, fileSize, previousMessage,
             RequestContainerV5.allStates);
        checkArgument(fileAttributes.getDefinedAttributes().containsAll(getRequiredAttributes()),
                      "Required attributes are missing");
    }

    /**
     * @param fileAttributes FileAttributes of the file to read
     * @param protocolInfo ProtocolInfo describe the transfer
     * @param fileSize The size of the file
     * @param previousMessage The message of the previous attempt; may be null
     * @param allowedStates Allowed states of the pool manager state machine
     */
    public PoolMgrSelectReadPoolMsg(FileAttributes fileAttributes,
                                    ProtocolInfo protocolInfo,
                                    long fileSize,
                                    PoolMgrSelectReadPoolMsg previousMessage,
                                    EnumSet<RequestContainerV5.RequestState> allowedStates)
    {
        super(fileAttributes, protocolInfo , fileSize , allowedStates);
        _previousMessage = previousMessage;
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        return EnumSet.of(PNFSID, STORAGEINFO, LOCATIONS);
    }

    public PoolMgrSelectReadPoolMsg getPreviousMessage()
    {
        return _previousMessage;
    }
}
