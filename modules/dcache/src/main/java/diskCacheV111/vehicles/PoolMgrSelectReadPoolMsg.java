// $Id: PoolMgrSelectReadPoolMsg.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.io.Serializable;
import java.util.EnumSet;

import diskCacheV111.poolManager.RequestContainerV5;

import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.SelectedPool;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.*;
import static diskCacheV111.poolManager.RequestContainerV5.RequestState.*;

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
 * error code. The requester is expected to refresh available file
 * attributes and retry the request immediately.
 *
 * Should pool selection fail for any reason then the requester may
 * retry the request. In such cases PoolManager needs access to state
 * from the previous request. It is the responsibility of the
 * requester to maintain this state and provide it when retrying the
 * request. The state is encapsulated in the request context. This
 * context should be attached to the retry request.
 *
 * The requester should expect that a subsequent request to read the
 * file from a pool may fail. Typical reasons for such failures is
 * that the pool was disabled after pool manager selected the pool, or
 * that the name space contained stale information (such stale
 * information is cleared by pool on attempt to read the file). The
 * requester may retry the pool selection and should reread file meta
 * data before doing so.
 */
public class PoolMgrSelectReadPoolMsg extends PoolMgrSelectPoolMsg
{
    private static final EnumSet<FileAttribute> REQUIRED_ATTRIBUTES =
            EnumSet.of(PNFSID, STORAGEINFO, STORAGECLASS, CACHECLASS, HSM, LOCATIONS, SIZE, ACCESS_LATENCY, RETENTION_POLICY);

    private static final long serialVersionUID = -2126253028981131441L;

    private Context _context;
    private String  _poolGroup;

    public PoolMgrSelectReadPoolMsg(FileAttributes fileAttributes,
                                    ProtocolInfo protocolInfo,
                                    Context context)
    {
        this(fileAttributes, protocolInfo, context, RequestContainerV5.allStates);
    }

    /**
     * @param fileAttributes FileAttributes of the file to read
     * @param protocolInfo ProtocolInfo describe the transfer
     * @param context The context of the previous attempt; may be null
     * @param allowedStates Allowed states of the pool manager state machine
     */
    public PoolMgrSelectReadPoolMsg(FileAttributes fileAttributes,
                                    ProtocolInfo protocolInfo,
                                    Context context,
                                    EnumSet<RequestContainerV5.RequestState> allowedStates)
    {
        super(fileAttributes, protocolInfo, allowedStates);
        checkArgument(fileAttributes.isDefined(getRequiredAttributes()),
                "Required attributes are missing: %s",
                missingFileAttributes(fileAttributes));
        _context = (context == null) ? new Context() : context;
    }

    private static EnumSet<FileAttribute> missingFileAttributes(FileAttributes fileAttributes)
    {
        EnumSet<FileAttribute> attributes = getRequiredAttributes();
        attributes.removeAll(fileAttributes.getDefinedAttributes());
        return attributes;
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        EnumSet<FileAttribute> attributes = REQUIRED_ATTRIBUTES.clone();
        attributes.addAll(Pool2PoolTransferMsg.NEEDED_ATTRIBUTES);
        return attributes;
    }

    public Context getContext()
    {
        return _context;
    }

    public String getPoolGroup()
    {
        return _poolGroup;
    }

    public void setContext(Context context)
    {
        _context = context;
    }

    public void setContext(int retryCounter, SelectedPool previousStagePool)
    {
        setContext(new Context(retryCounter, previousStagePool));
    }

    public void setPoolGroup(String poolGroup) {
        _poolGroup = poolGroup;
    }

    /**
     * Pool selection context. Captures the state the pool manager
     * must maintain between repeated attempt to select a read pool
     * for a file.
     */
    public static class Context implements Serializable
    {
        private static final long serialVersionUID = -1896293244725567276L;
        private final int _retryCounter;
        private final SelectedPool _previousStagePool;

        public Context()
        {
            _retryCounter = 0;
            _previousStagePool = null;
        }

        public Context(int retryCounter, SelectedPool previousStagePool)
        {
            _retryCounter = retryCounter;
            _previousStagePool = previousStagePool;
        }

        public int getRetryCounter()
        {
            return _retryCounter;
        }

        public SelectedPool getPreviousStagePool()
        {
            return _previousStagePool;
        }
    }

    @Override
    public boolean requiresAffinity() {
	// require affinity only it stage/p2p is enabled.
	EnumSet<RequestContainerV5.RequestState> allowedStates = getAllowedStates();
	return allowedStates.contains(ST_POOL_2_POOL) || allowedStates.contains(ST_STAGE);
    }

}
