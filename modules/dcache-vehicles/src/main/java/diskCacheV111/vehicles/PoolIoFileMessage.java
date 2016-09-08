package diskCacheV111.vehicles;

import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.assumption.Assumption;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;

public class PoolIoFileMessage extends PoolMessage {

    private final FileAttributes _fileAttributes;
    private final ProtocolInfo _protocolInfo;
    private boolean      _isPool2Pool;
    private String       _ioQueueName;
    private int          _moverId;
    private String       _initiator = "<undefined>";
    private boolean      _forceSourceMode;
    private String _pnfsPath;
    private String _transferPath;
    private final Assumption _assumption;

    private static final long serialVersionUID = -6549886547049510754L;

    public PoolIoFileMessage(String pool ,
                             ProtocolInfo protocolInfo ,
                             FileAttributes fileAttributes,
                             Assumption assumption)
    {
        super(pool);

        checkNotNull(fileAttributes);
        checkArgument(fileAttributes.isDefined(EnumSet.of(STORAGEINFO, PNFSID)));

        _fileAttributes = fileAttributes;
        _protocolInfo = protocolInfo;
        _assumption = assumption;
    }

    public PoolIoFileMessage(String pool,
                             PnfsId pnfsId,
                             ProtocolInfo protocolInfo,
                             Assumption assumption)
    {
        super(pool);
        _protocolInfo = protocolInfo;
        _fileAttributes = FileAttributes.ofPnfsId(pnfsId);
        _assumption = assumption;
    }

    public Assumption getAssumption()
    {
        // REVISIT in 4.0: We need to check for null for backwards compatibility
        return _assumption == null ? Assumptions.none() : _assumption;
    }

    public PnfsId       getPnfsId(){ return _fileAttributes.getPnfsId(); }
    public ProtocolInfo getProtocolInfo(){ return _protocolInfo ; }

    public boolean isPool2Pool(){ return _isPool2Pool ; }
    public void setPool2Pool(){ _isPool2Pool = true ; }

    public void setIoQueueName( String ioQueueName ){
       _ioQueueName = ioQueueName ;
    }
    public String getIoQueueName(){
       return _ioQueueName ;
    }
    /**
     * Getter for property moverId.
     * @return Value of property moverId.
     */
    public int getMoverId() {
        return _moverId;
    }

    /**
     * Setter for property moverId.
     * @param moverId New value of property moverId.
     */
    public void setMoverId(int moverId) {
        this._moverId = moverId;
    }


    public void setInitiator(String initiator) {
        _initiator = initiator;
    }

    public String getInitiator() {
        return _initiator;
    }

    public String getBillingPath()
    {
        return _pnfsPath != null ? _pnfsPath : null;
    }

    public void setBillingPath(String path)
    {
        _pnfsPath = path;
    }

    public String getTransferPath()
    {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path)
    {
        _transferPath = path;
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public void setForceSourceMode(boolean forceSourceMode)
    {
        _forceSourceMode = forceSourceMode;
    }

    public boolean isForceSourceMode()
    {
        return _forceSourceMode;
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}
