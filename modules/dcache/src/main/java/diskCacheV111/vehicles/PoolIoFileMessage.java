package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class PoolIoFileMessage extends PoolMessage {

    private StorageInfo  _storageInfo;
    private ProtocolInfo _protocolInfo;
    private PnfsId       _pnfsId;
    private boolean      _isPool2Pool;
    private String       _ioQueueName;
    private int          _moverId;
    private String       _initiator = "<undefined>";
    private boolean      _forceSourceMode;

    private static final long serialVersionUID = -6549886547049510754L;

    public PoolIoFileMessage( String pool ,
                              PnfsId pnfsId ,
                              ProtocolInfo protocolInfo ,
                              StorageInfo  storageInfo   ){
       super( pool ) ;
       _storageInfo  = storageInfo ;
       _protocolInfo = protocolInfo ;
       _pnfsId       = pnfsId ;
    }

    public PoolIoFileMessage( String pool ,
                              PnfsId pnfsId ,
                              ProtocolInfo protocolInfo  ){
       super( pool ) ;
       _protocolInfo = protocolInfo ;
       _pnfsId       = pnfsId ;
    }
    public PnfsId       getPnfsId(){ return _pnfsId ; }
    public StorageInfo  getStorageInfo(){ return _storageInfo ; }
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

    public void setForceSourceMode(boolean forceSourceMode)
    {
        _forceSourceMode = forceSourceMode;
    }

    public boolean isForceSourceMode()
    {
        return _forceSourceMode;
    }
}
