// $Id: PoolMgrSelectPoolMsg.java,v 1.7 2006-10-10 13:50:49 tigran Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;
import diskCacheV111.poolManager.RequestContainerV5;

public class PoolMgrSelectPoolMsg extends PoolMgrGetPoolMsg {


    private ProtocolInfo _protocolInfo;
    private long         _fileSize;
    private String       _ioQueueName = null ;
    private String       _pnfsPath;
    private String       _linkGroup = null;
    private static final long serialVersionUID = -5874326080375390208L;
    private final int    _allowedStates;

    public PoolMgrSelectPoolMsg( String       pnfsId ,
                                 StorageInfo  storageInfo,
                ProtocolInfo protocolInfo,
                long fileSize ){

        this(new PnfsId(pnfsId), storageInfo, protocolInfo, fileSize);

    }
    public PoolMgrSelectPoolMsg( PnfsId       pnfsId ,
                                 StorageInfo  storageInfo,
                ProtocolInfo protocolInfo,
                long fileSize ){

    this( pnfsId , storageInfo, protocolInfo, fileSize, RequestContainerV5.allStates) ;

    }

    public PoolMgrSelectPoolMsg( String pnfsId ,
            StorageInfo  storageInfo,
            ProtocolInfo protocolInfo,
            long fileSize,
            int allowedStates){

        this(new PnfsId(pnfsId), storageInfo, protocolInfo, fileSize, allowedStates);
    }

    public PoolMgrSelectPoolMsg( PnfsId pnfsId ,
            StorageInfo  storageInfo,
            ProtocolInfo protocolInfo,
            long fileSize,
            int allowedStates){

        super( pnfsId , storageInfo ) ;
        _protocolInfo = protocolInfo;
        _fileSize     = fileSize;
        _allowedStates = allowedStates;
    }

    public void setFileSize(long fileSize)
    {
        _fileSize = fileSize;
    }

    public long getFileSize(){ return _fileSize; }

    public ProtocolInfo getProtocolInfo(){ return _protocolInfo; }
    public void setProtocolInfo( ProtocolInfo protocolInfo ){ _protocolInfo = protocolInfo ; }
    public void setIoQueueName( String ioQueueName ){ _ioQueueName = ioQueueName ; }
    public String getIoQueueName(){ return _ioQueueName ; }

    public String getPnfsPath() {
        return _pnfsPath;
    }

    public void setPnfsPath(String pnfsPath) {
        this._pnfsPath = pnfsPath;
    }

    public void setLinkGroup(String linkGroup) {
        _linkGroup = linkGroup;
    }

    public String getLinkGroup() {
        return _linkGroup;
    }

    public int getAllowedStates() {
        return _allowedStates;
    }

}
