// $Id: PoolMgrSelectPoolMsg.java,v 1.5 2006-04-18 07:13:47 patrick Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

public class PoolMgrSelectPoolMsg extends PoolMgrGetPoolMsg {


    private ProtocolInfo _protocolInfo;
    private long         _fileSize;
    private String       _ioQueueName = null ;
    
    private static final long serialVersionUID = -5874326080375390208L;
    
    public PoolMgrSelectPoolMsg( String       pnfsId ,
                                 StorageInfo  storageInfo,
				 ProtocolInfo protocolInfo,
				 long fileSize ){
                                 
	super( pnfsId , storageInfo ) ;
	_protocolInfo = protocolInfo;
	_fileSize     = fileSize;
        
    }
    public PoolMgrSelectPoolMsg( PnfsId       pnfsId ,
                                 StorageInfo  storageInfo,
				 ProtocolInfo protocolInfo,
				 long fileSize ){
                                 
	super( pnfsId , storageInfo ) ;
	_protocolInfo = protocolInfo;
	_fileSize     = fileSize;
        
    }
    
    
    public long getFileSize(){ return _fileSize; };
    
    public ProtocolInfo getProtocolInfo(){ return _protocolInfo; };
    public void setProtocolInfo( ProtocolInfo protocolInfo ){ _protocolInfo = protocolInfo ; }
    public void setIoQueueName( String ioQueueName ){ _ioQueueName = ioQueueName ; }
    public String getIoQueueName(){ return _ioQueueName ; }
}
