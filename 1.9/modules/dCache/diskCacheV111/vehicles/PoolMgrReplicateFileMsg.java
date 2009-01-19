/*
 * PoolMgrReplicateFile.java
 *
 * Created on February 28, 2005, 2:30 PM
 */

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;

/**
 *
 * @author  patrick
 */
public class PoolMgrReplicateFileMsg extends PoolMgrSelectReadPoolMsg {

    private static final long serialVersionUID = -2126253846930131441L;    
 
    private boolean _allowRestore          = false ;
    private int     _destinationFileStatus = Pool2PoolTransferMsg.UNDETERMINED ;
    
    /** Creates a new instance of PoolMgrReplicateFile */
    public PoolMgrReplicateFileMsg(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, long fileSize) {
         super( pnfsId , storageInfo , protocolInfo , fileSize ) ;
    }
    public void setAllowRestore( boolean allowRestore ){
        _allowRestore = allowRestore ;
    }
    public boolean allowRestore(){ return _allowRestore ; }
    public void setDestinationFileStatus( int status ){
       _destinationFileStatus = status ;
    }
    public int getDestinationFileStatus(){
       return _destinationFileStatus ;
    }
}
