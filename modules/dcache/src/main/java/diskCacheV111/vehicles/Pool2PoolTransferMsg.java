// $Id: Pool2PoolTransferMsg.java,v 1.8 2006-04-18 07:13:47 patrick Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;


public class Pool2PoolTransferMsg extends PoolMessage {

    public final static int   UNDETERMINED = 0 ;
    public final static int   PRECIOUS     = 1 ;
    public final static int   CACHED       = 2 ;

    private PnfsId      _pnfsId;
    private StorageInfo _storageInfo;
    private String      _destinationPoolName;
    private int         _destinationFileStatus = UNDETERMINED ;

    private static final long serialVersionUID = -4227857007512530410L;

    public Pool2PoolTransferMsg( String sourcePoolName ,
                                 String destinationPoolName ,
                                 PnfsId pnfsId ,
                                 StorageInfo storageInfo ){
       super( sourcePoolName ) ;
       _pnfsId      = pnfsId ;
       _storageInfo = storageInfo ;
       _destinationPoolName = destinationPoolName ;
       setReplyRequired(true);
    }

    public PnfsId getPnfsId(){
	return _pnfsId ;
    }
    public StorageInfo getStorageInfo(){ return _storageInfo ; }
    public String getSourcePoolName(){ return getPoolName() ; }
    public String getDestinationPoolName(){ return _destinationPoolName ; }

    public void setDestinationFileStatus( int status ){
       _destinationFileStatus = status ;
    }
    public int getDestinationFileStatus(){
       return _destinationFileStatus ;
    }
    public String toString(){
       return getPoolName()+";pnfsid="+_pnfsId + ";mode="+
             ( _destinationFileStatus==UNDETERMINED?
                "Undetermined":
                ( _destinationFileStatus==PRECIOUS?"Precious":"Cached" ));
    }
}
