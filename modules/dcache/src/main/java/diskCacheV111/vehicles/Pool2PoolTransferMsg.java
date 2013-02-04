// $Id: Pool2PoolTransferMsg.java,v 1.8 2006-04-18 07:13:47 patrick Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;
import org.dcache.vehicles.FileAttributes;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.EnumSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.namespace.FileAttribute.*;


public class Pool2PoolTransferMsg extends PoolMessage {

    public final static int   UNDETERMINED = 0 ;
    public final static int   PRECIOUS     = 1 ;
    public final static int   CACHED       = 2 ;

    private FileAttributes _fileAttributes;

    @Deprecated // Remove in 2.7
    private PnfsId      _pnfsId;

    @Deprecated // Remove in 2.7
    private StorageInfo _storageInfo;

    private String      _destinationPoolName;
    private int         _destinationFileStatus = UNDETERMINED ;

    private static final long serialVersionUID = -4227857007512530410L;

    public Pool2PoolTransferMsg( String sourcePoolName ,
                                 String destinationPoolName ,
                                 FileAttributes fileAttributes){
        super( sourcePoolName ) ;

        checkNotNull(fileAttributes);
        checkArgument(fileAttributes.isDefined(EnumSet.of(PNFSID, SIZE)));

        _fileAttributes = fileAttributes;
        _pnfsId      = fileAttributes.getPnfsId();
        if (fileAttributes.isDefined(STORAGEINFO)) {
            _storageInfo = fileAttributes.getStorageInfo();
        }
        _destinationPoolName = destinationPoolName ;
        setReplyRequired(true);
    }

    public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    public String getSourcePoolName(){ return getPoolName() ; }
    public String getDestinationPoolName(){ return _destinationPoolName ; }

    public void setDestinationFileStatus( int status ){
       _destinationFileStatus = status ;
    }
    public int getDestinationFileStatus(){
       return _destinationFileStatus ;
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public String toString(){
       return getPoolName()+";pnfsid=" + _fileAttributes.getPnfsId() + ";mode="+
             ( _destinationFileStatus==UNDETERMINED?
                "Undetermined":
                ( _destinationFileStatus==PRECIOUS?"Precious":"Cached" ));
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_fileAttributes == null) {
            _fileAttributes = new FileAttributes();
            _fileAttributes.setStorageInfo(_storageInfo);
            _fileAttributes.setPnfsId(_pnfsId);
        }
    }
}
