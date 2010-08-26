// $Id: PnfsGetFileMetaDataMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles ;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import java.util.Set;
import org.dcache.util.Checksum;

public class PnfsGetFileMetaDataMessage extends PnfsMessage {

    private FileMetaData _metaData    = null ;
    private boolean      _resolve     = true ;
    private boolean _checksumsRequested = false ;

    private Set<Checksum> _checksums = null;

    private static final long serialVersionUID = 1591894346369251468L;

    public PnfsGetFileMetaDataMessage(){
       super() ;
       setReplyRequired(true);
    }
    public PnfsGetFileMetaDataMessage( String pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public PnfsGetFileMetaDataMessage( PnfsId pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public FileMetaData getMetaData(){ return _metaData ; }
    public void setMetaData( FileMetaData metaData ){ _metaData = metaData ; }
    public String toString(){
       return super.toString()+";"+
             (_metaData==null?"[noMetaData]":_metaData.toString()) ;
    }
    public void setResolve( boolean resolve ){ _resolve = resolve ; }
    public boolean resolve(){ return _resolve ; }

    public void setChecksums(Set<Checksum> checksums)
    {
        _checksums = checksums;
    }

    public Set<Checksum> getChecksums()
    {
        return _checksums;
    }

    public boolean isChecksumsRequested() {
        return _checksumsRequested;
    }

    public void setChecksumsRequested(boolean checksumsRequested) {
        _checksumsRequested = checksumsRequested;
    }

    public void requestChecksum() {
        _checksumsRequested = true;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }

    @Override
    public boolean isSubsumedBy(Message message)
    {
        if (message.getClass().equals(PnfsGetFileMetaDataMessage.class)) {
            PnfsId pnfsId = getPnfsId();
            String path = getPnfsPath();
            PnfsGetFileMetaDataMessage other =
                (PnfsGetFileMetaDataMessage) message;
            return
                other.resolve() == resolve() &&
                (pnfsId == null || pnfsId.equals(other.getPnfsId())) &&
                (path == null || path.equals(other.getPnfsPath())) &&
                (!isChecksumsRequested() || other.isChecksumsRequested()) &&
                (getSubject().equals(other.getSubject())) &&
                (getId() == other.getId());
        }

        return false;
    }

    @Override
    public boolean isIdempotent()
    {
        return true;
    }
}
