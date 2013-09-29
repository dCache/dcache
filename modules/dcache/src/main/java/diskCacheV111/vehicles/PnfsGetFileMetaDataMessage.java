// $Id: PnfsGetFileMetaDataMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles ;

import java.util.Set;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.PNFSID;

public class PnfsGetFileMetaDataMessage extends PnfsGetFileAttributes
{
    private boolean      _resolve     = true ;

    private static final long serialVersionUID = 1591894346369251468L;

    public PnfsGetFileMetaDataMessage()
    {
        super((PnfsId) null, FileMetaData.getKnownFileAttributes());
        _attributes.add(PNFSID);
        setReplyRequired(true);
    }

    public PnfsGetFileMetaDataMessage(Set<FileAttribute> attr)
    {
        super((PnfsId) null, FileMetaData.getKnownFileAttributes());
        _attributes.add(PNFSID);
        _attributes.addAll(attr);
        setReplyRequired(true);
    }

    public PnfsGetFileMetaDataMessage(PnfsId pnfsId)
    {
        super(pnfsId, FileMetaData.getKnownFileAttributes());
        _attributes.add(PNFSID);
	setReplyRequired(true);
    }

    public void setResolve( boolean resolve ){ _resolve = resolve ; }
    public boolean resolve(){ return _resolve ; }

    public void setChecksumsRequested(boolean checksumsRequested)
    {
        if (checksumsRequested) {
            _attributes.add(CHECKSUM);
        } else {
            _attributes.remove(CHECKSUM);
        }
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }

    @Override
    public boolean fold(Message message)
    {
        if (message.getClass().equals(PnfsGetFileMetaDataMessage.class)) {
            PnfsGetFileMetaDataMessage other =
                (PnfsGetFileMetaDataMessage) message;
            if (other.resolve() == resolve()) {
                return super.fold(message);
            }
        }

        return false;
    }
}
