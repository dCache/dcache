// $Id: PnfsGetFileMetaDataMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles ;

import java.util.Set;

import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.PNFSID;

public class PnfsGetFileMetaDataMessage extends PnfsGetFileAttributes
{
    private FileMetaData _metaData;
    private boolean      _resolve     = true ;
    private boolean _checksumsRequested;

    private Set<Checksum> _checksums;

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

    @Override
    public void setFileAttributes(FileAttributes fileAttributes)
    {
        /* For backwards compatibility with old versions we set these
         * fields. We do this even though we don't use these fields.
         */
        _metaData = new FileMetaData(fileAttributes);
        if (fileAttributes.isDefined(CHECKSUM)) {
            _checksums = fileAttributes.getChecksums();
        }

        /* For backwards compatibility with old versions, we do not
         * set the file attributes unless they have actually been
         * requested. This avoid deserialization problems with clients
         * that cannot handle all values in the FileAttribute enum.
         */
        if (_attributes != null) {
            super.setFileAttributes(fileAttributes);
        }
    }

    public FileMetaData getMetaData()
    {
        return
            (_fileAttributes == null)
            ? null
            : new FileMetaData(_fileAttributes);
    }

    public void setResolve( boolean resolve ){ _resolve = resolve ; }
    public boolean resolve(){ return _resolve ; }

    public Set<Checksum> getChecksums()
    {
        return
            (_fileAttributes == null || !_fileAttributes.isDefined(CHECKSUM))
            ? null
            : _fileAttributes.getChecksums();
    }

    public boolean isChecksumsRequested() {
        return _checksumsRequested;
    }

    public void setChecksumsRequested(boolean checksumsRequested)
    {
        _checksumsRequested = checksumsRequested;
        if (checksumsRequested) {
            _attributes.add(CHECKSUM);
        } else {
            _attributes.remove(CHECKSUM);
        }
    }

    public void requestChecksum()
    {
        _checksumsRequested = true;
        _attributes.add(CHECKSUM);
    }

    /* To ensure backwards compatibility with pre 1.9.6 clients, we
     * explicitly add attributes compatible with
     * PnfsGetFileMetaDataMessage to the set of requested attributes
     * if the attribute set is null.
     */
    @Override
    public Set<FileAttribute> getRequestedAttributes()
    {
        Set<FileAttribute> attributes = _attributes;
        if (attributes == null) {
            attributes = FileMetaData.getKnownFileAttributes();
            if (_checksumsRequested) {
                attributes.add(CHECKSUM);
            }
        }
        return attributes;
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
