// $Id: PoolFetchFileMessage.java,v 1.6 2006-04-10 12:19:03 tigran Exp $

package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.ObjectInputStream;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

/**
 * restore file from HSM
 */

public class PoolFetchFileMessage extends PoolMessage {

    private PnfsId _pnfsId;
    private StorageInfo _storageInfo;
    private FileAttributes _fileAttributes;

    private static final long serialVersionUID = 1856537534158868883L;

    public PoolFetchFileMessage(String poolName, FileAttributes fileAttributes)
    {
        super(poolName);
        _fileAttributes = fileAttributes;
        _pnfsId = fileAttributes.getPnfsId();
        _storageInfo = StorageInfos.extractFrom(fileAttributes);
        setReplyRequired(true);
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_fileAttributes == null) {
            _fileAttributes = new FileAttributes();
            if (_storageInfo != null) {
                StorageInfos.injectInto(_storageInfo, _fileAttributes);
            }
            _fileAttributes.setPnfsId(_pnfsId);
        }
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + " " + getPnfsId();
    }

}
