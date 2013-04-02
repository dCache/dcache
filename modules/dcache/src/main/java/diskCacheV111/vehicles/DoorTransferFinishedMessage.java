// $Id: DoorTransferFinishedMessage.java,v 1.7 2005-06-01 06:03:54 patrick Exp $

package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.ObjectInputStream;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

/**
 * Signals the completion of a transfer on a pool.
 */
public class DoorTransferFinishedMessage extends Message {
   private final ProtocolInfo _protocol;
   private FileAttributes _fileAttributes;
   @Deprecated // Can be removed in 2.7
   private final StorageInfo  _info;
   private final PnfsId _pnfsId;
   private final String _poolName;
   private final String _ioQueueName;
   private static final long serialVersionUID = -7563456962335030196L;

   public DoorTransferFinishedMessage(long id,
                                      PnfsId pnfsId,
                                      ProtocolInfo protocol,
                                      FileAttributes fileAttributes,
                                      String poolName,
                                      String ioQueueName) {
       setId(id);
       _protocol = protocol;
       _fileAttributes = fileAttributes;
       _info = StorageInfos.extractFrom(fileAttributes);
       _pnfsId   = pnfsId;
       _poolName = poolName;
       _ioQueueName = ioQueueName;
   }

   public String getIoQueueName() {
       return _ioQueueName;
   }

   public ProtocolInfo getProtocolInfo() {
       return _protocol;
   }

   public FileAttributes getFileAttributes() {
       return _fileAttributes;
   }

   public PnfsId getPnfsId() {
       return _pnfsId;
   }

   public String getPoolName() {
       return _poolName;
   }

    // Can be removed in 2.7
    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_fileAttributes == null) {
            _fileAttributes = new FileAttributes();
            if (_info != null) {
                StorageInfos.injectInto(_info, _fileAttributes);
            }
            _fileAttributes.setPnfsId(_pnfsId);
        }
    }
}


