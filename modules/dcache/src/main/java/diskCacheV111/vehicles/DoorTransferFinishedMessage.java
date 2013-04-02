// $Id: DoorTransferFinishedMessage.java,v 1.7 2005-06-01 06:03:54 patrick Exp $

package diskCacheV111.vehicles;

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
        _info = fileAttributes.getStorageInfo();
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
       if (_fileAttributes == null && _info != null) {
           _fileAttributes = new FileAttributes();
           _fileAttributes.setStorageInfo(_info);
           _fileAttributes.setSize(_info.getFileSize());
           _fileAttributes.setAccessLatency(_info.getAccessLatency());
           _fileAttributes.setRetentionPolicy(_info.getRetentionPolicy());
       }
       return _fileAttributes;
   }

   public PnfsId getPnfsId() {
       return _pnfsId;
   }

   public String getPoolName() {
       return _poolName;
   }
}


