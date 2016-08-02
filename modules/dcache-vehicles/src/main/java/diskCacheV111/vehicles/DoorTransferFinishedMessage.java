package diskCacheV111.vehicles;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Signals the completion of a transfer on a pool.
 */
@ParametersAreNonnullByDefault
public class DoorTransferFinishedMessage extends Message {
   private final ProtocolInfo _protocol;
   private final FileAttributes _fileAttributes;
   private final PnfsId _pnfsId;
   private final String _poolName;
   private final String _ioQueueName;
   private static final long serialVersionUID = -7563456962335030196L;

   public DoorTransferFinishedMessage(long id,
                                      PnfsId pnfsId,
                                      ProtocolInfo protocol,
                                      FileAttributes fileAttributes,
                                      String poolName,
                                      @Nullable String ioQueueName) {
       setId(id);
       _fileAttributes = checkNotNull(fileAttributes);
       _protocol = checkNotNull(protocol);
       _pnfsId   = checkNotNull(pnfsId);
       _poolName = checkNotNull(poolName);
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

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}


