// $Id: Pool2PoolTransferMsg.java,v 1.8 2006-04-18 07:13:47 patrick Exp $

package diskCacheV111.vehicles;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.CACHECLASS;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.HSM;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGECLASS;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import diskCacheV111.util.PnfsId;
import java.util.EnumSet;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;


public class Pool2PoolTransferMsg extends PoolMessage {

    public static final ImmutableSet<FileAttribute> NEEDED_ATTRIBUTES =
          Sets.immutableEnumSet(PNFSID, STORAGEINFO, CHECKSUM, SIZE, ACCESS_LATENCY,
                RETENTION_POLICY,
                STORAGECLASS, CACHECLASS, HSM);

    public static final int UNDETERMINED = 0;
    public static final int PRECIOUS = 1;
    public static final int CACHED = 2;

    private final FileAttributes _fileAttributes;

    private final String _destinationPoolName;
    private int _destinationFileStatus = UNDETERMINED;

    private static final long serialVersionUID = -4227857007512530410L;

    public Pool2PoolTransferMsg(String sourcePoolName,
          String destinationPoolName,
          FileAttributes fileAttributes) {
        super(sourcePoolName);

        requireNonNull(fileAttributes);
        checkArgument(fileAttributes.isDefined(EnumSet.of(PNFSID)));

        _fileAttributes = fileAttributes;
        _destinationPoolName = destinationPoolName;
        setReplyRequired(true);
    }

    public PnfsId getPnfsId() {
        return _fileAttributes.getPnfsId();
    }

    public String getSourcePoolName() {
        return getPoolName();
    }

    public String getDestinationPoolName() {
        return _destinationPoolName;
    }

    public void setDestinationFileStatus(int status) {
        _destinationFileStatus = status;
    }

    public int getDestinationFileStatus() {
        return _destinationFileStatus;
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public String toString() {
        return getPoolName() + ";pnfsid=" + _fileAttributes.getPnfsId() + ";mode=" +
              (_destinationFileStatus == UNDETERMINED ?
                    "Undetermined" :
                    (_destinationFileStatus == PRECIOUS ? "Precious" : "Cached"));
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }

}
