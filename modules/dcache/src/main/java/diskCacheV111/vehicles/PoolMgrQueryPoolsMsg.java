// $Id: PoolMgrQueryPoolsMsg.java,v 1.6 2007-07-23 10:49:09 behrmann Exp $

package diskCacheV111.vehicles;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import java.util.List;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

public class PoolMgrQueryPoolsMsg extends PoolManagerMessage {

    private static final long serialVersionUID = 2891200690839001621L;

    private final DirectionType _accessType;

    private final String _netUnitName;
    private final String _protocolUnitName;
    private final FileAttributes _fileAttributes;
    private List<String>[] _poolList;

    public PoolMgrQueryPoolsMsg(DirectionType accessType,
          String protocolUnit,
          String netUnitName,
          FileAttributes fileAttributes) {
        _accessType = requireNonNull(accessType);
        _protocolUnitName = requireNonNull(protocolUnit);
        _netUnitName = requireNonNull(netUnitName);
        _fileAttributes = requireNonNull(fileAttributes);
        checkArgument(fileAttributes.isDefined(FileAttribute.STORAGEINFO));

        setReplyRequired(true);
    }

    public DirectionType getAccessType() {
        return _accessType;
    }

    public String getNetUnitName() {
        return _netUnitName;
    }

    public String getProtocolUnitName() {
        return _protocolUnitName;
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public void setPoolList(List<String>[] poolList) {
        _poolList = poolList;
    }

    public List<String>[] getPools() {
        return _poolList;
    }
}
