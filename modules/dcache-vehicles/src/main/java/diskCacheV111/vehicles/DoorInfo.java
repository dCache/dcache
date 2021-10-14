package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nonnull;

public class DoorInfo implements Serializable {

    private final String _cellName;
    private String _cellDomainName;
    private String _protocolFamily = "<unknown>";
    private String _protocolVersion = "<unknown>";
    private String _owner = "<unknown>";
    private String _process = "<unknown>";
    private Object _detail;

    private static final long serialVersionUID = 8147992359534291288L;

    public DoorInfo(String cellName, String cellDomainName) {
        _cellName = requireNonNull(cellName);
        _cellDomainName = requireNonNull(cellDomainName);
    }

    public void setProtocol(String protocolFamily, String protocolVersion) {
        _protocolFamily = requireNonNull(protocolFamily);
        _protocolVersion = requireNonNull(protocolVersion);
    }

    @Nonnull
    public String getProtocolFamily() {
        return _protocolFamily;
    }

    @Nonnull
    public String getProtocolVersion() {
        return _protocolVersion;
    }

    @Nonnull
    public String getCellName() {
        return _cellName;
    }

    @Nonnull
    public String getDomainName() {
        return _cellDomainName;
    }

    public void setOwner(String owner) {
        _owner = requireNonNull(owner);
    }

    public void setProcess(String process) {
        _process = requireNonNull(process);
    }

    @Nonnull
    public String getOwner() {
        return _owner;
    }

    @Nonnull
    public String getProcess() {
        return _process;
    }

    public void setDetail(Serializable detail) {
        _detail = detail;
    }

    public Serializable getDetail() {
        return (Serializable) _detail;
    }

    public String toString() {
        return _cellName + '@' + _cellDomainName + ";p=" + _protocolFamily + '-' + _protocolVersion
              + ";o=" + _owner + '/' + _process + ';';
    }

    private void readObject(java.io.ObjectInputStream stream)
          throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        _cellDomainName = _cellDomainName.intern();
        _protocolFamily = _protocolFamily.intern();
        _protocolVersion = _protocolVersion.intern();
        _owner = _owner.intern();
        _process = _process.intern();
    }
}
