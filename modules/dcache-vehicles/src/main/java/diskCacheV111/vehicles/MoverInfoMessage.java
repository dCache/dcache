package diskCacheV111.vehicles;

import java.time.Duration;
import java.util.Optional;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

public class MoverInfoMessage extends PnfsFileInfoMessage
{
    private long _dataTransferred;
    private long _connectionTime;

    private ProtocolInfo _protocolInfo;
    private boolean _fileCreated;
    private String _initiator = "<undefined>";
    private boolean _isP2p;
    private double _meanReadBandwidth = Double.NaN;
    private double _meanWriteBandwidth = Double.NaN;
    private Duration _readActive;
    private Duration _readIdle;
    private Duration _writeActive;
    private Duration _writeIdle;

    private static final long serialVersionUID = -7013160118909496211L;
    private String _transferPath;

    public MoverInfoMessage(CellAddressCore address, PnfsId pnfsId)
    {
        super("transfer", "pool", address, pnfsId);
    }

    public void setFileCreated(boolean created)
    {
        _fileCreated = created;
    }

    public void setTransferAttributes(
            long dataTransferred,
            long connectionTime,
            ProtocolInfo protocolInfo)
    {
        _dataTransferred = dataTransferred;
        _connectionTime = connectionTime;
        _protocolInfo = protocolInfo;
    }

    public void setInitiator(String transaction)
    {
        _initiator = transaction;
    }

    public void setP2P(boolean isP2p)
    {
        _isP2p = isP2p;
    }

    public String getInitiator()
    {
        return _initiator;
    }

    public long getDataTransferred()
    {
        return _dataTransferred;
    }

    public long getConnectionTime()
    {
        return _connectionTime;
    }

    public boolean isFileCreated()
    {
        return _fileCreated;
    }

    public boolean isP2P()
    {
        return _isP2p;
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    public String getTransferPath()
    {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path)
    {
        _transferPath = path;
    }

    public void setMeanReadBandwidth(double value)
    {
        _meanReadBandwidth = value;
    }

    public double getMeanReadBandwidth()
    {
        return _meanReadBandwidth;
    }

    public void setMeanWriteBandwidth(double value)
    {
        _meanWriteBandwidth = value;
    }

    public double getMeanWriteBandwidth()
    {
        return _meanWriteBandwidth;
    }

    public void setReadIdle(Duration value)
    {
        _readIdle = value;
    }

    public Optional<Duration> getReadIdle()
    {
        return Optional.ofNullable(_readIdle);
    }

    public void setReadActive(Duration value)
    {
        _readActive = value;
    }

    public Optional<Duration> getReadActive()
    {
        return Optional.ofNullable(_readActive);
    }

    public void setWriteIdle(Duration value)
    {
        _writeIdle = value;
    }

    public Optional<Duration> getWriteIdle()
    {
        return Optional.ofNullable(_writeIdle);
    }

    public void setWriteActive(Duration value)
    {
        _writeActive = value;
    }

    public Optional<Duration> getWriteActive()
    {
        return Optional.ofNullable(_writeActive);
    }

    @Override
    public String toString()
    {
        return "MoverInfoMessage{" +
               "dataTransferred=" + _dataTransferred +
               ", connectionTime=" + _connectionTime +
               ", protocolInfo=" + _protocolInfo +
               ", fileCreated=" + _fileCreated +
               ", initiator='" + _initiator + '\'' +
               ", isP2p=" + _isP2p +
               ", transferPath='" + _transferPath + '\'' +
               ", readBw='" + _meanReadBandwidth + '\'' +
               ", writeBw='" + _meanWriteBandwidth + '\'' +
               ", readIdle='" + _readIdle + '\'' +
               ", readActive='" + _readActive + '\'' +
               ", writeIdle='" + _writeIdle + '\'' +
               ", writeActive='" + _writeActive + '\'' +
               "} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
