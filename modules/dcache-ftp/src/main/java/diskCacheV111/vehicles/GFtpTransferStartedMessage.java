package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

public class GFtpTransferStartedMessage extends Message
{
    private String            _pnfsId;
    private boolean           _passive;
    private InetSocketAddress _address;

    private static final long serialVersionUID = -829883739386394165L;

    public GFtpTransferStartedMessage(String pnfsId, String hostname,
                                      int port)
    {
        _pnfsId   = pnfsId;
        _address  = new InetSocketAddress(hostname, port);
        _passive  = true;
    }

    public GFtpTransferStartedMessage(String pnfsId)
    {
        _pnfsId   = pnfsId;
        _passive  = false;
        _address  = null;
    }

    public String getPnfsId()
    {
        return _pnfsId;
    }

    public boolean getPassive()
    {
        return _passive;
    }

    public InetSocketAddress getPoolAddress()
    {
        return _address;
    }

    @Override
    public String getDiagnosticContext()
    {
        return super.getDiagnosticContext() + " " + getPnfsId();
    }

}
