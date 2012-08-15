package diskCacheV111.vehicles;

import java.util.Collection;
import java.util.ArrayList;
import java.net.URI;

public class PoolRemoveFilesFromHSMMessage extends PoolMessage
{
    private static final long serialVersionUID = 7659588592755172141L;
    private final String _hsm;
    private Collection<URI> _files;
    private Collection<URI> _succeeded;
    private Collection<URI> _failed;

    public PoolRemoveFilesFromHSMMessage(String poolName, String hsm, Collection<URI> files)
    {
	super(poolName);
        _hsm = hsm;
        _files = files;
        _succeeded = new ArrayList<URI>();
        _failed = new ArrayList<URI>();
	setReplyRequired(true);
    }

    public String getHsm()
    {
        return _hsm;
    }

    public Collection<URI> getFiles()
    {
        return _files;
    }

    public Collection<URI> getSucceeded()
    {
        return _succeeded;
    }

    public Collection<URI> getFailed()
    {
        return _failed;
    }

    public void setResult(Collection<URI> succeeded, Collection<URI> failed)
    {
        if (succeeded == null || failed == null) {
            throw new IllegalArgumentException("Argument must not be null");
        }

        _files.clear();
        _succeeded = succeeded;
        _failed = failed;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(";files=").append(_files);
        sb.append(";succeeded=").append(_succeeded);
        sb.append(";failed=").append(_failed);
        return sb.toString();
    }
}
