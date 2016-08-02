package diskCacheV111.vehicles;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

public class PoolRemoveFilesFromHSMMessage extends PoolMessage
{
    private static final long serialVersionUID = 7659588592755172141L;
    private final String _hsm;
    private final Collection<URI> _files;
    private Collection<URI> _succeeded;
    private Collection<URI> _failed;

    public PoolRemoveFilesFromHSMMessage(String poolName, String hsm, Collection<URI> files)
    {
	super(poolName);
        _hsm = hsm;
        _files = files;
        _succeeded = new ArrayList<>();
        _failed = new ArrayList<>();
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
        String sb = super.toString() + ";files=" + _files +
                    ";succeeded=" + _succeeded +
                    ";failed=" + _failed;
        return sb;
    }
}
