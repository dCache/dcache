package diskCacheV111.vehicles;

import java.util.Collection;
import java.net.URI;

public class PoolRemoveFilesFromHSMMessage extends PoolMessage 
{
    private final String _hsm;
    private Collection<URI> _files;
    private Collection<URI> _succeeded;
    private Collection<URI> _failed;
        
    public PoolRemoveFilesFromHSMMessage(String poolName, String hsm, Collection<URI> files)
    {
	super(poolName);
        _hsm = hsm;
        _files = files;
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
        _files = null;
        _succeeded = succeeded;
        _failed = failed;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(super.toString());
        if (_files != null) {
            sb.append(";files=").append(_files);
        }
        if (_succeeded != null) {
            sb.append(";succeeded=").append(_succeeded);
        }
        if (_failed != null) {
            sb.append(";failed=").append(_failed);
        }
        return sb.toString();
    }
}
