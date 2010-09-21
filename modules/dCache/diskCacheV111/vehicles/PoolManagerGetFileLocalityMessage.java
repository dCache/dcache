package diskCacheV111.vehicles;

import java.util.Set;
import java.util.EnumSet;

import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import diskCacheV111.util.FileLocality;

import static org.dcache.namespace.FileAttribute.*;

public class PoolManagerGetFileLocalityMessage extends PoolManagerMessage
{
    static final long serialVersionUID = 2876698847976115757L;

    private final FileAttributes _attributes;
    private final String _client;
    private FileLocality _locality;

    public PoolManagerGetFileLocalityMessage(FileAttributes attributes,
                                             String client)
    {
        _attributes = attributes;
        _client = client;
    }

    public FileAttributes getFileAttributes()
    {
        return _attributes;
    }

    public String getClient()
    {
        return _client;
    }

    public void setFileLocality(FileLocality locality)
    {
        _locality = locality;
    }

    public FileLocality getFileLocality()
    {
        return _locality;
    }

    public static Set<FileAttribute> getRequiredAttributes()
    {
        return EnumSet.of(STORAGEINFO, SIZE, LOCATIONS);
    }
}