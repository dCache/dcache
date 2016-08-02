package dmg.cells.nucleus;

import java.io.Serializable;

import org.dcache.util.Version;

public class CellVersion implements Serializable
{
    private static final long serialVersionUID = 883744769418282912L;

    private final String _version;
    private final String _release;
    private final String _revision;

    public CellVersion()
    {
        this("Unknown", "Unknown");
    }

    public CellVersion(Version version)
    {
        this(version.getVersion(), version.getBuild());
    }

    public CellVersion(String release, String revision)
    {
        _revision = revision;
        _release = release;
        _version = _release + '(' + _revision + ')';
    }

    public String getRelease()
    {
        return _release;
    }

    public String getRevision()
    {
        return _revision;
    }

    @Override
    public String toString()
    {
        return _version;
    }
}
