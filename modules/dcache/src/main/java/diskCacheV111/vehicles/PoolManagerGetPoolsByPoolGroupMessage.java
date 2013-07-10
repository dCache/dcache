package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolManagerGetPoolsByPoolGroupMessage
    extends PoolManagerGetPoolsMessage
{
    private static final long serialVersionUID = 2808625734157545379L;

    @Deprecated // Remove in release after golden release 4
    private final String _poolGroup = null;
    private Iterable<String> _poolGroups;

    public PoolManagerGetPoolsByPoolGroupMessage(Iterable<String> poolGroups)
    {
        _poolGroups = checkNotNull(poolGroups);
    }

    public Iterable<String> getPoolGroups()
    {
        return _poolGroups;
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        if (_poolGroup != null && _poolGroups == null) {
            _poolGroups = Collections.singleton(_poolGroup);
        }
    }
}
