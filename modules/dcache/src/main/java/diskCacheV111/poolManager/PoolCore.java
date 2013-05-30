package diskCacheV111.poolManager;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

abstract class PoolCore implements Serializable {
    private static final long serialVersionUID = -8571296485927073985L;
    private final String _name;
    protected final Map<String, Link> _linkList = new ConcurrentHashMap<>();

    protected PoolCore(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

}
