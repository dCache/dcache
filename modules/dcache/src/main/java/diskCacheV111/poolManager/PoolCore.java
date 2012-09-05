package diskCacheV111.poolManager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

class PoolCore implements Serializable {
    private static final long serialVersionUID = -8571296485927073985L;
    protected final String _name;
    protected final Map<String, Link> _linkList = new HashMap<String, Link>();

    protected PoolCore(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

}
