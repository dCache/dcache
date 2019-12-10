package diskCacheV111.vehicles;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;

public class PoolManagerPoolUpMessage extends PoolManagerMessage {

    private static final long serialVersionUID = -8421133630068493665L;
    private final String _poolName ;
    private final long         _serialId ;
    private final PoolCostInfo _poolCostInfo ;
    private final PoolV2Mode   _mode;

    private String  _hostName;
    private Map<String, String>       _tagMap;
    private Set<String> _hsmInstances;
    private String    _message                 = "";
    private int       _code;

    public PoolManagerPoolUpMessage(String poolName, long serialId,
                                    PoolV2Mode mode)
    {
        this(poolName, serialId, mode, null);
    }

    public PoolManagerPoolUpMessage(String poolName, long serialId,
                                    PoolV2Mode mode, PoolCostInfo costInfo)
    {
        assert mode != null;

        _poolName = poolName;
        _serialId = serialId;
        _mode = mode;
        _poolCostInfo = costInfo;
        setReplyRequired(false);
    }

    public PoolCostInfo getPoolCostInfo(){ return _poolCostInfo ; }
    public String getPoolName(){
        return _poolName;
    }
    public long getSerialId(){ return _serialId ; }
    public void setTagMap( Map<String, String> map ){ _tagMap = map ; }
    public Map<String, String>  getTagMap(){ return _tagMap ; }
    public void setHostName(String hostName)
    {
        _hostName = hostName;
    }
    public String getHostName() { return _hostName; }

    /**
     * Sets the human readable status message of the pool.
     */
    public void setMessage(String msg)
    {
        assert msg != null;

        _message = msg;
    }

    /**
     * Returns the human readable status message of the pool. May be
     * null.
     */
    public String getMessage()
    {
        return _message;
    }

    /**
     * Sets the machine interpretable status code of the pool.
     */
    public void setCode(int code)
    {
        _code = code;
    }

    /**
     * Returns the machine interpretable status code of the
     * pool. Returns 0 if the status code has not been set.
     */
    public int getCode()
    {
        return _code;
    }

    /**
     * Returns the mode of the pool. The mode indicates which
     * operations are currently supported by the pool.
     */
    public PoolV2Mode getPoolMode()
    {
        return _mode;
    }

    /** Returns the names of attached HSM instances. */
    public Set<String> getHsmInstances()
    {
        return _hsmInstances;
    }

    /**
     * Sets the set of names of attached HSM instances.
     *
     * @param value Set of HSM instance names. Must implement Serializable.
     */
    public void setHsmInstances(Set<String> value)
    {
        assert value instanceof Serializable;
        _hsmInstances = value;
    }
}
