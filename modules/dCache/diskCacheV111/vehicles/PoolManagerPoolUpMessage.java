// $Id: PoolManagerPoolUpMessage.java,v 1.8 2007-07-23 10:49:09 behrmann Exp $

package diskCacheV111.vehicles;

import java.util.*; 
import java.io.Serializable; 
import diskCacheV111.pools.PoolCostInfo ;

public class PoolManagerPoolUpMessage extends PoolManagerMessage {
   
    private String    _poolName                = null;
    private long      _serialId                = 0L ;
    private PoolCostInfo _poolCostInfo         = null ;
    private Map       _tagMap                  = null ;
    private Set<String> _hsmInstances          = null;

    private static final long serialVersionUID = -8421133630068493665L;
    
    public PoolManagerPoolUpMessage(String poolName, long serialId ){
        _poolName = poolName;
	_serialId = serialId ;
	setReplyRequired(false);
    }
    public PoolManagerPoolUpMessage( String poolName , long serialId , PoolCostInfo costInfo ){
        this( poolName , serialId ) ;
        _poolCostInfo = costInfo ;
    }    
    public PoolCostInfo getPoolCostInfo(){ return _poolCostInfo ; }
    public String getPoolName(){
        return _poolName;
    }
    public long getSerialId(){ return _serialId ; }
    public void setTagMap( Map map ){ _tagMap = map ; }
    public Map  getTagMap(){ return _tagMap ; }

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
