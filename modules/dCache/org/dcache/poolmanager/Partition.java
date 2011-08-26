package org.dcache.poolmanager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.NoSuchElementException;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import com.google.common.collect.ImmutableMap;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;

/**
 * Encapsulates configuration parameters and pool selection logic.
 *
 * A Partition contains a number of properties. The value of a
 * property may be default, inherited or defined. The default value is
 * hardcoded, the inherited value is injected, while the defined value
 * is defined in this Partition.
 *
 * Properties are immutable and any update requires that a new
 * instance is created. When created, properties will be parsed and
 * values will be cached in immutable fields.
 *
 * Although properties are immutable, the partition may not be: Some
 * selection strategies may be stateful and will have to update the
 * partition when selecting pools. An example of such a strategy is
 * round robin selection.
 *
 * Partition itself is abstract. Subclasses must implement the pool
 * selection methods. The base class does recognize a number of
 * standard properties used by other parts of pool manager.
 *
 * Several partitions can be created and assigned to different pool
 * manager links.
 *
 * The class is thread safe.
 */
abstract public class Partition implements Serializable
{
    static final long serialVersionUID = -4195341006626972862L;

    private final Map<String,String> DEFAULTS =
        ImmutableMap.<String,String>builder()
        .put("p2p-allowed", "yes")
        .put("p2p-oncost", "no")
        .put("p2p-fortransfer", "no")
        .put("stage-allowed", "no")
        .put("stage-oncost", "no")
        .build();

    private final ImmutableMap<String,String> _defaults;
    private final ImmutableMap<String,String> _inherited;
    private final ImmutableMap<String,String> _properties;

    public final boolean _p2pAllowed;
    public final boolean _p2pOnCost;
    public final boolean _p2pForTransfer;

    public final boolean _hasHsmBackend;
    public final boolean _stageOnCost;

    protected Partition(Map<String,String> defaults,
                        Map<String,String> inherited,
                        Map<String,String> properties)
    {
        _defaults =
            ImmutableMap.<String,String>builder()
            .putAll(DEFAULTS)
            .putAll(defaults)
            .build();
        _inherited =
            ImmutableMap.copyOf(filterKeys(inherited, in(_defaults.keySet())));
        _properties =
            ImmutableMap.copyOf(filterKeys(properties, in(_defaults.keySet())));

        _p2pAllowed = getBoolean("p2p-allowed");
        _p2pOnCost = getBoolean("p2p-oncost");
        _p2pForTransfer = getBoolean("p2p-fortransfer");
        _hasHsmBackend = getBoolean("stage-allowed");
        _stageOnCost = getBoolean("stage-oncost");
    }

    /**
     * Creates a new partition of the same type as this partition.
     *
     * Used when updating properties.
     */
    abstract protected Partition create(Map<String,String> inherited,
                                        Map<String,String> properties);

    /**
     * Returns a map of properties defined in this partition.
     *
     * The map is unmodifiable.
     */
    public Map<String,String> getProperties()
    {
        return _properties;
    }

    /**
     * Returns a map of all properties of this partition, including
     * inherited and default properties.
     */
    public Map<String,String> getAllProperties()
    {
        Map<String,String> map = new HashMap<String,String>();
        map.putAll(_defaults);
        map.putAll(_inherited);
        map.putAll(_properties);
        return map;
    }

    /**
     * Updates defined properties.
     *
     * A new partition with the updated defined properties is
     * returned.
     *
     * If value is null then the property is removed and a default
     * value is used instead. Properties not present in the new map
     * will be carried over from this Partition.
     */
    public Partition updateProperties(Map<String,String> properties)
    {
        ImmutableMap<String,String> map =
            ImmutableMap.<String,String>builder()
            .putAll(filterKeys(_properties, not(in(properties.keySet()))))
            .putAll(filterValues(properties, notNull()))
            .build();
        return create(_inherited, map);
    }

    /**
     * Updates inherited properties.
     *
     * A new partition with the updated defined properties is
     * returned. Non of the inherited properties of this Partition are
     * carried over.
     */
    public Partition updateInherited(Map<String,String> inherited)
    {
        return create(inherited, _properties);
    }

    /**
     * Returns the value of a property.
     */
    public String getProperty(String name)
        throws NoSuchElementException
    {
        String value = _properties.get(name);
        if (value == null) {
            value = _inherited.get(name);
        }
        if (value == null) {
            value = _defaults.get(name);
        }
        if (value == null) {
            throw new NoSuchElementException("No such property: " + name);
        }
        return value;
    }

    /**
     * Returns the boolean value of a boolean property.
     *
     * Legal values of boolean property are yes and no.
     *
     * Throws IllegalArgumentException if the property is not a valid
     * boolean property.
     */
    public boolean getBoolean(String name)
        throws NoSuchElementException,
               IllegalArgumentException
    {
        String value = getProperty(name);
        if (value.equals("yes")) {
            return true;
        } else if (value.equals("no")) {
            return false;
        } else {
            throw new IllegalArgumentException("Boolean property " + name + " has invalid value: " + value);
        }
    }

    /**
     * Returns the integer value of an integer property.
     *
     * Throws IllegalArgumentException if the property is not a valid
     * integer property.
     */
    public int getInteger(String name)
        throws NoSuchElementException,
               IllegalArgumentException
    {
        return Integer.parseInt(getProperty(name));
    }

    /**
     * Returns the double value of an double property.
     *
     * Throws IllegalArgumentException if the property is not a valid
     * double property.
     */
    public double getDouble(String name)
        throws NoSuchElementException,
               IllegalArgumentException
    {
        return Double.parseDouble(getProperty(name));
    }

    /**
     * Legacy helper method used by HttpPoolMgrEngineV3.
     */
    public synchronized Map<String,Object[]> toMap()
    {
        Map<String,Object[]> map = new HashMap<String,Object[]>();

        for (Map.Entry<String,String> entry: _defaults.entrySet()) {
            map.put(entry.getKey(),
                    new Object[] { false, entry.getValue() });
        }

        for (Map.Entry<String,String> entry: _inherited.entrySet()) {
            map.put(entry.getKey(),
                    new Object[] { false, entry.getValue() });
        }

        for (Map.Entry<String,String> entry: _properties.entrySet()) {
            map.put(entry.getKey(), new Object[] { true, entry.getValue() });
        }

        return map;
    }

    public boolean getP2PAllowed()
    {
        return _p2pAllowed;
    }

    public boolean getP2POnCost()
    {
        return _p2pOnCost;
    }

    public boolean getP2PForTransfer()
    {
        return _p2pForTransfer;
    }

    public boolean getHasHsmBackend()
    {
        return _hasHsmBackend;
    }

    public boolean getStageOnCost()
    {
        return _stageOnCost;
    }

    /**
     * Selects a pool for writing among a set of pools. May modify
     * the input list of pools.
     */
    abstract public PoolInfo
        selectWritePool(CostModule cm, List<PoolInfo> pools, long filesize)
        throws CacheException;

    /**
     * Selects a pool for reading among a set of pools. May modify
     * the input list of pools.
     */
    abstract public PoolInfo
        selectReadPool(CostModule cm, List<PoolInfo> pools, PnfsId pnfsId)
        throws CacheException;

    /**
     * Selects a pair of pools for pool to pool among a set of
     * pools. May modify the input lists of pools.
     */
    abstract public P2pPair
        selectPool2Pool(CostModule cm,
                        List<PoolInfo> src,
                        List<PoolInfo> dst,
                        long filesize,
                        boolean force)
        throws CacheException;

    /**
     * Selects a pool for staging among a set of pools. May modify the
     * input list of pools.
     */
    abstract public PoolInfo selectStagePool(CostModule cm,
                                             List<PoolInfo> pools,
                                             String previousPool,
                                             String previousHost,
                                             long size)
        throws CacheException;

    /**
     * Immutable helper class to represent a source and destination
     * pair for pool to pool selection.
     */
    public static class P2pPair
    {
        public final PoolInfo source;
        public final PoolInfo destination;

        public P2pPair(PoolInfo source, PoolInfo destination)
        {
            this.source = source;
            this.destination = destination;
        }
    }
}
