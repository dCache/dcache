package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.util.CacheException;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.*;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;

/**
 * Encapsulates configuration parameters and pool selection logic.
 *
 * A Partition contains a number of properties. The value of a
 * property may be the default, inherited or defined. The default
 * value is hardcoded, inherited values are potentially shared by
 * multiple partitions, while defined values are defined for this
 * Partition.
 *
 * Properties are immutable and any update requires that a new
 * instance is created (see updateProperties and updateInherited). The
 * reason to distinguish between inherited and defined properies is
 * that each can be updated separately. The abstract create method is
 * used to implement this copy-on-write scheme.
 *
 * New partitions are usually created through a PartitionFactory or
 * instantiated directly. The create method is protected and is only
 * used to implement the copy-on-write scheme to update properties.
 *
 * Properties will be parsed during partition instantiation and values
 * will be cached in immutable fields. Subclasses are encouraged to do
 * the same in their constructor for properties specific to the
 * subclass.
 *
 * A number of utility methods are provided to query the value of a
 * property, taking default, inherited and defined values into account
 * (with the default value having the lowest and the defined value
 * having the highest precedence). Subclasses should use these methods
 * to parse properties.
 *
 * Although properties are immutable, the partition may not be: Some
 * selection strategies may be stateful and will have to update the
 * partition when selecting pools. An example of such a strategy is
 * round robin selection.
 *
 * Partition itself is abstract. Subclasses must implement the pool
 * selection, create and the getType methods. The base class does
 * recognize a number of standard properties used by other parts of
 * pool manager.
 *
 * Several partitions can be created and assigned to different pool
 * manager links.
 *
 * The class is thread safe.
 */
public abstract class Partition implements Serializable
{
    private static final long serialVersionUID = -4195341006626972862L;

    protected static final Map<String,String> NO_PROPERTIES =
        ImmutableMap.of();

    /**
     * P2P
     *
     *   p2p-allowed     boolean
     *   p2p-oncost      boolean
     *   p2p-fortransfer boolean
     *
     * STAGING
     *
     *   stage-allowed   boolean
     *   stage-oncost    boolean
     */
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
    private final ImmutableMap<String,String> _defined;

    public final boolean _p2pAllowed;
    public final boolean _p2pOnCost;
    public final boolean _p2pForTransfer;

    public final boolean _hasHsmBackend;
    public final boolean _stageOnCost;

    /**
     * Constructs new partition.
     *
     * Subclasses are to call this constructor, providing their own
     * default properties and the inherited and defined properties
     * provided by the PartitionManager.
     *
     * Only properties for which a default value is known are
     * retained. It is essential that subclasses provide default
     * entries for all supported properties.
     *
     * It is recommended that subclasses provide a three parameter
     * constructor too, as this facilitates further
     * subclassing. Subclasses are free to provide additional
     * constructors for other purposes.
     *
     * @param defaults Defaults provided by subclass
     * @param inherited Runtime inherited properties
     * @param defined Runtime provided properties for this partition
     */
    protected Partition(Map<String,String> defaults,
                        Map<String,String> inherited,
                        Map<String,String> defined)
    {
        _defaults =
            ImmutableMap.<String,String>builder()
            .putAll(DEFAULTS)
            .putAll(defaults)
            .build();
        _inherited =
            ImmutableMap.copyOf(filterKeys(inherited, in(_defaults.keySet())));
        _defined =
            ImmutableMap.copyOf(filterKeys(defined, in(_defaults.keySet())));

        _p2pAllowed = getBoolean("p2p-allowed");
        _p2pOnCost = getBoolean("p2p-oncost");
        _p2pForTransfer = getBoolean("p2p-fortransfer");
        _hasHsmBackend = getBoolean("stage-allowed");
        _stageOnCost = getBoolean("stage-oncost");
    }

    /**
     * Creates a new partition of the same type as this partition.
     *
     * Used when updating properties to implement a copy-on-write
     * scheme.
     *
     * Advanced implementations may choose to preserve additional
     * state that isn't captured by configuraton parameters.
     */
    protected abstract Partition create(Map<String,String> inherited,
                                        Map<String,String> defined);

    /**
     * Returns a map of defined properties in this partition.
     *
     * The map is unmodifiable.
     */
    public Map<String,String> getProperties()
    {
        return _defined;
    }

    /**
     * Returns a map of all properties of this partition, including
     * inherited and default properties.
     */
    public Map<String,String> getAllProperties()
    {
        Map<String,String> map = new HashMap<>();
        map.putAll(_defaults);
        map.putAll(_inherited);
        map.putAll(_defined);
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
    public Partition updateProperties(Map<String,String> defined)
    {
        ImmutableMap<String,String> map =
            ImmutableMap.<String,String>builder()
            .putAll(filterKeys(_defined, not(in(defined.keySet()))))
            .putAll(filterValues(defined, notNull()))
            .build();
        return create(_inherited, map);
    }

    /**
     * Updates inherited properties.
     *
     * A new partition with the updated inherited properties is
     * returned. Non of the inherited properties of this Partition are
     * carried over.
     */
    public Partition updateInherited(Map<String,String> inherited)
    {
        return create(inherited, _defined);
    }

    /**
     * Returns the value of a property.
     *
     * @throws NoSuchElementException If the property is undefined
     */
    public String getProperty(String name)
        throws NoSuchElementException
    {
        String value = _defined.get(name);
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
     * @throws IllegalArgumentException If the value of the property
     *                                  is not a boolean
     * @throws NoSuchElementException If the property is undefined
     */
    public boolean getBoolean(String name)
        throws NoSuchElementException,
               IllegalArgumentException
    {
        String value = getProperty(name);
        switch (value) {
        case "yes":
            return true;
        case "no":
            return false;
        default:
            throw new IllegalArgumentException("Boolean property " + name + " has invalid value: " + value);
        }
    }

    /**
     * Returns the long value of an long property.
     *
     * @throws IllegalArgumentException If the value of the property
     *                                  is not a long
     * @throws NoSuchElementException If the property is undefined
     */
    public long getLong(String name)
        throws NoSuchElementException,
               IllegalArgumentException
    {
        return Long.parseLong(getProperty(name));
    }

    /**
     * Returns the double value of an double property.
     *
     * @throws IllegalArgumentException If the value of the property
     *                                  is not a double
     * @throws NoSuchElementException If the property is undefined
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
        Map<String,Object[]> map = new HashMap<>();

        for (Map.Entry<String,String> entry: _defaults.entrySet()) {
            map.put(entry.getKey(),
                    new Object[] { false, entry.getValue() });
        }

        for (Map.Entry<String,String> entry: _inherited.entrySet()) {
            map.put(entry.getKey(),
                    new Object[] { false, entry.getValue() });
        }

        for (Map.Entry<String,String> entry: _defined.entrySet()) {
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
     * Returns the short name of the partitions type. Must correspond
     * to the short name used by the Partition's factory.
     */
    public abstract String getType();

    /**
     * Selects a pool for writing among a set of pools. May modify
     * the input list of pools.
     *
     * An implementation cannot rely on any file attributes being defined.
     */
    public abstract SelectedPool selectWritePool(CostModule cm,
                                                 List<PoolInfo> pools,
                                                 FileAttributes attributes,
                                                 long preallocated)
        throws CacheException;

    /**
     * Selects a pool for reading among a set of pools. May modify
     * the input list of pools.
     *
     * An implementation cannot rely on any file attributes other than
     * PNFS id, storage info and locations being defined.
     */
    public abstract SelectedPool selectReadPool(CostModule cm,
                                                List<PoolInfo> pools,
                                                FileAttributes attributes)
        throws CacheException;

    /**
     * Selects a pair of pools for pool to pool among a set of
     * pools. May modify the input lists of pools.
     *
     * An implementation cannot rely on any file attributes other than
     * PNFS id, storage info and locations being defined.
     */
    public abstract P2pPair selectPool2Pool(CostModule cm,
                                            List<PoolInfo> src,
                                            List<PoolInfo> dst,
                                            FileAttributes attributes,
                                            boolean force)
        throws CacheException;

    /**
     * Selects a pool for staging among a set of pools. May modify the
     * input list of pools.
     *
     * An implementation cannot rely on any file attributes other than
     * PNFS id, storage info and locations being defined.
     */
    public abstract SelectedPool selectStagePool(CostModule cm,
                                                 List<PoolInfo> pools,
                                                 String previousPool,
                                                 String previousHost,
                                                 FileAttributes attributes)
        throws CacheException;

    /**
     * Immutable helper class to represent a source and destination
     * pair for pool to pool selection.
     */
    public static class P2pPair
    {
        public final SelectedPool source;
        public final SelectedPool destination;

        public P2pPair(SelectedPool source, SelectedPool destination)
        {
            this.source = source;
            this.destination = destination;
        }
    }
}
