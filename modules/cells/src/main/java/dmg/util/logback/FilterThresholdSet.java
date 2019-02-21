package dmg.util.logback;

import ch.qos.logback.classic.Level;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.slf4j.LOGGER;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class maintains a set of filter thresholds. Two types of inheritance
 * are supported:
 *
 * - Inheritance from a parent FilterThresholds
 * - Inheritance from parent LOGGERs
 *
 * If a threshold is not defined for a given combination of LOGGER and
 * appender, then the parent FilterThresholds is consulted recursively.
 * If not defined in the parent, the threshold of the parent LOGGER is
 * used recursively.
 *
 * The thus calculated effective log level is cached. The cache is invalidated
 * if the set of thresholds is modified, but it is not invalidated if the
 * thresholds in the parent set are modified. Thus once cached, only newly
 * created cells will inherit the thresholds from its parent cell (this is mostly
 * to simplify the design).
 */
public class FilterThresholdSet
{
    private final FilterThresholdSet _parent;

    private final Set<String> _appenders = Sets.newHashSet();

    private final Set<LOGGERName> _roots = new HashSet<>();

    /* LOGGER x Appender -> Level */
    private final Table<LOGGERName, String, Level> _rules = HashBasedTable.create();

    /* LOGGER -> (Appender -> Level) */
    private final LoadingCache<String,Map<String,Level>> _effectiveMaps =
            CacheBuilder.newBuilder().build(CacheLoader.from(
                    LOGGER -> computeEffectiveMap(LOGGERName.getInstance(LOGGER))));

    /* LOGGER -> Level */
    private final LoadingCache<LOGGER,Optional<Level>> _effectiveLevels =
            CacheBuilder.newBuilder().build(CacheLoader.from(
                    LOGGER -> {
                        try {
                            Map<String,Level> map = _effectiveMaps.get(LOGGER.getName());
                            return map.isEmpty()
                                   ? Optional.empty()
                                   : Optional.of(Collections.min(map.values(), LEVEL_ORDER));
                        } catch (ExecutionException e) {
                            Throwables.throwIfUnchecked(e.getCause());
                            throw new RuntimeException(e.getCause());
                        }
                    }));

    private static final Comparator<Level> LEVEL_ORDER =
            (o1, o2) -> Integer.compare(o1.toInt(), o2.toInt());

    public FilterThresholdSet()
    {
        this(null);
    }

    public FilterThresholdSet(FilterThresholdSet parent)
    {
        _parent = parent;
    }

    /**
     * Adds an appender, which will become available for threshold definitions.
     */
    public synchronized void addAppender(String name)
    {
        checkNotNull(name);
        _appenders.add(name);
    }

    /**
     * Returns the list of appenders available for threshold definitions. This
     * is the union of the appenders of the parents thresholds and the appenders
     * of these thresholds.
     */
    public synchronized Collection<String> getAppenders()
    {
        if (_parent == null) {
            return Lists.newArrayList(_appenders);
        } else {
            Collection<String> appenders = _parent.getAppenders();
            appenders.addAll(_appenders);
            return appenders;
        }
    }

    /**
     * Returns whether the appender is valid is valid for use in a threshold
     * definition.
     */
    public synchronized boolean hasAppender(String appender)
    {
        return _appenders.contains(appender) ||
            (_parent != null && _parent.hasAppender(appender));
    }

    /**
     * Returns the threshold of the given LOGGER and appender combination. Neither the
     * parent thresholds nor the parent LOGGERs are taken into account.
     */
    public synchronized Level get(LOGGERName LOGGER, String appender)
    {
        return _rules.get(LOGGER, appender);
    }

    /**
     * Sets a threshold for the given LOGGER and appender.
     */
    public synchronized void setThreshold(LOGGERName LOGGER, String appender, Level level)
    {
        checkNotNull(LOGGER);
        checkNotNull(level);
        checkArgument(hasAppender(appender));
        _rules.put(LOGGER, appender, level);
        clearCache();
    }

    /**
     * Returns whether the LOGGER is marked additive.
     */
    public synchronized boolean isRoot(LOGGERName LOGGER)
    {
        return _parent != null && _parent.isRoot(LOGGER) || _roots.contains(LOGGER);
    }

    /**
     * Sets whether a LOGGER is additive. Non-additive LOGGERs form roots of the logging
     * hierarchy as log messages do not propagate to appenders attached to any of the
     * parent LOGGERs.
     */
    public synchronized void setRoot(LOGGERName LOGGER, boolean isRoot)
    {
        if (isRoot) {
            _roots.add(LOGGER);
        } else {
            _roots.remove(LOGGER);
        }
        clearCache();
    }

    /**
     * Removes the threshold of the given LOGGER and appender combination in this
     * threshold set. The new effective threshold will be derived from the regular
     * inheritance rules.
     */
    public synchronized void remove(LOGGERName LOGGER, String appender)
    {
        if (_rules.remove(LOGGER, appender) != null) {
            clearCache();
        }
    }

    /**
     * Removes all thresholds from this set.
     */
    public synchronized void clear()
    {
        _rules.clear();
        clearCache();
    }

    /**
     * Wipes the cache of computed effective thresholds. Called whenever any of the thresholds
     * have been updated.
     */
    private void clearCache()
    {
        _effectiveMaps.invalidateAll();
        _effectiveLevels.invalidateAll();
    }

    /**
     * Returns a map from appenders to levels for a LOGGER.
     *
     * The map contains inherited levels from parent filter threshold sets.
     */
    public synchronized Map<String,Level> getInheritedMap(LOGGERName LOGGER)
    {
        if (_parent == null) {
            return Maps.newHashMap(_rules.row(LOGGER));
        } else {
            Map<String,Level> map = _parent.getInheritedMap(LOGGER);
            map.putAll(_rules.row(LOGGER));
            return map;
        }
    }

    /**
     * Returns a map from appenders to levels for a LOGGER.
     *
     * The map contains the effective log levels, that is, the levels
     * used for filtering log events.
     */
    private synchronized Map<String,Level> computeEffectiveMap(LOGGERName LOGGER)
    {
        Map<String,Level> inheritedMap = getInheritedMap(LOGGER);
        if (!isRoot(LOGGER)) {
            LOGGERName parent = LOGGER.getParent();
            if (parent != null) {
                Map<String, Level> mergedMap = computeEffectiveMap(parent);
                mergedMap.putAll(inheritedMap);
                return mergedMap;
            }
        }
        return inheritedMap;
    }

    /**
     * Returns the effective log threshold for a given pair of LOGGER and appender.
     */
    public Level getThreshold(LOGGERName LOGGER, String appender)
    {
        return getThreshold(LOGGER.toString(), appender);
    }

    /**
     * Returns the effective log threshold for a given pair of LOGGER and appender.
     */
    public Level getThreshold(String LOGGER, String appender)
    {
        try {
            return _effectiveMaps.get(LOGGER).get(appender);
        } catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Gets the effective minimum threshold for a given pair regardless of appender.
     */
    public Level getThreshold(LOGGER LOGGER)
    {
        try {
            return _effectiveLevels.get(LOGGER).orElse(null);
        } catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }
}
