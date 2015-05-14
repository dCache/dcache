package dmg.util.logback;

import ch.qos.logback.classic.Level;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class maintains a set of filter thresholds. Two types of inheritance
 * are supported:
 *
 * - Inheritance from a parent FilterThresholds
 * - Inheritance from parent loggers
 *
 * If a threshold is not defined for a given combination of logger and
 * appender, then the parent FilterThresholds is consulted recursively.
 * If not defined in the parent, the threshold of the parent logger is
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

    /* Logger x Appender -> Level */
    private final Table<LoggerName, String, Level> _rules = HashBasedTable.create();

    /* Logger -> (Appender -> Level) */
    private final LoadingCache<LoggerName,Map<String,Level>> _effectiveMaps =
            CacheBuilder.newBuilder().build(CacheLoader.from(this::computeEffectiveMap));

    /* Logger -> Level */
    private final LoadingCache<LoggerName,Optional<Level>> _effectiveLevels =
            CacheBuilder.newBuilder().build(
                    new CacheLoader<LoggerName,Optional<Level>>()
                    {
                        @Override
                        public Optional<Level> load(LoggerName logger)
                        {
                            try {
                                Map<String,Level> map = _effectiveMaps.get(logger);
                                return map.isEmpty()
                                        ? Optional.<Level>absent()
                                        : Optional.of(Collections.min(map.values(), LEVEL_ORDER));
                            } catch (ExecutionException e) {
                                throw Throwables.propagate(e.getCause());
                            }
                        }
                    });

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
     * Returns the threshold of the given logger and appender combination. Neither the
     * parent thresholds nor the parent loggers are taken into account.
     */
    public synchronized Level get(LoggerName logger, String appender)
    {
        return _rules.get(logger, appender);
    }

    /**
     * Sets a threshold for the given logger and appender.
     */
    public synchronized void setThreshold(LoggerName logger, String appender, Level level)
    {
        checkNotNull(logger);
        checkNotNull(level);
        checkArgument(hasAppender(appender));
        _rules.put(logger, appender, level);
        clearCache();
    }

    /**
     * Removes the threshold of the given logger and appender combination in this
     * threshold set. The new effective threshold will be derived from the regular
     * inheritance rules.
     */
    public synchronized void remove(LoggerName logger, String appender)
    {
        if (_rules.remove(logger, appender) != null) {
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
     * Returns a map from appenders to levels for a logger.
     *
     * The map contains inherited levels from parent filter threshold sets.
     */
    public synchronized Map<String,Level> getInheritedMap(LoggerName logger)
    {
        if (_parent == null) {
            return Maps.newHashMap(_rules.row(logger));
        } else {
            Map<String,Level> map = _parent.getInheritedMap(logger);
            map.putAll(_rules.row(logger));
            return map;
        }
    }

    /**
     * Returns a map from appenders to levels for a logger.
     *
     * The map contains the effective log levels, that is, the levels
     * used for filtering log events.
     */
    private synchronized Map<String,Level> computeEffectiveMap(LoggerName logger)
    {
        LoggerName parent = logger.getParent();
        Map<String,Level> map;
        if (parent == null) {
            map = getInheritedMap(logger);
        } else {
            map = computeEffectiveMap(parent);
            map.putAll(getInheritedMap(logger));
        }
        return map;
    }

    /**
     * Returns the effective log threshold for a given pair of logger and appender.
     */
    public Level getThreshold(LoggerName logger, String appender)
    {
        try {
            return _effectiveMaps.get(logger).get(appender);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    /**
     * Gets the effective minimum threshold for a given pair regardless of appender.
     */
    public Level getThreshold(LoggerName logger)
    {
        try {
            return _effectiveLevels.get(logger).orNull();
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }
}
